package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenCIDN/OpenCIDN/pkg/cache"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/client"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/model"
)

func (r *Runner) runBlobSync(ctx context.Context) {
	for {
		err := r.runOnceBlobSync(context.Background())
		if err != nil {
			if err != errWait {
				r.logger.Warn("runOnceBlobSync", "error", err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case <-r.syncBlobCh:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *Runner) getBlobPending() []client.MessageResponse {
	r.pendingMut.Lock()
	defer r.pendingMut.Unlock()

	var pendingMessages []client.MessageResponse
	for _, msg := range r.blobPending {
		if msg.Status == model.StatusPending {
			pendingMessages = append(pendingMessages, msg)
		}
	}

	sort.Slice(pendingMessages, func(i, j int) bool {
		a := pendingMessages[i]
		b := pendingMessages[j]

		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return a.MessageID < b.MessageID
	})

	return pendingMessages
}

func (r *Runner) runOnceBlobSync(ctx context.Context) error {
	var (
		err  error
		errs []error
		resp client.MessageResponse
	)

	pending := r.getBlobPending()
	if len(pending) == 0 {
		return errWait
	}

	for _, msg := range pending {
		resp, err = r.queueClient.Consume(ctx, msg.MessageID, r.lease)
		if err != nil {
			errs = append(errs, err)
		} else {
			break
		}
	}

	if resp.MessageID == 0 || resp.Content == "" {
		if len(errs) == 0 {
			return errWait
		}
		return errors.Join(errs...)
	}

	return r.blobSync(ctx, resp)
}

func (r *Runner) blob(ctx context.Context, host, name, blob string, _ int64, gotSize, progress *atomic.Int64) error {
	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", name, blob),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to head blob: status code %d", resp.StatusCode)
	}
	size := resp.ContentLength
	contentType := resp.Header.Get("Content-Type")

	if size > 0 {
		gotSize.Store(size)
	}

	var caches []*cache.Cache

	if r.bigCache != nil && r.bigCacheSize > 0 && size >= int64(r.bigCacheSize) {
		caches = append([]*cache.Cache{r.bigCache}, r.caches...)
	} else {
		caches = append(caches, r.caches...)
	}

	var subCaches []*cache.Cache
	for _, cache := range caches {
		stat, err := cache.StatBlob(ctx, blob)
		if err == nil {
			gotSize := stat.Size()
			if size == gotSize {
				continue
			}
			r.logger.Error("size is not meeting expectations", "digest", blob, "size", size, "gotSize", gotSize)
		}
		subCaches = append(subCaches, cache)
	}

	if len(subCaches) == 0 {
		r.logger.Info("skip blob by cache", "digest", blob)
		return nil
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err = r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get blob: status code %d", resp.StatusCode)
	}

	r.logger.Info("start sync blob", "digest", blob, "url", u.String())

	if resp.ContentLength > 0 && size > 0 {
		if resp.ContentLength != size {
			return fmt.Errorf("failed to retrieve blob: expected size %d, got %d", size, resp.ContentLength)
		}
	}

	body := &readerCounter{
		r:       resp.Body,
		counter: progress,
	}

	if len(subCaches) == 1 {
		n, err := subCaches[0].PutBlob(ctx, blob, body, contentType)
		if err != nil {
			return fmt.Errorf("put blob failed: %w", err)
		}

		r.logger.Info("finish sync blob", "digest", blob, "size", n)
		return nil
	}

	var writers []io.Writer
	var closers []io.Closer
	var wg sync.WaitGroup

	for _, ca := range subCaches {
		pr, pw := io.Pipe()
		writers = append(writers, pw)
		closers = append(closers, pw)
		wg.Add(1)
		go func(cache *cache.Cache, pr io.Reader) {
			defer wg.Done()
			_, err := cache.PutBlob(ctx, blob, pr, contentType)
			if err != nil {
				r.logger.Error("put blob failed", "digest", blob, "error", err)
				io.Copy(io.Discard, pr)
				return
			}
		}(ca, pr)
	}

	n, err := io.Copy(io.MultiWriter(writers...), body)
	if err != nil {
		return fmt.Errorf("copy blob failed: %w", err)
	}
	for _, c := range closers {
		c.Close()
	}

	wg.Wait()

	r.logger.Info("finish sync blob", "digest", blob, "size", n)
	return nil
}

func (r *Runner) blobSync(ctx context.Context, resp client.MessageResponse) error {
	var errCh = make(chan error, 1)

	var gotSize, progress atomic.Int64

	go func() {
		errCh <- r.blob(ctx, resp.Data.Host, resp.Data.Image, resp.Content, resp.Data.Size, &gotSize, &progress)
	}()

	return r.heartbeat(ctx, resp.MessageID, &gotSize, &progress, errCh)
}

type readerCounter struct {
	r       io.Reader
	counter *atomic.Int64
}

func (r *readerCounter) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)

	r.counter.Add(int64(n))
	return n, err
}

type skipWriter struct {
	writer  io.Writer
	offset  uint64
	skipped uint64
}

func (w *skipWriter) Write(p []byte) (int, error) {
	if w.skipped >= w.offset {
		return w.writer.Write(p)
	}

	remaining := w.offset - w.skipped
	if uint64(len(p)) <= remaining {
		w.skipped += uint64(len(p))
		return len(p), nil
	}

	w.skipped = w.offset
	written, err := w.writer.Write(p[remaining:])
	return int(remaining) + written, err
}
