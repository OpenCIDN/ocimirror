package blobs

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/wzshiming/ioswmr"
)

type teeBlob struct {
	swmr ioswmr.SWMR
	size int64
}

// startTeeBlob initiates a streaming download of a blob that is simultaneously
// written to the persistent cache and made available for reading by one or more
// HTTP clients. It returns a *teeBlob whose SWMR can be used to serve the blob
// content before the download is complete.
func (b *Blobs) startTeeBlob(ctx context.Context, info *BlobInfo) (*teeBlob, error) {
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", info.Image, info.Blobs),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		resp.Body.Close()
		return nil, fmt.Errorf("upstream denied with status %d", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status %d from upstream", resp.StatusCode)
	}

	size := resp.ContentLength

	fw, err := b.cache.BlobWriter(ctx, info.Blobs, false)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}

	digest := info.Blobs

	// tee is declared before the SWMR so that AfterCloseFunc can reference it
	// with a pointer-equality check via CompareAndDelete.
	var tee *teeBlob

	var swmrOpts []ioswmr.Option
	swmrOpts = append(swmrOpts, ioswmr.WithAutoClose())
	swmrOpts = append(swmrOpts, ioswmr.WithAfterCloseFunc(func(_ error) error {
		// Only remove our entry from the map; leave it alone if another tee
		// has already replaced ours (e.g. due to the LoadOrStore race).
		b.teeCache.CompareAndDelete(digest, tee)
		return nil
	}))
	if size > 0 && size <= math.MaxInt {
		swmrOpts = append(swmrOpts, ioswmr.WithLength(int(size)))
	}

	swmr := ioswmr.NewSWMR(
		ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) {
			return os.CreateTemp("", "ocimirror-tee-*")
		}),
		swmrOpts...,
	)

	tee = &teeBlob{
		swmr: swmr,
		size: size,
	}

	b.logger.Info("starting tee blob download", "digest", digest, "size", size)

	go func() {
		w := swmr.Writer()
		defer resp.Body.Close()
		defer fw.Close()

		mw := io.MultiWriter(w, fw)
		n, copyErr := io.Copy(mw, resp.Body)

		if copyErr != nil {
			b.logger.Warn("tee blob copy error", "digest", digest, "error", copyErr)
			_ = fw.Cancel(context.Background())
			_ = w.CloseWithError(copyErr)
			return
		}

		if size > 0 && n != size {
			sizeErr := fmt.Errorf("tee blob size mismatch: expected %d, got %d", size, n)
			b.logger.Warn("tee blob size error", "digest", digest, "error", sizeErr)
			_ = fw.Cancel(context.Background())
			_ = w.CloseWithError(sizeErr)
			return
		}

		commitErr := fw.Commit(context.Background())
		if commitErr != nil {
			b.logger.Warn("tee blob commit error", "digest", digest, "error", commitErr)
			_ = w.CloseWithError(commitErr)
			return
		}

		b.cache.CleanCacheStatBlob(digest)
		b.logger.Info("tee blob cached", "digest", digest, "size", n)
		_ = w.Close()
	}()

	return tee, nil
}

// serveTeeBlob serves an in-progress tee blob download to an HTTP client.
// When the blob size is known it uses http.ServeContent so that range requests
// are supported; otherwise it streams the data directly.
func (b *Blobs) serveTeeBlob(rw http.ResponseWriter, r *http.Request, tee *teeBlob) {
	size := tee.size
	rw.Header().Set("Content-Type", "application/octet-stream")

	if size > 0 && size <= math.MaxInt {
		rs := tee.swmr.NewReadSeeker(0, int(size))
		defer rs.Close()
		http.ServeContent(rw, r, "", time.Time{}, rs)
	} else {
		rc := tee.swmr.NewReader(0)
		defer rc.Close()
		rw.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = io.Copy(rw, rc)
		}
	}
}

// serveTee looks up or creates a teeBlob for the given blob digest, then
// serves it to the HTTP client. It returns true if the tee path was used,
// false if an error prevented the tee from being started (the caller should
// fall back to the regular caching path).
func (b *Blobs) serveTee(rw http.ResponseWriter, r *http.Request, info *BlobInfo) bool {
	// Check if an in-progress tee already exists for this digest.
	if v, ok := b.teeCache.Load(info.Blobs); ok {
		b.serveTeeBlob(rw, r, v.(*teeBlob))
		return true
	}

	itee, err, _ := b.flight.Do(info.Blobs, func() (any, error) {
		// Start a new tee download.
		tee, err := b.startTeeBlob(r.Context(), info)
		if err != nil {
			b.logger.Warn("failed to start tee blob", "digest", info.Blobs, "error", err)
			return nil, err
		}

		// Atomically register the tee so that concurrent requests for the same blob
		// share this download. If another goroutine already registered one we use
		// ours; the duplicate background download is harmless (identical content).
		actual, loaded := b.teeCache.LoadOrStore(info.Blobs, tee)
		if loaded {
			return actual.(*teeBlob), nil
		}

		return tee, nil
	})
	if err != nil {
		return false
	}

	b.serveTeeBlob(rw, r, itee.(*teeBlob))
	return true
}
