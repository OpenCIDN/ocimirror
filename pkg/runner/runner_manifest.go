package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenCIDN/OpenCIDN/internal/spec"
	"github.com/OpenCIDN/OpenCIDN/pkg/cache"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/client"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/model"
)

func (r *Runner) runManifestSync(ctx context.Context, deep bool) {
	for {
		err := r.runOnceManifestSync(context.Background(), deep)
		if err != nil {
			if err != errWait {
				r.logger.Warn("runOnceManifestSync", "error", err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case <-r.syncManifestCh:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *Runner) getManifestPending(deep bool) []client.MessageResponse {
	r.pendingMut.Lock()
	defer r.pendingMut.Unlock()

	var pendingMessages []client.MessageResponse
	for _, msg := range r.manifestPending {
		if msg.Status == model.StatusPending && msg.Data.Deep == deep {
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

func (r *Runner) runOnceManifestSync(ctx context.Context, deep bool) error {
	var (
		err  error
		errs []error
		resp client.MessageResponse
	)

	pending := r.getManifestPending(deep)
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

	return r.manifestSync(ctx, resp)
}

func (r *Runner) manifestSync(ctx context.Context, resp client.MessageResponse) error {
	var fullImage string
	var tagOrBlob string
	if i := strings.Index(resp.Content, "@"); i > 0 {
		fullImage = resp.Content[:i]
		tagOrBlob = resp.Content[i+1:]
	} else if i := strings.Index(resp.Content, ":"); i > 0 {
		fullImage = resp.Content[:i]
		tagOrBlob = resp.Content[i+1:]
	} else {
		_ = r.queueClient.Failed(ctx, resp.MessageID, client.FailedRequest{
			Lease: r.lease,
			Data: model.MessageAttr{
				Error: "invalid content format",
			},
		})
		return fmt.Errorf("invalid content format: %s", resp.Content)
	}

	hostAndImage := strings.SplitN(fullImage, "/", 2)
	if len(hostAndImage) != 2 {
		_ = r.queueClient.Failed(ctx, resp.MessageID, client.FailedRequest{
			Lease: r.lease,
			Data: model.MessageAttr{
				Error: "invalid content format",
			},
		})
		return fmt.Errorf("invalid content format: %s", resp.Content)
	}
	host := hostAndImage[0]
	image := hostAndImage[1]

	var errCh = make(chan error, 1)
	var gotSize, progress atomic.Int64
	go func() {
		errCh <- r.manifest(ctx, resp.MessageID, host, image, tagOrBlob, resp.Data.Deep, resp.Priority, &gotSize, &progress)
	}()

	return r.heartbeat(ctx, resp.MessageID, &gotSize, &progress, errCh)
}

var acceptsStr = "application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"

func (r *Runner) manifest(ctx context.Context, messageID int64, host, image, tagOrBlob string, deep bool, priority int, gotSize, progress *atomic.Int64) error {

	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", image, tagOrBlob),
	}

	var caches []*cache.Cache

	if r.manifestCache != nil {
		caches = append([]*cache.Cache{r.manifestCache}, r.caches...)
	} else {
		caches = append(caches, r.caches...)
	}

	var subCaches []*cache.Cache

	if strings.HasPrefix(tagOrBlob, "sha256:") {
		for _, cache := range caches {
			exist, _ := cache.StatManifest(ctx, host, image, tagOrBlob)
			if !exist {
				subCaches = append(subCaches, cache)
			}
		}
	} else if u.Host != "ollama.com" {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", acceptsStr)
		resp, err := r.httpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.Body != nil {
			_ = resp.Body.Close()
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to head manifest: status code %d", resp.StatusCode)
		}

		digest := resp.Header.Get("Docker-Content-Digest")
		if digest != "" {
			for _, cache := range caches {
				exist, _ := cache.StatOrRelinkManifest(ctx, host, image, tagOrBlob, digest)
				if !exist {
					subCaches = append(subCaches, cache)
				}
			}
		} else {
			for _, cache := range caches {
				exist, _ := cache.StatManifest(ctx, host, image, tagOrBlob)
				if !exist {
					subCaches = append(subCaches, cache)
				}
			}
		}
	} else {
		for _, cache := range caches {
			exist, _ := cache.StatManifest(ctx, host, image, tagOrBlob)
			if !exist {
				subCaches = append(subCaches, cache)
			}
		}
	}

	if len(subCaches) == 0 {
		r.logger.Info("skip manifest by cache", "host", host, "image", image, "tagOrBlob", tagOrBlob)
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", acceptsStr)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get manifest: status code %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	contentType := resp.Header.Get("Content-Type")

	_ = r.queueClient.Heartbeat(ctx, messageID, client.HeartbeatRequest{
		Lease: r.lease,
		Data: model.MessageAttr{
			Host:  host,
			Image: image,
		},
	})
	r.logger.Info("start sync maifest", "url", u.String())

	if !deep {
		for _, cache := range subCaches {
			_, _, _, err := cache.PutManifestContent(ctx, host, image, tagOrBlob, body, contentType)
			if err != nil {
				r.logger.Error("PutManifest", "error", err)
			}
		}
		return nil
	}

	{
		m := spec.IndexManifestLayers{}
		json.Unmarshal(body, &m)
		if len(m.Manifests) != 0 {
			wg := sync.WaitGroup{}

			for _, l := range m.Manifests {
				if r.filterPlatform != nil {
					if !r.filterPlatform(l.Platform) {
						continue
					}
				}

				msg := fmt.Sprintf("%s/%s@%s", host, image, l.Digest)
				r.logger.Info("Create manifest", "msg", msg)
				mr, err := r.queueClient.Create(ctx, msg, priority, model.MessageAttr{
					Kind:  model.KindBlob,
					Host:  host,
					Image: image,
					Size:  l.Size,
				})
				if err != nil {
					return err
				}

				mrCh, err := r.queueClient.Watch(ctx, mr.MessageID)
				if err != nil {
					return err
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					var prevSize int64
					var prevProgress int64
					for m := range mrCh {
						progress.Add(m.Data.Progress - prevProgress)
						prevProgress = m.Data.Progress

						gotSize.Add(m.Data.Size - prevSize)
						prevSize = m.Data.Size
					}

					progress.Add(prevSize - prevProgress)
				}()
			}
			wg.Wait()

			for _, cache := range subCaches {
				_, _, _, err := cache.PutManifestContent(ctx, host, image, tagOrBlob, body, contentType)
				if err != nil {
					r.logger.Error("PutManifest", "error", err)
				}
			}
			return nil
		}
	}
	{
		m := spec.ManifestLayers{}
		json.Unmarshal(body, &m)
		if len(m.Layers) != 0 {
			wg := sync.WaitGroup{}

			for _, l := range m.Layers {
				gotSize.Add(l.Size)

				r.logger.Info("Create blob", "msg", l.Digest)
				mr, err := r.queueClient.Create(ctx, l.Digest, priority, model.MessageAttr{
					Kind:  model.KindBlob,
					Host:  host,
					Image: image,
					Size:  l.Size,
				})
				if err != nil {
					return err
				}

				mrCh, err := r.queueClient.Watch(ctx, mr.MessageID)
				if err != nil {
					return err
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					var prevProgress int64
					for m := range mrCh {
						progress.Add(m.Data.Progress - prevProgress)
						prevProgress = m.Data.Progress
					}

					progress.Add(l.Size - prevProgress)
				}()
			}

			l := m.Config

			gotSize.Add(l.Size)

			r.logger.Info("Create config blob", "msg", l.Digest)
			mr, err := r.queueClient.Create(ctx, l.Digest, priority, model.MessageAttr{
				Kind:  model.KindBlob,
				Host:  host,
				Image: image,
				Size:  l.Size,
			})
			if err != nil {
				return err
			}
			mrCh, err := r.queueClient.Watch(ctx, mr.MessageID)
			if err != nil {
				return err
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				var prevProgress int64
				for m := range mrCh {
					progress.Add(m.Data.Progress - prevProgress)
					prevProgress = m.Data.Progress
				}

				progress.Add(l.Size - prevProgress)
			}()

			wg.Wait()

			for _, cache := range subCaches {
				_, _, _, err := cache.PutManifestContent(ctx, host, image, tagOrBlob, body, contentType)
				if err != nil {
					r.logger.Error("PutManifest", "error", err)
				}
			}

			return nil
		}
	}

	return fmt.Errorf("failed to sync manifest content: no valid manifest layers found")
}
