package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenCIDN/OpenCIDN/internal/spec"
	"github.com/OpenCIDN/OpenCIDN/pkg/cache"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/client"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/model"
)

type Runner struct {
	bigCacheSize  int
	bigCache      *cache.Cache
	manifestCache *cache.Cache

	caches      []*cache.Cache
	httpClient  *http.Client
	queueClient *client.MessageClient
	lease       string

	pendingMut  sync.Mutex
	blobPending map[int64]client.MessageResponse
	syncBlobCh  chan struct{}

	manifestPending map[int64]client.MessageResponse
	syncManifestCh  chan struct{}

	filterPlatform func(pf spec.Platform) bool

	logger *slog.Logger
}

type Option func(r *Runner)

func WithHttpClient(httpClient *http.Client) Option {
	return func(r *Runner) {
		r.httpClient = httpClient
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(r *Runner) {
		r.logger = logger
	}
}

func WithCaches(caches ...*cache.Cache) Option {
	return func(r *Runner) {
		r.caches = caches
	}
}

func WithFilterPlatform(filterPlatform func(pf spec.Platform) bool) Option {
	return func(r *Runner) {
		r.filterPlatform = filterPlatform
	}
}

func WithQueueClient(queueClient *client.MessageClient) Option {
	return func(r *Runner) {
		r.queueClient = queueClient
	}
}

func WithLease(lease string) Option {
	return func(r *Runner) {
		r.lease = lease
	}
}

func WithBigCache(cache *cache.Cache, size int) Option {
	return func(c *Runner) {
		c.bigCache = cache
		c.bigCacheSize = size
	}
}

func WithManifestCache(cache *cache.Cache) Option {
	return func(c *Runner) {
		c.manifestCache = cache
	}
}

func NewRunner(opts ...Option) (*Runner, error) {
	r := &Runner{
		httpClient:      http.DefaultClient,
		blobPending:     map[int64]client.MessageResponse{},
		syncBlobCh:      make(chan struct{}, 1),
		manifestPending: map[int64]client.MessageResponse{},
		syncManifestCh:  make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.queueClient == nil {
		return nil, fmt.Errorf("queueClient must not be nil")
	}

	if r.lease == "" {
		return nil, fmt.Errorf("lease must not be empty")
	}

	if len(r.caches) == 0 {
		return nil, fmt.Errorf("at least one cache must be provided")
	}

	return r, nil

}

func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("lease", "id", r.lease)

	r.sync(ctx)
	return ctx.Err()
}

func (r *Runner) watch(ctx context.Context) {
	for ctx.Err() == nil {
		err := r.runWatch(ctx)
		if err != nil {
			r.logger.Error("watch", "error", err)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) sync(ctx context.Context) {
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.watch(watchCtx)

	wg := sync.WaitGroup{}

	wg.Add(4)
	go func() {
		defer wg.Done()
		r.runBlobSync(ctx)
	}()
	go func() {
		defer wg.Done()
		r.runManifestSync(ctx, false)
	}()
	for i := 0; i != 2; i++ {
		go func() {
			defer wg.Done()
			r.runManifestSync(ctx, true)
		}()
	}
	wg.Wait()
}

func (r *Runner) runWatch(ctx context.Context) error {
	ch, err := r.queueClient.WatchList(ctx)
	if err != nil {
		return err
	}

	r.pendingMut.Lock()
	clear(r.blobPending)
	clear(r.manifestPending)
	r.pendingMut.Unlock()

	for msg := range ch {
		switch msg.Data.Kind {
		case model.KindBlob:
			r.pendingMut.Lock()
			if msg.Status == model.StatusPending {
				r.blobPending[msg.MessageID] = msg
			} else {
				delete(r.blobPending, msg.MessageID)
			}
			r.pendingMut.Unlock()

			if msg.Status == model.StatusPending {
				select {
				case r.syncBlobCh <- struct{}{}:
				default:
				}
			}
		case model.KindManifest:
			r.pendingMut.Lock()
			if msg.Status == model.StatusPending {
				r.manifestPending[msg.MessageID] = msg
			} else {
				delete(r.manifestPending, msg.MessageID)
			}
			r.pendingMut.Unlock()

			if msg.Status == model.StatusPending {
				select {
				case r.syncManifestCh <- struct{}{}:
				default:
				}
			}
		}
	}
	return nil
}

var errWait = fmt.Errorf("no message received and no errors occurred")

func (r *Runner) heartbeat(ctx context.Context, messageID int64, gotSize, progress *atomic.Int64, errCh chan error) error {
	ticker := time.NewTicker(time.Millisecond * time.Duration(100+rand.Int32N(900)))
	defer ticker.Stop()

	var prevProgress int64 = -1

	for {
		select {
		case <-ticker.C:
			p := progress.Load()
			if prevProgress == p {
				prevProgress = -1
				continue
			}

			prevProgress = p

			err := r.queueClient.Heartbeat(ctx, messageID, client.HeartbeatRequest{
				Lease: r.lease,
				Data: model.MessageAttr{
					Size:     gotSize.Load(),
					Progress: p,
				},
			})

			if err != nil {
				r.logger.Error("Heartbeat", "error", err)
			}

			ticker.Reset(10*time.Second + time.Millisecond*time.Duration(rand.UintN(1000)))
		case err := <-errCh:
			if err == nil {
				_ = r.queueClient.Heartbeat(ctx, messageID, client.HeartbeatRequest{
					Lease: r.lease,
					Data: model.MessageAttr{
						Size:     gotSize.Load(),
						Progress: progress.Load(),
					},
				})
				return r.queueClient.Complete(ctx, messageID, client.CompletedRequest{
					Lease: r.lease,
				})
			}

			if errors.Is(err, context.Canceled) {
				err0 := r.queueClient.Cancel(ctx, messageID, client.CancelRequest{
					Lease: r.lease,
				})
				if err0 != nil {
					return errors.Join(err, err0)
				}

				return err
			}

			err0 := r.queueClient.Failed(ctx, messageID, client.FailedRequest{
				Lease: r.lease,
				Data: model.MessageAttr{
					Error: err.Error(),
				},
			})
			if err0 != nil {
				return errors.Join(err, err0)
			}

			return err
		case <-ctx.Done():
			return r.queueClient.Cancel(ctx, messageID, client.CancelRequest{
				Lease: r.lease,
			})
		}
	}
}
