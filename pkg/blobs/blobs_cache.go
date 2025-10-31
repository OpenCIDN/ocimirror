package blobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/wzshiming/imc"
)

type blobsCache struct {
	digest   *imc.Cache[string, blobValue]
	duration time.Duration
}

func newBlobsCache(duration time.Duration) *blobsCache {
	return &blobsCache{
		digest:   imc.NewCache[string, blobValue](),
		duration: duration,
	}
}

func (c *blobsCache) Start(ctx context.Context, logger *slog.Logger) {
	go c.digest.RunEvict(ctx, func(key string, value blobValue) bool {
		if value.Error != nil {
			logger.Info("evict blob error", "key", key, "error", value.Error)
		} else {
			logger.Info("evict blob", "key", key)
		}
		return true
	})

}

func (c *blobsCache) Get(key string) (blobValue, bool) {
	return c.digest.Get(key)
}

func (c *blobsCache) Remove(key string) {
	c.digest.Remove(key)
}

func (c *blobsCache) PutError(key string, err error, sc int) {
	c.digest.SetWithTTL(key, blobValue{
		Error:      err,
		StatusCode: sc,
	}, c.duration)
}

func (c *blobsCache) Put(key string, modTime time.Time, size int64) {
	c.digest.SetWithTTL(key, blobValue{
		Size:    size,
		ModTime: modTime,
	}, c.duration)
}

func (c *blobsCache) PutNoTTL(key string, modTime time.Time, size int64) {
	c.digest.Set(key, blobValue{
		Size:    size,
		ModTime: modTime,
	})
}

type blobValue struct {
	Size       int64
	ModTime    time.Time
	Error      error
	StatusCode int
}
