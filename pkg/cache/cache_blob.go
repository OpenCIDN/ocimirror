package cache

import (
	"bytes"
	"context"
	"io"

	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/wzshiming/sss"
)

func (c *Cache) RedirectBlob(ctx context.Context, blob string, referer string) (string, error) {
	return c.Redirect(ctx, registry.BlobCachePath(blob), referer)
}

func (c *Cache) StatBlob(ctx context.Context, blob string) (sss.FileInfo, error) {
	return c.Stat(ctx, registry.BlobCachePath(blob))
}

func (c *Cache) PutBlob(ctx context.Context, blob string, r io.Reader) (int64, error) {
	cachePath := registry.BlobCachePath(blob)
	return c.PutWithHash(ctx, cachePath, r, registry.CleanDigest(blob), 0)
}

func (c *Cache) PutBlobContent(ctx context.Context, blob string, content []byte) (int64, error) {
	cachePath := registry.BlobCachePath(blob)
	return c.PutWithHash(ctx, cachePath, bytes.NewBuffer(content), registry.CleanDigest(blob), int64(len(content)))
}

func (c *Cache) GetBlob(ctx context.Context, blob string) (io.ReadCloser, error) {
	cachePath := registry.BlobCachePath(blob)
	return c.Get(ctx, cachePath)
}

func (c *Cache) GetBlobWithOffset(ctx context.Context, blob string, offset int64) (io.ReadCloser, error) {
	cachePath := registry.BlobCachePath(blob)
	return c.GetWithOffset(ctx, cachePath, offset)
}

func (c *Cache) DeleteBlob(ctx context.Context, blob string) error {
	cachePath := registry.BlobCachePath(blob)
	return c.Delete(ctx, cachePath)
}

func (c *Cache) GetBlobContent(ctx context.Context, blob string) ([]byte, error) {
	r, err := c.GetBlob(ctx, blob)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
