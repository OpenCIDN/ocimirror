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

func (c *Cache) CleanCacheStatBlobIfError(blob string) {
	if c.blobCache == nil {
		return
	}
	info, ok := c.blobCache.Get(blob)
	if ok && info.Error != nil {
		c.blobCache.Remove(blob)
	}
}

func (c *Cache) CleanCacheStatManifestIfError(manifest string) {
	if c.manifestCache == nil {
		return
	}
	info, ok := c.manifestCache.Get(manifest)
	if ok && info.Error != nil {
		c.manifestCache.Remove(manifest)
	}
}

func (c *Cache) CleanCacheStatManifestTag(manifest string) {
	if c.manifestTagCache == nil {
		return
	}

	c.manifestTagCache.Remove(manifest)
}

func (c *Cache) StatBlob(ctx context.Context, blob string) (sss.FileInfo, error) {
	if c.blobCache == nil {
		return c.Stat(ctx, registry.BlobCachePath(blob))
	}

	if info, ok := c.blobCache.Get(blob); ok {
		if info.Error != nil {
			return nil, info.Error
		}
		return info.Value, nil
	}

	info, err := c.Stat(ctx, registry.BlobCachePath(blob))
	if err != nil {
		c.blobCache.SetWithTTL(blob, errPair[sss.FileInfo]{
			Error: err,
		}, c.blobCacheTTL)
		return nil, err
	}
	c.blobCache.SetWithTTL(blob, errPair[sss.FileInfo]{
		Value: info,
	}, c.blobCacheTTL)
	return info, nil
}

func (c *Cache) PutBlob(ctx context.Context, blob string, r io.Reader) (int64, error) {
	cachePath := registry.BlobCachePath(blob)
	defer c.CleanCacheStatBlobIfError(blob)
	return c.PutWithHash(ctx, cachePath, r, registry.CleanDigest(blob), 0)
}

func (c *Cache) PutBlobContent(ctx context.Context, blob string, content []byte) (int64, error) {
	cachePath := registry.BlobCachePath(blob)
	defer c.CleanCacheStatBlobIfError(blob)
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
	err := c.Delete(ctx, cachePath)
	if c.blobCache != nil {
		c.blobCache.Remove(blob)
	}
	return err
}

func (c *Cache) GetBlobContent(ctx context.Context, blob string) ([]byte, error) {
	r, err := c.GetBlob(ctx, blob)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
