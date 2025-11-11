package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/wzshiming/imc"
	"github.com/wzshiming/sss"
)

type Cache struct {
	bytesPool     sync.Pool
	storageDriver *sss.SSS
	linkExpires   time.Duration
	signLink      bool
	redirectLinks *url.URL

	blobCache    *imc.Cache[string, errPair[sss.FileInfo]]
	blobCacheTTL time.Duration

	manifestTagCache *imc.Cache[string, errPair[string]]
	manifestCache    *imc.Cache[string, errPair[*Manifest]]
	manifestCacheTTL time.Duration
}

type errPair[T any] struct {
	Value T
	Error error
}

type Option func(c *Cache)

func WithManifestCache(ttl time.Duration) Option {
	return func(c *Cache) {
		c.manifestCacheTTL = ttl
		c.manifestCache = imc.NewCache[string, errPair[*Manifest]]()
		c.manifestTagCache = imc.NewCache[string, errPair[string]]()
	}
}

func WithBlobCache(ttl time.Duration) Option {
	return func(c *Cache) {
		c.blobCacheTTL = ttl
		c.blobCache = imc.NewCache[string, errPair[sss.FileInfo]]()
	}
}

func WithLinkExpires(d time.Duration) Option {
	return func(c *Cache) {
		c.linkExpires = d
	}
}

func WithRedirectLinks(l *url.URL) Option {
	return func(c *Cache) {
		c.redirectLinks = l
	}
}

func WithStorageDriver(storageDriver *sss.SSS) Option {
	return func(c *Cache) {
		c.storageDriver = storageDriver
	}
}

func WithSignLink(signLink bool) Option {
	return func(c *Cache) {
		c.signLink = signLink
	}
}

func NewCache(opts ...Option) (*Cache, error) {
	c := &Cache{
		bytesPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 32*1024)
			},
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	_, err := c.put(context.Background(), "opencidn.txt", bytes.NewBufferString("opencidn"), nil)
	if err != nil {
		return nil, err
	}

	if c.manifestCache != nil {
		go c.manifestCache.RunEvict(context.Background(), func(key string, value errPair[*Manifest]) bool {
			return true
		})
	}

	if c.manifestTagCache != nil {
		go c.manifestTagCache.RunEvict(context.Background(), func(key string, value errPair[string]) bool {
			return true
		})
	}

	if c.blobCache != nil {
		go c.blobCache.RunEvict(context.Background(), func(key string, value errPair[sss.FileInfo]) bool {
			return true
		})
	}
	return c, nil
}

func (c *Cache) Redirect(ctx context.Context, blobPath string, referer string) (string, error) {
	if !c.signLink && c.redirectLinks != nil {
		u, err := c.redirectLinks.Parse(strings.TrimPrefix(blobPath, "/"))
		if err != nil {
			return "", err
		}
		return u.String(), nil
	}

	linkExpires := c.linkExpires

	u, err := c.storageDriver.SignGet(blobPath, linkExpires)
	if err != nil {
		return "", err
	}

	if c.redirectLinks != nil {
		uri, err := url.Parse(u)
		if err == nil {
			uri.Scheme = c.redirectLinks.Scheme
			uri.Host = c.redirectLinks.Host
			u = uri.String()
		}
	}
	return u, nil
}

func (c *Cache) Writer(ctx context.Context, cachePath string, append bool) (sss.FileWriter, error) {
	if append {
		return c.storageDriver.WriterWithAppend(ctx, cachePath)
	}
	return c.storageDriver.Writer(ctx, cachePath)
}

func (c *Cache) BlobWriter(ctx context.Context, blob string, append bool) (sss.FileWriter, error) {
	cachePath := registry.BlobCachePath(blob)

	if append {
		return c.Writer(ctx, cachePath, true)
	}
	fw, err := c.Writer(ctx, cachePath, false)
	if err != nil {
		return nil, err
	}

	return &blobWriter{
		FileWriter: fw,
		h:          sha256.New(),
		cacheHash:  registry.CleanDigest(blob),
	}, nil
}

func (c *Cache) put(ctx context.Context, cachePath string, r io.Reader, checkFunc func(int64) error) (int64, error) {
	fw, err := c.storageDriver.Writer(ctx, cachePath)
	if err != nil {
		return 0, err
	}

	buf := c.bytesPool.Get().([]byte)
	defer c.bytesPool.Put(buf)

	n, err := io.CopyBuffer(fw, r, buf)
	if err != nil {
		return 0, err
	}

	if checkFunc != nil {
		err = checkFunc(n)
		if err != nil {
			return 0, err
		}
	}

	err = fw.Commit(ctx)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *Cache) Put(ctx context.Context, cachePath string, r io.Reader) (int64, error) {
	return c.put(ctx, cachePath, r, nil)
}

func (c *Cache) PutContent(ctx context.Context, cachePath string, content []byte) error {
	return c.storageDriver.PutContent(ctx, cachePath, content)
}

func (c *Cache) PutWithHash(ctx context.Context, cachePath string, r io.Reader, cacheHash string, cacheSize int64) (int64, error) {
	h := sha256.New()
	return c.put(ctx, cachePath, io.TeeReader(r, h), func(i int64) error {
		if cacheSize > 0 && i != cacheSize {
			return fmt.Errorf("expected %d bytes, got %d", cacheSize, i)
		}
		hash := hex.EncodeToString(h.Sum(nil)[:])
		if cacheHash != hash {
			return fmt.Errorf("expected %s hash, got %s", cacheHash, hash)
		}
		return nil
	})
}

func (c *Cache) Delete(ctx context.Context, cachePath string) error {
	return c.storageDriver.Delete(ctx, cachePath)
}

func (c *Cache) Get(ctx context.Context, cachePath string) (io.ReadCloser, error) {
	return c.storageDriver.Reader(ctx, cachePath)
}

func (c *Cache) GetWithOffset(ctx context.Context, cachePath string, offset int64) (io.ReadCloser, error) {
	return c.storageDriver.ReaderWithOffset(ctx, cachePath, offset)
}

func (c *Cache) GetContent(ctx context.Context, cachePath string) ([]byte, error) {
	return c.storageDriver.GetContent(ctx, cachePath)
}

func (c *Cache) Stat(ctx context.Context, cachePath string) (sss.FileInfo, error) {
	return c.storageDriver.Stat(ctx, cachePath)
}

func (c *Cache) Walk(ctx context.Context, cachePath string, fun fs.WalkDirFunc) error {
	return c.storageDriver.Walk(ctx, cachePath, func(fi sss.FileInfo) error {
		p := fi.Path()
		fiw := fileInfoWrap{
			name:     path.Base(p),
			FileInfo: fi,
		}

		return fun(path.Dir(p), fiw, nil)
	})
}

func (c *Cache) List(ctx context.Context, cachePath string) ([]string, error) {
	list := []string{}
	err := c.storageDriver.List(ctx, cachePath, func(fileInfo sss.FileInfo) bool {
		list = append(list, fileInfo.Path())
		return true
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

type fileInfoWrap struct {
	name string
	sss.FileInfo
}

var _ fs.DirEntry = (*fileInfoWrap)(nil)

func (f fileInfoWrap) Name() string {
	return f.name
}

func (fileInfoWrap) Mode() fs.FileMode {
	return 0666
}

func (fileInfoWrap) Type() fs.FileMode {
	return 0
}

func (fileInfoWrap) Sys() any {
	return nil
}

func (f fileInfoWrap) Info() (fs.FileInfo, error) {
	return f, nil
}
