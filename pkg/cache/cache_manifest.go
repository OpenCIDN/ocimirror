package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/slices"
)

func (c *Cache) RelinkManifest(ctx context.Context, host, image, tag string, blob string) error {
	blob = registry.EnsureDigestPrefix(blob)

	c.CleanCacheStatBlobIfError(blob)
	_, err := c.StatBlob(ctx, blob)
	if err != nil {
		return err
	}

	if tag != "" {
		if c.manifestCache != nil {
			cacheKey := host + "/" + image + ":" + tag
			c.CleanCacheStatManifestTag(cacheKey)
		}
		manifestLinkPath := registry.ManifestTagCachePath(host, image, tag)
		err = c.PutContent(ctx, manifestLinkPath, []byte(blob))
		if err != nil {
			return fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	c.CleanCacheStatManifestIfError(blob)
	manifestBlobLinkPath := registry.ManifestRevisionsCachePath(host, image, blob)
	err = c.PutContent(ctx, manifestBlobLinkPath, []byte(blob))
	if err != nil {
		return fmt.Errorf("put manifest revisions path %s error: %w", manifestBlobLinkPath, err)
	}

	return nil
}

func (c *Cache) PutManifestContent(ctx context.Context, host, image, tagOrBlob string, content []byte) error {
	if !json.Valid(content) {
		return errors.New("invalid manifest content")
	}

	h := sha256.New()
	h.Write(content)
	hash := "sha256:" + hex.EncodeToString(h.Sum(nil)[:])

	isHash := strings.HasPrefix(tagOrBlob, "sha256:")
	if isHash {
		if tagOrBlob != hash {
			return fmt.Errorf("expected hash %s is not same to %s", tagOrBlob, hash)
		}
	} else {
		manifestLinkPath := registry.ManifestTagCachePath(host, image, tagOrBlob)
		err := c.PutContent(ctx, manifestLinkPath, []byte(hash))
		if err != nil {
			return fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	manifestLinkPath := registry.ManifestRevisionsCachePath(host, image, hash)
	err := c.PutContent(ctx, manifestLinkPath, []byte(hash))
	if err != nil {
		return fmt.Errorf("put manifest revisions path %s error: %w", manifestLinkPath, err)
	}

	_, err = c.PutBlobContent(ctx, hash, content)
	if err != nil {
		return fmt.Errorf("put manifest blob path %s error: %w", hash, err)
	}

	if c.manifestCache != nil {
		if isHash {
			c.CleanCacheStatManifestIfError(hash)
		} else {
			cacheKey := host + "/" + image + ":" + tagOrBlob
			c.CleanCacheStatManifestTag(cacheKey)
			c.CleanCacheStatManifestIfError(hash)
		}
	}

	c.CleanCacheStatBlobIfError(hash)
	return nil
}

func (c *Cache) GetManifestContent(ctx context.Context, host, image, tagOrBlob string) (manifest *Manifest, err error) {
	var manifestLinkPath string
	isHash := strings.HasPrefix(tagOrBlob, "sha256:")

	if c.manifestCache != nil {
		if isHash {
			if mc, ok := c.manifestCache.Get(tagOrBlob); ok {
				if mc.Error != nil {
					return nil, mc.Error
				}
				return mc.Value, nil
			}
			defer func() {
				c.manifestCache.SetWithTTL(tagOrBlob, errPair[*Manifest]{
					Value: manifest,
					Error: err,
				}, c.manifestCacheTTL)
			}()
		} else {
			cacheKey := host + "/" + image + ":" + tagOrBlob
			if mtc, ok := c.manifestTagCache.Get(cacheKey); ok {
				if mtc.Error != nil {
					return nil, mtc.Error
				}
				if mc, ok := c.manifestCache.Get(mtc.Value); ok {
					if mc.Error != nil {
						return nil, mc.Error
					}
					return mc.Value, nil
				}
			}
			defer func() {
				if err != nil {
					c.manifestTagCache.SetWithTTL(cacheKey, errPair[string]{
						Error: err,
					}, c.manifestCacheTTL)
				} else {
					c.manifestTagCache.SetWithTTL(cacheKey, errPair[string]{
						Value: manifest.Digest,
					}, c.manifestCacheTTL)
					c.manifestCache.SetWithTTL(manifest.Digest, errPair[*Manifest]{
						Value: manifest,
						Error: err,
					}, c.manifestCacheTTL)
				}
			}()
		}
	}

	if isHash {
		manifestLinkPath = registry.ManifestRevisionsCachePath(host, image, tagOrBlob[7:])
	} else {
		manifestLinkPath = registry.ManifestTagCachePath(host, image, tagOrBlob)
	}

	digestContent, err := c.GetContent(ctx, manifestLinkPath)
	if err != nil {
		return nil, fmt.Errorf("get manifest link path %s error: %w", manifestLinkPath, err)
	}
	digest := string(digestContent)
	content, err := c.GetBlobContent(ctx, digest)
	if err != nil {
		cleanErr := c.Delete(ctx, manifestLinkPath)
		if cleanErr != nil {
			err = errors.Join(err, cleanErr)
		}
		return nil, fmt.Errorf("get manifest blob %s error: %w", digest, err)
	}

	mediaType, err := getMediaType(content)
	if err != nil {
		cleanErr := c.DeleteBlob(ctx, digest)
		if cleanErr != nil {
			err = errors.Join(err, cleanErr)
		}
		cleanErr = c.Delete(ctx, manifestLinkPath)
		if cleanErr != nil {
			err = errors.Join(err, cleanErr)
		}
		return nil, fmt.Errorf("invalid content: %w: %s", err, string(content))
	}

	return &Manifest{
		Content:   content,
		Digest:    digest,
		MediaType: mediaType,
	}, nil
}

type Manifest struct {
	Content   []byte
	Digest    string
	MediaType string
}

func getMediaType(content []byte) (string, error) {
	mt := struct {
		MediaType string          `json:"mediaType"`
		Manifests json.RawMessage `json:"manifests"`
	}{}
	err := json.Unmarshal(content, &mt)
	if err != nil {
		return "", err
	}

	mediaType := mt.MediaType
	if mediaType == "" {
		if len(mt.Manifests) != 0 {
			mediaType = "application/vnd.oci.image.index.v1+json"
		} else {
			mediaType = "application/vnd.oci.image.manifest.v1+json"
		}
	}
	return mediaType, nil
}

func (c *Cache) WalkTags(ctx context.Context, host, image string, tagCb func(tag string) bool) error {
	manifestLinkPath := registry.ManifestTagListCachePath(host, image)
	err := c.Walk(ctx, manifestLinkPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		if d.Name() != "link" {
			return nil
		}

		tag := path.Base(path.Dir(p))

		if !tagCb(tag) {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) ListTags(ctx context.Context, host, image string) ([]string, error) {
	manifestLinkPath := registry.ManifestTagListCachePath(host, image)
	list, err := c.List(ctx, manifestLinkPath)
	if err != nil {
		return nil, err
	}

	return slices.Map(list, path.Base), nil
}
