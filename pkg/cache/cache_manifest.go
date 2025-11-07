package cache

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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

	_, err := c.StatBlob(ctx, blob)
	if err != nil {
		return err
	}

	if tag != "" {
		manifestLinkPath := registry.ManifestTagCachePath(host, image, tag)
		err = c.PutContent(ctx, manifestLinkPath, []byte(blob))
		if err != nil {
			return fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	manifestBlobLinkPath := registry.ManifestRevisionsCachePath(host, image, blob)
	err = c.PutContent(ctx, manifestBlobLinkPath, []byte(blob))
	if err != nil {
		return fmt.Errorf("put manifest revisions path %s error: %w", manifestBlobLinkPath, err)
	}

	return nil
}

func (c *Cache) PutManifestContent(ctx context.Context, host, image, tagOrBlob string, content []byte) (int64, string, string, error) {
	mediaType, err := getMediaType(content)
	if err != nil {
		return 0, "", "", fmt.Errorf("invalid content: %w: %s", err, string(content))
	}

	h := sha256.New()
	h.Write(content)
	hash := "sha256:" + hex.EncodeToString(h.Sum(nil)[:])

	isHash := strings.HasPrefix(tagOrBlob, "sha256:")
	if isHash {
		if tagOrBlob != hash {
			return 0, "", "", fmt.Errorf("expected hash %s is not same to %s", tagOrBlob, hash)
		}
	} else {
		manifestLinkPath := registry.ManifestTagCachePath(host, image, tagOrBlob)
		err := c.PutContent(ctx, manifestLinkPath, []byte(hash))
		if err != nil {
			return 0, "", "", fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	manifestLinkPath := registry.ManifestRevisionsCachePath(host, image, hash)
	err = c.PutContent(ctx, manifestLinkPath, []byte(hash))
	if err != nil {
		return 0, "", "", fmt.Errorf("put manifest revisions path %s error: %w", manifestLinkPath, err)
	}

	n, err := c.PutBlobContent(ctx, hash, content)
	if err != nil {
		return 0, "", "", fmt.Errorf("put manifest blob path %s error: %w", hash, err)
	}
	return n, hash, mediaType, nil
}

func (c *Cache) GetManifestContent(ctx context.Context, host, image, tagOrBlob string) ([]byte, string, string, error) {
	// Create a unique cache key for this manifest using base64 encoding to handle special characters
	cacheKey := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s\x00%s\x00%s", host, image, tagOrBlob)))
	
	// Check in-memory cache first
	if c.manifestMemCache != nil {
		if cached, ok := c.manifestMemCache.get(cacheKey); ok {
			// Parse the cached data: first line is digest, second line is mediaType, rest is content
			lines := strings.SplitN(string(cached), "\n", 3)
			if len(lines) == 3 {
				return []byte(lines[2]), lines[0], lines[1], nil
			}
		}
	}

	var manifestLinkPath string
	isHash := strings.HasPrefix(tagOrBlob, "sha256:")
	if isHash {
		manifestLinkPath = registry.ManifestRevisionsCachePath(host, image, tagOrBlob[7:])
	} else {
		manifestLinkPath = registry.ManifestTagCachePath(host, image, tagOrBlob)
	}

	digestContent, err := c.GetContent(ctx, manifestLinkPath)
	if err != nil {
		return nil, "", "", fmt.Errorf("get manifest link path %s error: %w", manifestLinkPath, err)
	}
	digest := string(digestContent)
	content, err := c.GetBlobContent(ctx, digest)
	if err != nil {
		return nil, "", "", err
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
		return nil, "", "", fmt.Errorf("invalid content: %w: %s", err, string(content))
	}

	// Store in memory cache: digest + newline + mediaType + newline + content
	if c.manifestMemCache != nil {
		cached := fmt.Sprintf("%s\n%s\n%s", digest, mediaType, string(content))
		c.manifestMemCache.set(cacheKey, []byte(cached), c.memoryCacheTTL)
	}

	return content, digest, mediaType, nil
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

func (c *Cache) DigestManifest(ctx context.Context, host, image, tag string) (string, error) {
	manifestLinkPath := registry.ManifestTagCachePath(host, image, tag)

	digestContent, err := c.GetContent(ctx, manifestLinkPath)
	if err != nil {
		return "", fmt.Errorf("get manifest path %s error: %w", manifestLinkPath, err)
	}
	return string(digestContent), nil
}

func (c *Cache) StatManifest(ctx context.Context, host, image, tagOrBlob string) (bool, error) {
	var manifestLinkPath string
	isHash := strings.HasPrefix(tagOrBlob, "sha256:")
	if isHash {
		manifestLinkPath = registry.ManifestRevisionsCachePath(host, image, tagOrBlob[7:])
	} else {
		manifestLinkPath = registry.ManifestTagCachePath(host, image, tagOrBlob)
	}

	digestContent, err := c.GetContent(ctx, manifestLinkPath)
	if err != nil {
		return false, fmt.Errorf("stat manifest link path %s error: %w", manifestLinkPath, err)
	}
	digest := string(digestContent)
	stat, err := c.StatBlob(ctx, digest)
	if err != nil {
		return false, err
	}

	return stat.Size() != 0, nil
}

func (c *Cache) StatOrRelinkManifest(ctx context.Context, host, image, tag string, blob string) (bool, error) {
	manifestLinkPath := registry.ManifestTagCachePath(host, image, tag)

	digestContent, err := c.GetContent(ctx, manifestLinkPath)
	if err != nil {
		return false, fmt.Errorf("stat or relink manifest link path %s error: %w", manifestLinkPath, err)
	}
	digest := string(digestContent)
	stat, err := c.StatBlob(ctx, digest)
	if err != nil {
		return false, err
	}

	if stat.Size() == 0 {
		return false, nil
	}

	blob = registry.EnsureDigestPrefix(blob)
	if digest == blob {
		return true, nil
	}

	err = c.PutContent(ctx, manifestLinkPath, []byte(blob))
	if err != nil {
		return false, fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
	}

	manifestBlobLinkPath := registry.ManifestRevisionsCachePath(host, image, blob)
	err = c.PutContent(ctx, manifestBlobLinkPath, []byte(blob))
	if err != nil {
		return false, fmt.Errorf("put manifest revisions path %s error: %w", manifestLinkPath, err)
	}
	return true, nil
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
