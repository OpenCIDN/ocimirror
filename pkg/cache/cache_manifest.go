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

	"github.com/OpenCIDN/OpenCIDN/internal/slices"
)

func (c *Cache) RelinkManifest(ctx context.Context, host, image, tag string, blob string) error {
	blob = ensureDigestPrefix(blob)

	_, err := c.StatBlob(ctx, blob)
	if err != nil {
		return err
	}

	if tag != "" {
		manifestLinkPath := manifestTagCachePath(host, image, tag)
		err = c.PutContent(ctx, manifestLinkPath, []byte(blob))
		if err != nil {
			return fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	manifestBlobLinkPath := manifestRevisionsCachePath(host, image, blob)
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
		manifestLinkPath := manifestTagCachePath(host, image, tagOrBlob)
		err := c.PutContent(ctx, manifestLinkPath, []byte(hash))
		if err != nil {
			return 0, "", "", fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
		}
	}

	manifestLinkPath := manifestRevisionsCachePath(host, image, hash)
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
	var manifestLinkPath string
	isHash := strings.HasPrefix(tagOrBlob, "sha256:")
	if isHash {
		manifestLinkPath = manifestRevisionsCachePath(host, image, tagOrBlob[7:])
	} else {
		manifestLinkPath = manifestTagCachePath(host, image, tagOrBlob)
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
	manifestLinkPath := manifestTagCachePath(host, image, tag)

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
		manifestLinkPath = manifestRevisionsCachePath(host, image, tagOrBlob[7:])
	} else {
		manifestLinkPath = manifestTagCachePath(host, image, tagOrBlob)
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
	manifestLinkPath := manifestTagCachePath(host, image, tag)

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

	blob = ensureDigestPrefix(blob)
	if digest == blob {
		return true, nil
	}

	err = c.PutContent(ctx, manifestLinkPath, []byte(blob))
	if err != nil {
		return false, fmt.Errorf("put manifest link path %s error: %w", manifestLinkPath, err)
	}

	manifestBlobLinkPath := manifestRevisionsCachePath(host, image, blob)
	err = c.PutContent(ctx, manifestBlobLinkPath, []byte(blob))
	if err != nil {
		return false, fmt.Errorf("put manifest revisions path %s error: %w", manifestLinkPath, err)
	}
	return true, nil
}

func (c *Cache) WalkTags(ctx context.Context, host, image string, tagCb func(tag string) bool) error {
	manifestLinkPath := manifestTagListCachePath(host, image)
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
	manifestLinkPath := manifestTagListCachePath(host, image)
	list, err := c.List(ctx, manifestLinkPath)
	if err != nil {
		return nil, err
	}

	return slices.Map(list, path.Base), nil
}

func manifestRevisionsCachePath(host, image, blob string) string {
	blob = cleanDigest(blob)
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/revisions/sha256", blob, "link")
}

func manifestTagCachePath(host, image, tag string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags", tag, "current/link")
}

func manifestTagListCachePath(host, image string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags")
}
