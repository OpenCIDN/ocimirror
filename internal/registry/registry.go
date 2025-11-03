package registry

import (
	"path"
	"strings"
)

func CleanDigest(blob string) string {
	return strings.TrimPrefix(blob, "sha256:")
}

func EnsureDigestPrefix(blob string) string {
	if !strings.HasPrefix(blob, "sha256:") {
		return "sha256:" + blob
	}
	return blob
}

func BlobCachePath(blob string) string {
	blob = CleanDigest(blob)
	return path.Join("/docker/registry/v2/blobs/sha256", blob[:2], blob, "data")
}

func ManifestRevisionsCachePath(host, image, blob string) string {
	blob = CleanDigest(blob)
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/revisions/sha256", blob, "link")
}

func ManifestTagCachePath(host, image, tag string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags", tag, "current/link")
}

func ManifestTagListCachePath(host, image string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags")
}
