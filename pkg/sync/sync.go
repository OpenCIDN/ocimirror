package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/OpenCIDN/ocimirror/internal/format"
	"github.com/OpenCIDN/ocimirror/internal/spec"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
	"golang.org/x/sync/errgroup"
)

const priority = 10

// SyncImage synchronizes a single OCI image using CIDN
func SyncImage(ctx context.Context, g *errgroup.Group, cidnClient *cidn.CIDN, cache *cache.Cache, imageRef string, platformFilters []*spec.Platform) error {
	// Parse the image reference
	host, image, reference, isDigest, err := ParseImageReference(imageRef)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	host, image = utils.CorrectImage(host, image)

	if isDigest {
		slog.Info("Processing", "image", imageRef)

		// Sync the platform-specific manifest
		if err := cidnClient.ManifestDigest(ctx, host, image, reference, reference, priority); err != nil {
			return fmt.Errorf("failed to sync manifest digest: %w", err)
		}

		manifestContent, err := cache.GetBlobContent(ctx, reference)
		if err != nil {
			return fmt.Errorf("failed to get manifest content from cache: %w", err)
		}

		err = syncManifestAndBlobs(ctx, g, cidnClient, cache, manifestContent, host, image, reference, platformFilters)
		if err != nil {
			return fmt.Errorf("failed to sync manifest digest: %w", err)
		}
	} else {
		slog.Info("Processing", "image", imageRef, "platforms", formatPlatforms(platformFilters))

		// Tag reference - resolve to digest first
		tag := reference

		// Step 1: Get the manifest tag to resolve to a digest
		resp, err := cidnClient.ManifestTag(ctx, host, image, tag, priority)
		if err != nil {
			return fmt.Errorf("failed to get manifest tag: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get manifest tag: status code %d", resp.StatusCode)
		}

		// Extract the digest from response headers
		digest := resp.Headers["docker-content-digest"]
		if digest == "" {
			return fmt.Errorf("no digest found in manifest tag response")
		}

		slog.Info("Processing", "digest", digest)
		err = cidnClient.ManifestDigest(ctx, host, image, digest, digest, priority)
		if err != nil {
			return fmt.Errorf("failed to sync manifest digest: %w", err)
		}

		manifestContent, err := cache.GetBlobContent(ctx, digest)
		if err != nil {
			return fmt.Errorf("failed to get manifest content from cache: %w", err)
		}

		err = syncManifestAndBlobs(ctx, g, cidnClient, cache, manifestContent, host, image, digest, platformFilters)
		if err != nil {
			return fmt.Errorf("failed to sync manifest: %w", err)
		}

		err = cache.RelinkManifest(ctx, host, image, tag, digest)
		if err != nil {
			return fmt.Errorf("failed to relink manifest tag to digest in cache: %w", err)
		}
	}

	return nil
}

// syncManifestAndBlobs syncs a manifest and optionally its platform-specific blobs
func syncManifestAndBlobs(ctx context.Context, g *errgroup.Group, cidnClient *cidn.CIDN, cache *cache.Cache, manifestContent []byte, host, image, digest string, platformFilters []*spec.Platform) error {
	// Sync the manifest
	err := cidnClient.ManifestDigest(ctx, host, image, digest, digest, priority)
	if err != nil {
		return err
	}

	// Try to parse as image index/manifest list
	var indexManifest spec.IndexManifestLayers
	err = json.Unmarshal(manifestContent, &indexManifest)
	if err != nil {
		return fmt.Errorf("failed to parse manifest content: %w", err)
	}

	if len(indexManifest.Manifests) == 0 {
		// This is a regular manifest - sync its blobs
		err := syncManifestBlobs(ctx, g, cidnClient, manifestContent, host, image, digest)
		if err != nil {
			return fmt.Errorf("failed to sync blobs for manifest %s: %w", digest, err)
		}
		return nil
	}

	// This is a manifest list - find platform-specific manifests
	matched := false
	for _, m := range indexManifest.Manifests {
		if len(platformFilters) > 0 && !matchesPlatforms(m.Platform, platformFilters) {
			slog.Info("Skip processing", "digest", m.Digest, "plantform", formatPlatform(m.Platform))
			continue
		}

		slog.Info("Processing", "digest", m.Digest, "platform", formatPlatform(m.Platform))
		matched = true
		m := m // capture range variable
		g.Go(func() error {
			// Sync the platform-specific manifest
			if err := cidnClient.ManifestDigest(ctx, host, image, m.Digest, m.Digest, priority); err != nil {
				return fmt.Errorf("failed to sync platform manifest %s for %s/%s: %w", m.Digest, host, image, err)
			}
			// Get the manifest content from cache
			manifestContent, err := cache.GetBlobContent(ctx, m.Digest)
			if err != nil {
				return fmt.Errorf("failed to get platform manifest content %s from cache: %w", m.Digest, err)
			}
			// Get and parse the platform-specific manifest to sync its blobs
			err = syncManifestBlobs(ctx, g, cidnClient, manifestContent, host, image, m.Digest)
			if err != nil {
				return fmt.Errorf("failed to sync blobs for platform manifest %s: %w", m.Digest, err)
			}
			return nil
		})
	}

	if !matched {
		return fmt.Errorf("no manifests matched the specified platforms")
	}

	return nil
}

// formatPlatforms formats a list of platforms as a string
func formatPlatforms(platforms []*spec.Platform) string {
	var formatted []string
	for _, platform := range platforms {
		formatted = append(formatted, formatPlatform(*platform))
	}
	return strings.Join(formatted, ", ")
}

// formatPlatform formats a platform as a string
func formatPlatform(platform spec.Platform) string {
	if platform.Variant != "" {
		return fmt.Sprintf("%s/%s/%s", platform.OS, platform.Architecture, platform.Variant)
	}
	return fmt.Sprintf("%s/%s", platform.OS, platform.Architecture)
}

// syncManifestBlobs parses a manifest and syncs its config and layer blobs
func syncManifestBlobs(ctx context.Context, g *errgroup.Group, cidnClient *cidn.CIDN, manifestContent []byte, host, image, manifestDigest string) error {
	// Parse the manifest to get blob digests
	var manifest spec.ManifestLayers
	err := json.Unmarshal(manifestContent, &manifest)
	if err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Sync config blob
	if manifest.Config.Digest != "" {
		digest := manifest.Config.Digest // capture variable
		slog.Info("Processing", "digest", digest, "manifest", manifestDigest)
		g.Go(func() error {
			err := cidnClient.Blob(ctx, host, image, digest, false, priority)
			if err != nil {
				return fmt.Errorf("failed to sync config blob %s: %w", digest, err)
			}
			return nil
		})
	}

	// Sync layer blobs
	for i, layer := range manifest.Layers {
		index := i
		digest := layer.Digest
		slog.Info("Processing", "digest", digest, "manifest", manifestDigest)
		g.Go(func() error {
			err := cidnClient.Blob(ctx, host, image, digest, false, priority)
			if err != nil {
				return fmt.Errorf("failed to sync layer blob %s (index %d): %w", digest, index, err)
			}
			return nil
		})
	}

	return nil
}

// matchesPlatforms checks if a platform matches any of the filters
func matchesPlatforms(platform spec.Platform, filters []*spec.Platform) bool {
	for _, filter := range filters {
		if matchesPlatform(platform, *filter) {
			return true
		}
	}
	return false
}

// matchesPlatform checks if a platform matches the filter
func matchesPlatform(platform, filter spec.Platform) bool {
	return platform.OS == filter.OS &&
		platform.Architecture == filter.Architecture &&
		(filter.Variant == "" || platform.Variant == filter.Variant)
}

func ParseImageReference(imageRef string) (host, image, reference string, isDigest bool, err error) {
	// Check for digest
	if strings.Contains(imageRef, "@") {
		parts := strings.SplitN(imageRef, "@", 2)
		if len(parts) != 2 {
			return "", "", "", false, fmt.Errorf("invalid digest reference: %s", imageRef)
		}
		reference = parts[1]
		if !strings.HasPrefix(reference, "sha256:") {
			return "", "", "", false, fmt.Errorf("invalid digest format (missing sha256: prefix): %s", reference)
		}

		isDigest = true
		imageRef = parts[0]

	} else {
		lastColon := strings.LastIndex(imageRef, ":")
		if lastColon > 0 {
			reference = imageRef[lastColon+1:]
			imageRef = imageRef[:lastColon]
		} else {
			reference = "latest"
		}
		isDigest = false
	}

	lastSlash := strings.Index(imageRef, "/")

	if lastSlash > 0 && format.IsDomainName(imageRef[:lastSlash]) {
		host = imageRef[:lastSlash]
		image = imageRef[lastSlash+1:]
	} else {
		host = "docker.io"
		image = imageRef
	}

	return host, image, reference, isDigest, nil
}
