package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/spec"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
)

// SyncImage synchronizes a single OCI image using CIDN
func SyncImage(ctx context.Context, cidnClient *cidn.CIDN, cache *cache.Cache, imageRef string, platformFilters []*spec.Platform, logger *slog.Logger) error {
	// Parse the image reference
	host, image, reference, isDigest, err := ParseImageReference(imageRef)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	// Apply host corrections
	host, image = utils.CorrectImage(host, image)

	logger.Info("Parsed image reference", "host", host, "image", image, "reference", reference, "isDigest", isDigest)

	// Determine if this is a tag or digest reference
	if isDigest {
		// Direct digest reference - sync the manifest
		logger.Info("Syncing manifest by digest", "host", host, "image", image, "digest", reference)

		if err := syncManifestAndBlobs(ctx, cidnClient, cache, host, image, reference, platformFilters, logger); err != nil {
			return fmt.Errorf("failed to sync manifest digest: %w", err)
		}

		logger.Info("Manifest synced successfully", "digest", reference)
	} else {
		// Tag reference - resolve to digest first
		tag := reference
		logger.Info("Syncing manifest by tag", "host", host, "image", image, "tag", tag)

		// Step 1: Get the manifest tag to resolve to a digest
		resp, err := cidnClient.ManifestTag(ctx, host, image, tag)
		if err != nil {
			return fmt.Errorf("failed to get manifest tag: %w", err)
		}

		// Extract the digest from response headers
		digest := resp.Headers["docker-content-digest"]
		if digest == "" {
			digest = resp.Headers["Docker-Content-Digest"]
		}
		if digest == "" {
			return fmt.Errorf("no digest found in manifest tag response")
		}

		logger.Info("Resolved tag to digest", "tag", tag, "digest", digest)

		// Step 2: Sync the manifest by digest
		if err := syncManifestAndBlobs(ctx, cidnClient, cache, host, image, digest, platformFilters, logger); err != nil {
			return fmt.Errorf("failed to sync manifest: %w", err)
		}

		// Step 3: Rebuild the tag->digest link in cache
		if err := cache.RelinkManifest(ctx, host, image, tag, digest); err != nil {
			logger.Warn("Failed to relink manifest", "tag", tag, "digest", digest, "error", err)
		} else {
			logger.Info("Relinked manifest", "tag", tag, "digest", digest)
		}

		logger.Info("Manifest synced successfully", "digest", digest)
	}

	return nil
}

// syncManifestAndBlobs syncs a manifest and optionally its platform-specific blobs
func syncManifestAndBlobs(ctx context.Context, cidnClient *cidn.CIDN, cache *cache.Cache, host, image, digest string, platformFilters []*spec.Platform, logger *slog.Logger) error {
	// Sync the manifest
	if err := cidnClient.ManifestDigest(ctx, host, image, digest); err != nil {
		return err
	}

	// If platform filters are specified, we need to parse the manifest and sync specific blobs
	if len(platformFilters) > 0 {
		// Get the manifest content from cache
		manifestContent, _, _, err := cache.GetManifestContent(ctx, host, image, digest)
		if err != nil {
			logger.Warn("Failed to get manifest content from cache, skipping blob sync", "error", err)
			return nil
		}

		// Try to parse as image index/manifest list
		var indexManifest spec.IndexManifestLayers
		if err := json.Unmarshal(manifestContent, &indexManifest); err == nil && len(indexManifest.Manifests) > 0 {
			// This is a manifest list - find platform-specific manifests
			logger.Info("Detected manifest list", "manifestCount", len(indexManifest.Manifests))

			for _, m := range indexManifest.Manifests {
				for _, filter := range platformFilters {
					if matchesPlatform(m.Platform, *filter) {
						logger.Info("Found matching platform manifest", "digest", m.Digest, "platform", fmt.Sprintf("%s/%s%s", m.Platform.OS, m.Platform.Architecture, formatVariant(m.Platform.Variant)))

						// Sync the platform-specific manifest
						if err := cidnClient.ManifestDigest(ctx, host, image, m.Digest); err != nil {
							return fmt.Errorf("failed to sync platform manifest: %w", err)
						}

						// Get and parse the platform-specific manifest to sync its blobs
						if err := syncManifestBlobs(ctx, cidnClient, cache, host, image, m.Digest, logger); err != nil {
							logger.Warn("Failed to sync blobs for platform manifest", "error", err)
						}

						break
					}
				}
			}
		} else {
			// This is a regular manifest - sync its blobs
			if err := syncManifestBlobs(ctx, cidnClient, cache, host, image, digest, logger); err != nil {
				logger.Warn("Failed to sync manifest blobs", "error", err)
			}
		}
	}

	return nil
}

// formatVariant returns a formatted variant string for logging
func formatVariant(variant string) string {
	if variant == "" {
		return ""
	}
	return "/" + variant
}

// syncManifestBlobs parses a manifest and syncs its config and layer blobs
func syncManifestBlobs(ctx context.Context, cidnClient *cidn.CIDN, cache *cache.Cache, host, image, digest string, logger *slog.Logger) error {
	// Get the manifest content from cache
	manifestContent, _, _, err := cache.GetManifestContent(ctx, host, image, digest)
	if err != nil {
		return fmt.Errorf("failed to get manifest content: %w", err)
	}

	// Parse the manifest to get blob digests
	var manifest spec.ManifestLayers
	if err := json.Unmarshal(manifestContent, &manifest); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Sync config blob
	if manifest.Config.Digest != "" {
		logger.Info("Syncing config blob", "digest", manifest.Config.Digest)
		if err := cidnClient.Blob(ctx, host, image, manifest.Config.Digest); err != nil {
			logger.Warn("Failed to sync config blob", "digest", manifest.Config.Digest, "error", err)
		}
	}

	// Sync layer blobs
	for i, layer := range manifest.Layers {
		logger.Info("Syncing layer blob", "index", i, "digest", layer.Digest)
		if err := cidnClient.Blob(ctx, host, image, layer.Digest); err != nil {
			logger.Warn("Failed to sync layer blob", "digest", layer.Digest, "error", err)
		}
	}

	return nil
}

// matchesPlatform checks if a platform matches the filter
func matchesPlatform(platform, filter spec.Platform) bool {
	if platform.OS != filter.OS || platform.Architecture != filter.Architecture {
		return false
	}
	// If filter specifies a variant, it must match
	if filter.Variant != "" && platform.Variant != filter.Variant {
		return false
	}
	return true
}

// ParseImageReference parses an image reference in the format:
// - host/image:tag
// - host/image@digest
// - image:tag (defaults to docker.io)
func ParseImageReference(imageRef string) (host, image, reference string, isDigest bool, err error) {
	// Check for digest
	if strings.Contains(imageRef, "@") {
		parts := strings.SplitN(imageRef, "@", 2)
		if len(parts) != 2 {
			return "", "", "", false, fmt.Errorf("invalid digest reference: %s", imageRef)
		}
		reference = parts[1]
		isDigest = true
		imageRef = parts[0]
	} else {
		// Check for tag - need to be careful about hostname with port like localhost:5000
		// Find the last colon
		lastColon := strings.LastIndex(imageRef, ":")
		lastSlash := strings.LastIndex(imageRef, "/")

		// If there's a colon and it's after the last slash (or no slash), it's likely a tag
		if lastColon > 0 && (lastSlash < 0 || lastColon > lastSlash) {
			reference = imageRef[lastColon+1:]
			imageRef = imageRef[:lastColon]
		} else {
			reference = "latest"
		}
		isDigest = false
	}

	// Parse host and image
	parts := strings.SplitN(imageRef, "/", 2)
	if len(parts) == 1 {
		// No host specified, default to docker.io
		host = "docker.io"
		image = parts[0]
	} else if !strings.Contains(parts[0], ".") && !strings.Contains(parts[0], ":") && parts[0] != "localhost" {
		// First part is not a domain, it's part of the image name
		host = "docker.io"
		image = imageRef
	} else {
		host = parts[0]
		image = parts[1]
	}

	// Ensure digest has sha256: prefix if it's a digest
	if isDigest && !strings.HasPrefix(reference, "sha256:") {
		reference = registry.EnsureDigestPrefix(reference)
	}

	return host, image, reference, isDigest, nil
}
