package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/OpenCIDN/ocimirror/internal/spec"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
	"github.com/spf13/cobra"
	"github.com/wzshiming/sss"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	ctx := signals.SetupSignalContext()
	err := NewCommand().ExecuteContext(ctx)
	if err != nil {
		slog.Error("execute failed", "error", err)
		os.Exit(1)
	}
}

type flagpole struct {
	StorageURL            string
	Kubeconfig            string
	Master                string
	InsecureSkipTLSVerify bool
	Images                []string
	Platform              string
}

func NewCommand() *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Use:   "sync [image...]",
		Short: "Proactively synchronize OCI images using CIDN",
		Long: `Proactively create CIDN resources for synchronizing OCI images.
This command uses CIDN and cache, not the source directly.

Examples:
  sync --storage-url s3://mybucket --kubeconfig ~/.kube/config docker.io/library/nginx:latest
  sync --storage-url s3://mybucket --kubeconfig ~/.kube/config docker.io/library/nginx@sha256:abc123...
  sync --storage-url s3://mybucket --kubeconfig ~/.kube/config ghcr.io/owner/repo:v1.0.0`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.Images = args
			return runE(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringVar(&flags.StorageURL, "storage-url", flags.StorageURL, "Storage driver url (required)")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	cmd.Flags().StringVar(&flags.Platform, "platform", flags.Platform, "Platform in the format os/arch (e.g., linux/amd64, linux/arm64)")

	cmd.MarkFlagRequired("storage-url")

	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	if len(flags.Images) == 0 {
		return fmt.Errorf("at least one image reference is required")
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Parse platform if specified
	var platformFilter *spec.Platform
	if flags.Platform != "" {
		parts := strings.Split(flags.Platform, "/")
		if len(parts) != 2 {
			return fmt.Errorf("invalid platform format: %s (expected os/arch)", flags.Platform)
		}
		platformFilter = &spec.Platform{
			OS:           parts[0],
			Architecture: parts[1],
		}
	}

	// Parse storage URL
	u, err := url.Parse(flags.StorageURL)
	if err != nil {
		return fmt.Errorf("failed to parse storage URL: %w", err)
	}

	// Create storage driver
	sd, err := sss.NewSSS(sss.WithURL(flags.StorageURL))
	if err != nil {
		return fmt.Errorf("create storage driver failed: %w", err)
	}

	// Create cache
	cacheOpts := []cache.Option{
		cache.WithStorageDriver(sd),
	}
	sdcache, err := cache.NewCache(cacheOpts...)
	if err != nil {
		return fmt.Errorf("create cache failed: %w", err)
	}

	// Create Kubernetes client
	config, err := clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
	if err != nil {
		return fmt.Errorf("error getting config: %w", err)
	}
	config.TLSClientConfig.Insecure = flags.InsecureSkipTLSVerify

	clientset, err := versioned.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating clientset: %w", err)
	}

	// Create informers
	sharedInformerFactory := externalversions.NewSharedInformerFactory(clientset, 0)
	blobInformer := sharedInformerFactory.Task().V1alpha1().Blobs()
	chunkInformer := sharedInformerFactory.Task().V1alpha1().Chunks()

	// Start informers
	sharedInformerFactory.Start(ctx.Done())

	// Wait for cache sync
	logger.Info("Waiting for informer caches to sync...")
	synced := sharedInformerFactory.WaitForCacheSync(ctx.Done())
	for informerType, hasSynced := range synced {
		if !hasSynced {
			return fmt.Errorf("failed to sync cache for informer %s", informerType)
		}
	}
	logger.Info("Informer caches synced successfully")

	// Create CIDN client
	cidnClient := &cidn.CIDN{
		Client:        clientset,
		BlobInformer:  blobInformer,
		ChunkInformer: chunkInformer,
		Destination:   u.Scheme,
	}

	// Process each image
	for _, imageRef := range flags.Images {
		logger.Info("Processing image", "image", imageRef)
		if err := syncImage(ctx, cidnClient, sdcache, imageRef, platformFilter, logger); err != nil {
			logger.Error("Failed to sync image", "image", imageRef, "error", err)
			return fmt.Errorf("failed to sync image %s: %w", imageRef, err)
		}
		logger.Info("Successfully synced image", "image", imageRef)
	}

	logger.Info("All images synced successfully")
	return nil
}

func syncImage(ctx context.Context, cidnClient *cidn.CIDN, cache *cache.Cache, imageRef string, platformFilter *spec.Platform, logger *slog.Logger) error {
	// Parse the image reference
	host, image, reference, isDigest, err := parseImageReference(imageRef)
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

		if err := syncManifestAndBlobs(ctx, cidnClient, cache, host, image, reference, platformFilter, logger); err != nil {
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
		if err := syncManifestAndBlobs(ctx, cidnClient, cache, host, image, digest, platformFilter, logger); err != nil {
			return fmt.Errorf("failed to sync manifest: %w", err)
		}

		logger.Info("Manifest synced successfully", "digest", digest)
	}

	return nil
}

// syncManifestAndBlobs syncs a manifest and optionally its platform-specific blobs
func syncManifestAndBlobs(ctx context.Context, cidnClient *cidn.CIDN, cache *cache.Cache, host, image, digest string, platformFilter *spec.Platform, logger *slog.Logger) error {
	// Sync the manifest
	if err := cidnClient.ManifestDigest(ctx, host, image, digest); err != nil {
		return err
	}

	// If platform filter is specified, we need to parse the manifest and sync specific blobs
	if platformFilter != nil {
		// Get the manifest content from cache
		manifestContent, _, _, err := cache.GetManifestContent(ctx, host, image, digest)
		if err != nil {
			logger.Warn("Failed to get manifest content from cache, skipping blob sync", "error", err)
			return nil
		}

		// Try to parse as image index/manifest list
		var indexManifest spec.IndexManifestLayers
		if err := json.Unmarshal(manifestContent, &indexManifest); err == nil && len(indexManifest.Manifests) > 0 {
			// This is a manifest list - find the platform-specific manifest
			logger.Info("Detected manifest list", "manifestCount", len(indexManifest.Manifests))

			for _, m := range indexManifest.Manifests {
				if matchesPlatform(m.Platform, *platformFilter) {
					logger.Info("Found matching platform manifest", "digest", m.Digest, "platform", fmt.Sprintf("%s/%s", m.Platform.OS, m.Platform.Architecture))

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
		} else {
			// This is a regular manifest - sync its blobs
			if err := syncManifestBlobs(ctx, cidnClient, cache, host, image, digest, logger); err != nil {
				logger.Warn("Failed to sync manifest blobs", "error", err)
			}
		}
	}

	return nil
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
	return platform.OS == filter.OS && platform.Architecture == filter.Architecture
}

// parseImageReference parses an image reference in the format:
// - host/image:tag
// - host/image@digest
// - image:tag (defaults to docker.io)
func parseImageReference(imageRef string) (host, image, reference string, isDigest bool, err error) {
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
