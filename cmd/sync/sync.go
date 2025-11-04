package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/OpenCIDN/ocimirror/pkg/transport"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	Images                []string
	Platforms             []string
	StorageURL            string
	Destination           string
	Kubeconfig            string
	Master                string
	InsecureSkipTLSVerify bool
	Userpass              []string
	Retry                 int
	RetryInterval         time.Duration
}

func NewCommand() *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Proactively synchronize OCI images to CIDN resources",
		Long: `Create CIDN Blob and Chunk resources for specified OCI images.

This command proactively creates CIDN resources for synchronizing OCI images
before they are accessed, allowing for efficient pre-caching and distribution.

Examples:
  sync --images docker.io/library/busybox:latest
  sync --images docker.io/library/nginx:latest,ghcr.io/user/app:v1.0
  sync --images docker.io/library/alpine:3.18 --storage-url s3://bucket/path
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runE(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringSliceVar(&flags.Images, "images", flags.Images, "OCI images to synchronize (format: registry/repository:tag or registry/repository@digest)")
	cmd.Flags().StringSliceVar(&flags.Platforms, "platforms", flags.Platforms, "Platforms to sync (format: os/arch or os/arch/variant, e.g., linux/amd64,linux/arm64)")
	cmd.Flags().StringVar(&flags.StorageURL, "storage-url", flags.StorageURL, "Storage driver URL for CIDN destination")
	cmd.Flags().StringVar(&flags.Destination, "destination", flags.Destination, "CIDN destination name (defaults to storage URL scheme)")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity")
	cmd.Flags().StringSliceVarP(&flags.Userpass, "user", "u", flags.Userpass, "Registry credentials in format user:pwd@host")
	cmd.Flags().IntVar(&flags.Retry, "retry", flags.Retry, "Number of retries for failed requests")
	cmd.Flags().DurationVar(&flags.RetryInterval, "retry-interval", flags.RetryInterval, "Interval between retries")

	cmd.MarkFlagRequired("images")
	cmd.MarkFlagRequired("storage-url")

	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Parse storage URL to determine destination
	storageURL, err := url.Parse(flags.StorageURL)
	if err != nil {
		return fmt.Errorf("failed to parse storage URL: %w", err)
	}

	destination := flags.Destination
	if destination == "" {
		destination = storageURL.Scheme
	}

	// Create Kubernetes client
	config, err := clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
	if err != nil {
		return fmt.Errorf("error building kubeconfig: %w", err)
	}
	config.TLSClientConfig.Insecure = flags.InsecureSkipTLSVerify

	clientset, err := versioned.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating clientset: %w", err)
	}

	// Create HTTP transport
	transportOpts := []transport.Option{
		transport.WithUserAndPass(flags.Userpass),
		transport.WithLogger(logger),
	}

	tp, err := transport.NewTransport(transportOpts...)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}

	httpClient := &http.Client{
		Transport: tp,
	}

	// Process each image
	for _, imageRef := range flags.Images {
		logger.Info("Processing image", "image", imageRef)
		if err := syncImage(ctx, clientset, httpClient, imageRef, destination, flags.Platforms, logger); err != nil {
			logger.Error("Failed to sync image", "image", imageRef, "error", err)
			return fmt.Errorf("failed to sync image %s: %w", imageRef, err)
		}
		logger.Info("Successfully synced image", "image", imageRef)
	}

	return nil
}

func syncImage(ctx context.Context, clientset versioned.Interface, httpClient *http.Client, imageRef, destination string, platforms []string, logger *slog.Logger) error {
	// Parse image reference
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	// Get image descriptor
	desc, err := remote.Get(ref, remote.WithContext(ctx), remote.WithTransport(httpClient.Transport))
	if err != nil {
		return fmt.Errorf("failed to get image descriptor: %w", err)
	}

	host := ref.Context().RegistryStr()
	repository := ref.Context().RepositoryStr()
	manifestDigest := desc.Digest.String()
	
	// Check if this is a tag or digest reference
	_, isTag := ref.(name.Tag)

	logger.Info("Image details", "host", host, "repository", repository, "digest", manifestDigest, "isTag", isTag)

	// For tag-based references, create Chunk; for digest-based, create Blob
	if isTag {
		// Use Chunk for tag-based manifest
		if err := createManifestChunk(ctx, clientset, host, repository, ref.Identifier(), logger); err != nil {
			return fmt.Errorf("failed to create manifest chunk: %w", err)
		}
	} else {
		// Use Blob for digest-based manifest
		if err := createManifestBlob(ctx, clientset, host, repository, manifestDigest, destination, logger); err != nil {
			return fmt.Errorf("failed to create manifest blob: %w", err)
		}
	}

	// Parse manifest to get layer blobs
	manifest, err := desc.Image()
	if err != nil {
		// It might be an index
		index, indexErr := desc.ImageIndex()
		if indexErr != nil {
			return fmt.Errorf("failed to parse as image or index: image error: %w, index error: %w", err, indexErr)
		}
		return syncImageIndex(ctx, clientset, httpClient, index, host, repository, destination, platforms, logger)
	}

	return syncImageManifest(ctx, clientset, manifest, host, repository, destination, logger)
}

func syncImageIndex(ctx context.Context, clientset versioned.Interface, httpClient *http.Client, index containerregistry.ImageIndex, host, repository, destination string, platforms []string, logger *slog.Logger) error {
	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("failed to get index manifest: %w", err)
	}

	// Build platform filter map if specified
	platformFilter := make(map[string]bool)
	if len(platforms) > 0 {
		for _, p := range platforms {
			platformFilter[p] = true
		}
	}

	// Process each manifest in the index
	for _, desc := range indexManifest.Manifests {
		// Check platform filter
		if len(platformFilter) > 0 && desc.Platform != nil {
			platformStr := fmt.Sprintf("%s/%s", desc.Platform.OS, desc.Platform.Architecture)
			if desc.Platform.Variant != "" {
				platformStr = fmt.Sprintf("%s/%s", platformStr, desc.Platform.Variant)
			}
			if !platformFilter[platformStr] {
				logger.Info("Skipping platform", "digest", desc.Digest.String(), "platform", platformStr)
				continue
			}
		}

		logger.Info("Processing manifest from index", "digest", desc.Digest.String(), "platform", desc.Platform)

		// Create blob for this manifest (always use Blob for digest-based manifests in index)
		manifestDigest := desc.Digest.String()
		if err := createManifestBlob(ctx, clientset, host, repository, manifestDigest, destination, logger); err != nil {
			logger.Warn("Failed to create manifest blob from index", "digest", manifestDigest, "error", err)
			continue
		}

		// Get the image for this manifest
		img, err := index.Image(desc.Digest)
		if err != nil {
			logger.Warn("Failed to get image from index", "digest", desc.Digest.String(), "error", err)
			continue
		}

		if err := syncImageManifest(ctx, clientset, img, host, repository, destination, logger); err != nil {
			logger.Warn("Failed to sync image manifest from index", "digest", desc.Digest.String(), "error", err)
		}
	}

	return nil
}

func syncImageManifest(ctx context.Context, clientset versioned.Interface, img containerregistry.Image, host, repository, destination string, logger *slog.Logger) error {
	// Get layers
	layers, err := img.Layers()
	if err != nil {
		return fmt.Errorf("failed to get layers: %w", err)
	}

	// Create Blob for each layer
	for _, layer := range layers {
		digest, err := layer.Digest()
		if err != nil {
			logger.Warn("Failed to get layer digest", "error", err)
			continue
		}

		size, err := layer.Size()
		if err != nil {
			logger.Warn("Failed to get layer size", "digest", digest.String(), "error", err)
			continue
		}

		logger.Info("Processing layer", "digest", digest.String(), "size", size)

		if err := createLayerBlob(ctx, clientset, host, repository, digest.String(), destination, logger); err != nil {
			logger.Warn("Failed to create layer blob", "digest", digest.String(), "error", err)
			continue
		}
	}

	// Get config blob
	configName, err := img.ConfigName()
	if err != nil {
		return fmt.Errorf("failed to get config digest: %w", err)
	}

	logger.Info("Processing config", "digest", configName.String())
	if err := createLayerBlob(ctx, clientset, host, repository, configName.String(), destination, logger); err != nil {
		logger.Warn("Failed to create config blob", "digest", configName.String(), "error", err)
	}

	return nil
}

// getBearerName constructs the bearer name from host and repository
func getBearerName(host, repository string) string {
	return fmt.Sprintf("%s:%s", host, strings.ReplaceAll(repository, "/", ":"))
}

// createManifestChunk creates a CIDN Chunk resource for a tag-based manifest
func createManifestChunk(ctx context.Context, clientset versioned.Interface, host, repository, tag string, logger *slog.Logger) error {
	url := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repository, tag)
	chunkName := fmt.Sprintf("manifest:%s:%s:%s", host, strings.ReplaceAll(repository, "/", ":"), tag)
	
	chunks := clientset.TaskV1alpha1().Chunks()

	// Check if chunk already exists
	_, err := chunks.Get(ctx, chunkName, metav1.GetOptions{})
	if err == nil {
		logger.Info("Manifest chunk already exists", "name", chunkName)
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("error checking chunk: %w", err)
	}

	// Create the chunk
	_, err = chunks.Create(ctx, &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunkName,
		},
		Spec: v1alpha1.ChunkSpec{
			MaximumRetry: 3,
			Source: v1alpha1.ChunkHTTP{
				Request: v1alpha1.ChunkHTTPRequest{
					Method: http.MethodGet,
					URL:    url,
					Headers: map[string]string{
						"Accept": "application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json",
					},
				},
				Response: v1alpha1.ChunkHTTPResponse{
					StatusCode: http.StatusOK,
				},
			},
			BearerName: getBearerName(host, repository),
		},
	}, metav1.CreateOptions{})

	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create manifest chunk: %w", err)
	}

	logger.Info("Created manifest chunk", "name", chunkName, "tag", tag)
	return nil
}

// blobParams holds common parameters for blob creation
type blobParams struct {
	sourceURL          string
	displayName        string
	maximumRunning     int64
	chunksNumber       int64
	minimumChunkSize   int64
	blobType           string // "manifest" or "layer"
}

// createBlob is a helper function that creates a CIDN Blob resource
func createBlob(ctx context.Context, clientset versioned.Interface, host, repository, digest, destination string, params blobParams, logger *slog.Logger) error {
	cachePath := registry.BlobCachePath(digest)
	blobName := digest
	displayName := params.displayName

	blobs := clientset.TaskV1alpha1().Blobs()

	// Check if blob already exists
	_, err := blobs.Get(ctx, blobName, metav1.GetOptions{})
	if err == nil {
		logger.Info(fmt.Sprintf("%s blob already exists", params.blobType), "name", blobName)
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("error checking blob: %w", err)
	}

	// Build blob spec
	spec := v1alpha1.BlobSpec{
		MaximumRunning: params.maximumRunning,
		Source: []v1alpha1.BlobSource{
			{
				URL:        params.sourceURL,
				BearerName: getBearerName(host, repository),
			},
		},
		Destination: []v1alpha1.BlobDestination{
			{
				Name:         destination,
				Path:         cachePath,
				SkipIfExists: true,
			},
		},
		ContentSha256: registry.CleanDigest(digest),
	}

	if params.chunksNumber > 0 {
		spec.ChunksNumber = params.chunksNumber
	}
	if params.minimumChunkSize > 0 {
		spec.MinimumChunkSize = params.minimumChunkSize
	}

	// Create the blob
	_, err = blobs.Create(ctx, &v1alpha1.Blob{
		ObjectMeta: metav1.ObjectMeta{
			Name: blobName,
			Annotations: map[string]string{
				v1alpha1.BlobDisplayNameAnnotation: displayName,
			},
		},
		Spec: spec,
	}, metav1.CreateOptions{})

	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %s blob: %w", params.blobType, err)
	}

	logger.Info(fmt.Sprintf("Created %s blob", params.blobType), "name", blobName, "display", displayName)
	return nil
}

func createManifestBlob(ctx context.Context, clientset versioned.Interface, host, repository, digest, destination string, logger *slog.Logger) error {
	return createBlob(ctx, clientset, host, repository, digest, destination, blobParams{
		sourceURL:      fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repository, digest),
		displayName:    fmt.Sprintf("%s/%s@%s", host, repository, digest),
		maximumRunning: 1,
		chunksNumber:   1,
		blobType:       "manifest",
	}, logger)
}

func createLayerBlob(ctx context.Context, clientset versioned.Interface, host, repository, digest, destination string, logger *slog.Logger) error {
	return createBlob(ctx, clientset, host, repository, digest, destination, blobParams{
		sourceURL:        fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, repository, digest),
		displayName:      fmt.Sprintf("%s/%s@%s", host, repository, digest),
		maximumRunning:   3,
		minimumChunkSize: 128 * 1024 * 1024,
		blobType:         "layer",
	}, logger)
}
