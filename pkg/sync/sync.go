package sync

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImageReference represents a parsed OCI image reference
type ImageReference struct {
	Host       string
	Repository string
	Reference  string // tag or digest
	IsDigest   bool
}

// ParseImageReference parses an image reference like docker.io/library/nginx:latest or docker.io/library/nginx@sha256:...
func ParseImageReference(ref string) (*ImageReference, error) {
	// Handle references like nginx:latest (without registry)
	if !strings.Contains(ref, "/") {
		ref = "docker.io/library/" + ref
	} else if strings.Count(ref, "/") == 1 && !strings.Contains(strings.Split(ref, "/")[0], ".") {
		// Handle references like library/nginx:latest
		ref = "docker.io/" + ref
	}

	var host, repository, reference string
	var isDigest bool

	// Check if it's a digest reference
	if strings.Contains(ref, "@") {
		parts := strings.SplitN(ref, "@", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid digest reference: %s", ref)
		}
		reference = parts[1]
		isDigest = true
		ref = parts[0]
	} else if strings.Contains(ref, ":") {
		// Check for tag (but not part of the host)
		lastColon := strings.LastIndex(ref, ":")
		beforeColon := ref[:lastColon]
		if strings.Contains(beforeColon, "/") {
			// This is a tag, not a port
			reference = ref[lastColon+1:]
			ref = beforeColon
		}
	}

	// Default tag if none specified
	if reference == "" {
		reference = "latest"
	}

	// Split host and repository
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 && strings.Contains(parts[0], ".") {
		host = parts[0]
		repository = parts[1]
	} else {
		return nil, fmt.Errorf("invalid image reference: %s", ref)
	}

	return &ImageReference{
		Host:       host,
		Repository: repository,
		Reference:  reference,
		IsDigest:   isDigest,
	}, nil
}

// SyncImage synchronizes an OCI image by creating appropriate CIDN resources
func SyncImage(ctx context.Context, clientset versioned.Interface, imageRef, destination string, logger *slog.Logger) error {
	// Parse image reference
	ref, err := ParseImageReference(imageRef)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	logger.Info("Image details", "host", ref.Host, "repository", ref.Repository, "reference", ref.Reference, "isDigest", ref.IsDigest)

	// For tag-based references, create Chunk to resolve tag to digest
	if !ref.IsDigest {
		// Create Chunk for tag-based manifest (to resolve tag to digest)
		if err := CreateManifestChunk(ctx, clientset, ref.Host, ref.Repository, ref.Reference, logger); err != nil {
			return fmt.Errorf("failed to create manifest chunk: %w", err)
		}
		logger.Info("Created resources for tag-based image - CIDN will resolve and sync automatically")
	} else {
		// For digest-based references, directly create Blob
		if err := CreateManifestBlob(ctx, clientset, ref.Host, ref.Repository, ref.Reference, destination, logger); err != nil {
			return fmt.Errorf("failed to create manifest blob: %w", err)
		}
		logger.Info("Created manifest blob for digest-based image - CIDN will sync layers automatically")
	}

	return nil
}

// getBearerName constructs the bearer name from host and repository
func getBearerName(host, repository string) string {
	return fmt.Sprintf("%s:%s", host, strings.ReplaceAll(repository, "/", ":"))
}

// CreateManifestChunk creates a CIDN Chunk resource for a tag-based manifest
// This is used to resolve the tag to a digest via a HEAD request
func CreateManifestChunk(ctx context.Context, clientset versioned.Interface, host, repository, tag string, logger *slog.Logger) error {
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

	// Create the chunk with HEAD method to resolve tag to digest
	_, err = chunks.Create(ctx, &v1alpha1.Chunk{
		ObjectMeta: metav1.ObjectMeta{
			Name: chunkName,
		},
		Spec: v1alpha1.ChunkSpec{
			MaximumRetry: 3,
			Source: v1alpha1.ChunkHTTP{
				Request: v1alpha1.ChunkHTTPRequest{
					Method: http.MethodHead,
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
	sourceURL        string
	displayName      string
	maximumRunning   int64
	chunksNumber     int64
	minimumChunkSize int64
	blobType         string // "manifest" or "layer"
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

// CreateManifestBlob creates a CIDN Blob resource for a manifest
func CreateManifestBlob(ctx context.Context, clientset versioned.Interface, host, repository, digest, destination string, logger *slog.Logger) error {
	return createBlob(ctx, clientset, host, repository, digest, destination, blobParams{
		sourceURL:      fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repository, digest),
		displayName:    fmt.Sprintf("%s/%s@%s", host, repository, digest),
		maximumRunning: 1,
		chunksNumber:   1,
		blobType:       "manifest",
	}, logger)
}

// CreateLayerBlob creates a CIDN Blob resource for a layer
func CreateLayerBlob(ctx context.Context, clientset versioned.Interface, host, repository, digest, destination string, logger *slog.Logger) error {
	return createBlob(ctx, clientset, host, repository, digest, destination, blobParams{
		sourceURL:        fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, repository, digest),
		displayName:      fmt.Sprintf("%s/%s@%s", host, repository, digest),
		maximumRunning:   3,
		minimumChunkSize: 128 * 1024 * 1024,
		blobType:         "layer",
	}, logger)
}
