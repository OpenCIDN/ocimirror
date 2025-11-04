package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/OpenCIDN/ocimirror/internal/spec"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
	"github.com/OpenCIDN/ocimirror/pkg/transport"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"
	"github.com/wzshiming/httpseek"
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
	StorageURL            string
	Userpass              []string
	Retry                 int
	RetryInterval         time.Duration
	Kubeconfig            string
	Master                string
	InsecureSkipTLSVerify bool
	Timeout               time.Duration
}

func NewCommand() *cobra.Command {
	flags := &flagpole{
		Timeout: 30 * time.Minute,
	}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync OCI images to CIDN",
		Long:  "Proactively create CIDN resources for synchronizing OCI images",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runE(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringSliceVar(&flags.Images, "image", flags.Images, "OCI images to sync (e.g., docker.io/library/nginx:latest)")
	cmd.Flags().StringVar(&flags.StorageURL, "storage-url", flags.StorageURL, "Storage driver url")
	cmd.Flags().StringSliceVarP(&flags.Userpass, "user", "u", flags.Userpass, "host and username and password -u user:pwd@host")
	cmd.Flags().IntVar(&flags.Retry, "retry", flags.Retry, "Retry")
	cmd.Flags().DurationVar(&flags.RetryInterval, "retry-interval", flags.RetryInterval, "Retry interval")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	cmd.Flags().DurationVar(&flags.Timeout, "timeout", flags.Timeout, "Timeout for sync operation")

	cmd.MarkFlagRequired("image")
	cmd.MarkFlagRequired("storage-url")

	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	if len(flags.Images) == 0 {
		return fmt.Errorf("at least one image must be specified")
	}

	if flags.StorageURL == "" {
		return fmt.Errorf("--storage-url is required")
	}

	u, err := url.Parse(flags.StorageURL)
	if err != nil {
		return fmt.Errorf("failed to parse storage URL: %w", err)
	}

	// Setup Kubernetes client
	config, err := clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
	if err != nil {
		return fmt.Errorf("error getting config: %w", err)
	}
	config.TLSClientConfig.Insecure = flags.InsecureSkipTLSVerify

	clientset, err := versioned.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating clientset: %w", err)
	}

	sharedInformerFactory := externalversions.NewSharedInformerFactory(clientset, 0)
	blobInformer := sharedInformerFactory.Task().V1alpha1().Blobs()
	go blobInformer.Informer().RunWithContext(ctx)

	chunkInformer := sharedInformerFactory.Task().V1alpha1().Chunks()
	go chunkInformer.Informer().RunWithContext(ctx)

	cidnClient := &cidn.CIDN{
		Client:        clientset,
		BlobInformer:  blobInformer,
		ChunkInformer: chunkInformer,
		Destination:   u.Scheme,
	}

	// Setup HTTP client
	transportOpts := []transport.Option{
		transport.WithUserAndPass(flags.Userpass),
		transport.WithLogger(logger),
	}

	tp, err := transport.NewTransport(transportOpts...)
	if err != nil {
		return fmt.Errorf("create transport failed: %w", err)
	}

	if flags.RetryInterval > 0 {
		tp = httpseek.NewMustReaderTransport(tp, func(request *http.Request, retry int, err error) error {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if flags.Retry > 0 && retry >= flags.Retry {
				return err
			}
			logger.Warn("Retry", "url", request.URL, "retry", retry, "error", err)
			time.Sleep(flags.RetryInterval)
			return nil
		})
	}

	httpClient := &http.Client{
		Transport: tp,
	}

	// Create a timeout context
	syncCtx := ctx
	if flags.Timeout > 0 {
		var cancel context.CancelFunc
		syncCtx, cancel = context.WithTimeout(ctx, flags.Timeout)
		defer cancel()
	}

	// Sync each image
	for _, imageRef := range flags.Images {
		logger.Info("Syncing image", "image", imageRef)
		if err := syncImage(syncCtx, logger, cidnClient, httpClient, imageRef); err != nil {
			return fmt.Errorf("failed to sync image %s: %w", imageRef, err)
		}
		logger.Info("Successfully synced image", "image", imageRef)
	}

	return nil
}

func syncImage(ctx context.Context, logger *slog.Logger, cidnClient *cidn.CIDN, httpClient *http.Client, imageRef string) error {
	// Parse the image reference
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	host := ref.Context().RegistryStr()
	image := ref.Context().RepositoryStr()
	var tag, digest string

	// Determine if it's a tag or digest reference
	// Check if identifier contains any hash algorithm prefix (sha256:, sha512:, etc.)
	if strings.Contains(ref.Identifier(), ":") && (strings.HasPrefix(ref.Identifier(), "sha") || strings.HasPrefix(ref.Identifier(), "md5")) {
		digest = ref.Identifier()
	} else {
		tag = ref.Identifier()
	}

	logger.Info("Parsed image", "host", host, "image", image, "tag", tag, "digest", digest)

	// If it's a tag, we need to resolve it to a digest first
	var manifestDigest string
	if tag != "" {
		logger.Info("Resolving tag to digest", "tag", tag)
		resp, err := cidnClient.ManifestTag(ctx, host, image, tag)
		if err != nil {
			return fmt.Errorf("failed to resolve manifest tag: %w", err)
		}
		
		// Get the digest from response headers
		if dockerContentDigest, ok := resp.Headers["Docker-Content-Digest"]; ok {
			manifestDigest = dockerContentDigest
		} else {
			return fmt.Errorf("no Docker-Content-Digest header in response")
		}
		logger.Info("Resolved tag to digest", "tag", tag, "digest", manifestDigest)
	} else {
		manifestDigest = digest
	}

	// Sync the manifest
	logger.Info("Syncing manifest", "digest", manifestDigest)
	if err := cidnClient.ManifestDigest(ctx, host, image, manifestDigest); err != nil {
		return fmt.Errorf("failed to sync manifest: %w", err)
	}

	// Fetch the manifest to get the list of blobs
	logger.Info("Fetching manifest content to extract layers")
	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, image, manifestDigest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create manifest request: %w", err)
	}
	req.Header.Set("Accept", registry.OCIAcceptsValue)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch manifest: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read manifest body: %w", err)
	}

	// Determine manifest type by checking mediaType field
	var rawManifest map[string]interface{}
	if err := json.Unmarshal(body, &rawManifest); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}

	mediaType, _ := rawManifest["mediaType"].(string)
	
	// Try to parse based on mediaType
	var layers []string
	
	// Check if it's a manifest list/index
	isManifestList := mediaType == "application/vnd.docker.distribution.manifest.list.v2+json" ||
		mediaType == "application/vnd.oci.image.index.v1+json"
	
	if isManifestList {
		var indexManifest spec.IndexManifestLayers
		if err := json.Unmarshal(body, &indexManifest); err != nil {
			return fmt.Errorf("failed to parse manifest list: %w", err)
		}
		
		if len(indexManifest.Manifests) == 0 {
			return fmt.Errorf("manifest list has no manifests")
		}
		
		logger.Info("Processing manifest list", "count", len(indexManifest.Manifests))
		// For manifest list, sync each manifest
		for _, m := range indexManifest.Manifests {
			logger.Info("Syncing sub-manifest", "digest", m.Digest, "platform", fmt.Sprintf("%s/%s", m.Platform.OS, m.Platform.Architecture))
			if err := cidnClient.ManifestDigest(ctx, host, image, m.Digest); err != nil {
				return fmt.Errorf("failed to sync sub-manifest %s: %w", m.Digest, err)
			}
			
			// Fetch and process the sub-manifest
			subLayers, err := fetchManifestLayers(ctx, httpClient, host, image, m.Digest)
			if err != nil {
				return fmt.Errorf("failed to fetch sub-manifest layers %s: %w", m.Digest, err)
			}
			layers = append(layers, subLayers...)
		}
	} else {
		// Parse as single manifest
		var manifest spec.ManifestLayers
		if err := json.Unmarshal(body, &manifest); err != nil {
			return fmt.Errorf("failed to parse single manifest: %w", err)
		}
		
		logger.Info("Processing single manifest")
		// Add config blob
		if manifest.Config.Digest != "" {
			layers = append(layers, manifest.Config.Digest)
		}
		// Add layer blobs
		for _, layer := range manifest.Layers {
			layers = append(layers, layer.Digest)
		}
	}

	// Sync all blobs
	logger.Info("Syncing blobs", "count", len(layers))
	for i, blobDigest := range layers {
		logger.Info("Syncing blob", "index", i+1, "total", len(layers), "digest", blobDigest)
		if err := cidnClient.Blob(ctx, host, image, blobDigest); err != nil {
			return fmt.Errorf("failed to sync blob %s: %w", blobDigest, err)
		}
	}

	logger.Info("All blobs synced successfully")
	return nil
}

func fetchManifestLayers(ctx context.Context, httpClient *http.Client, host, image, digest string) ([]string, error) {
	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, image, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest request: %w", err)
	}
	req.Header.Set("Accept", registry.OCIAcceptsValue)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch manifest: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest body: %w", err)
	}

	var manifest spec.ManifestLayers
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	var layers []string
	// Add config blob
	if manifest.Config.Digest != "" {
		layers = append(layers, manifest.Config.Digest)
	}
	// Add layer blobs
	for _, layer := range manifest.Layers {
		layers = append(layers, layer.Digest)
	}

	return layers, nil
}
