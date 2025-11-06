package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/informers/externalversions"
	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/OpenCIDN/ocimirror/internal/spec"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
	"github.com/OpenCIDN/ocimirror/pkg/sync"
	"github.com/spf13/cobra"
	"github.com/wzshiming/sss"
	"golang.org/x/sync/errgroup"
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
	Platforms             []string
	Concurrency           int
	NoWait                bool
}

func NewCommand() *cobra.Command {
	flags := &flagpole{
		Platforms: []string{
			"linux/amd64",
		},
		Concurrency: 5,
	}

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
	cmd.Flags().StringSliceVar(&flags.Platforms, "platform", flags.Platforms, "Platform in the format os/arch[/variant] (e.g., linux/amd64, linux/arm64, linux/arm/v7). Can be specified multiple times.")
	cmd.Flags().IntVar(&flags.Concurrency, "concurrency", flags.Concurrency, "Number of concurrent workers for syncing images")
	cmd.Flags().BoolVar(&flags.NoWait, "no-wait", false, "Exit after creating all blobs, without waiting for them to complete")
	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	if len(flags.Images) == 0 {
		return fmt.Errorf("at least one image reference is required")
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Parse platforms if specified
	var platformFilters []*spec.Platform
	for _, platformStr := range flags.Platforms {
		parts := strings.Split(platformStr, "/")
		if len(parts) < 2 || len(parts) > 3 {
			return fmt.Errorf("invalid platform format: %s (expected os/arch or os/arch/variant)", platformStr)
		}
		platform := &spec.Platform{
			OS:           parts[0],
			Architecture: parts[1],
		}
		if len(parts) == 3 {
			platform.Variant = parts[2]
		}
		platformFilters = append(platformFilters, platform)
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
	go blobInformer.Informer().RunWithContext(ctx)
	chunkInformer := sharedInformerFactory.Task().V1alpha1().Chunks()
	go chunkInformer.Informer().RunWithContext(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(flags.Concurrency)

	// Process each image
	for _, imageRef := range flags.Images {
		// Create CIDN client
		cidnClient := &cidn.CIDN{
			Client:        clientset,
			BlobInformer:  blobInformer,
			ChunkInformer: chunkInformer,
			Destination:   u.Scheme,
		}

		logger.Info("Processing image", "image", imageRef, "platforms", platformFilters)
		if err := sync.SyncImage(ctx, g, cidnClient, sdcache, imageRef, platformFilters, flags.NoWait); err != nil {
			return fmt.Errorf("failed to sync image %s: %w", imageRef, err)
		}
	}

	err = g.Wait()
	if err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	return nil
}
