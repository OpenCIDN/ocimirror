package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/ocimirror/internal/signals"
	"github.com/OpenCIDN/ocimirror/pkg/sync"
	"github.com/OpenCIDN/ocimirror/pkg/transport"
	"github.com/spf13/cobra"
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

	_, err = transport.NewTransport(transportOpts...)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}

	// Process each image
	for _, imageRef := range flags.Images {
		logger.Info("Processing image", "image", imageRef)
		if err := sync.SyncImage(ctx, clientset, imageRef, destination, logger); err != nil {
			logger.Error("Failed to sync image", "image", imageRef, "error", err)
			return fmt.Errorf("failed to sync image %s: %w", imageRef, err)
		}
		logger.Info("Successfully synced image", "image", imageRef)
	}

	return nil
}
