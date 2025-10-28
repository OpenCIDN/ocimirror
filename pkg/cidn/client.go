package cidn

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// Client wraps CIDN Kubernetes client for blob synchronization
type Client struct {
	client           versioned.Interface
	blobInformer     informers.BlobInformer
	destination      string
	maximumRunning   int64
	minimumChunkSize int64
}

// NewClient creates a new CIDN client
func NewClient(client versioned.Interface, blobInformer informers.BlobInformer, destination string) *Client {
	return &Client{
		client:           client,
		blobInformer:     blobInformer,
		destination:      destination,
		maximumRunning:   10,
		minimumChunkSize: 128 * 1024 * 1024, // 128MB
	}
}

// SyncBlob creates a CIDN Blob resource and waits for it to complete synchronization
func (c *Client) SyncBlob(ctx context.Context, sourceURL, cachePath string) error {
	blobs := c.client.TaskV1alpha1().Blobs()
	name := getBlobName(cachePath)

	blob, err := c.blobInformer.Lister().Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("error getting blob from informer: %w", err)
		}

		blob, err = blobs.Create(ctx, &v1alpha1.Blob{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Annotations: map[string]string{
					v1alpha1.BlobDisplayNameAnnotation: sourceURL,
				},
			},
			Spec: v1alpha1.BlobSpec{
				MaximumRunning:   c.maximumRunning,
				MinimumChunkSize: c.minimumChunkSize,
				Source: []v1alpha1.BlobSource{
					{
						URL: sourceURL,
					},
				},
				Destination: []v1alpha1.BlobDestination{
					{
						Name:         c.destination,
						Path:         cachePath,
						SkipIfExists: true,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create blob: %w", err)
		}
	}

	// Check if already completed
	switch blob.Status.Phase {
	case v1alpha1.BlobPhaseSucceeded:
		return nil
	case v1alpha1.BlobPhaseFailed:
		errorMsg := "blob sync failed"
		for _, condition := range blob.Status.Conditions {
			if condition.Message != "" {
				errorMsg = condition.Message
				break
			}
		}
		return fmt.Errorf("blob sync failed: %s", errorMsg)
	}

	// Wait for completion
	return c.waitForBlobCompletion(ctx, name)
}

func (c *Client) waitForBlobCompletion(ctx context.Context, name string) error {
	statusChan := make(chan *v1alpha1.Blob, 1)
	defer close(statusChan)

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			blob, ok := obj.(*v1alpha1.Blob)
			if !ok {
				return
			}
			if blob.Name == name {
				statusChan <- blob
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldBlob, ok := oldObj.(*v1alpha1.Blob)
			if !ok {
				return
			}
			newBlob, ok := newObj.(*v1alpha1.Blob)
			if !ok {
				return
			}
			if newBlob.Name == name && oldBlob.Status.Phase != newBlob.Status.Phase {
				statusChan <- newBlob
			}
		},
		DeleteFunc: func(obj interface{}) {
			statusChan <- nil
		},
	}

	rer, err := c.blobInformer.Informer().AddEventHandler(handler)
	if err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}
	defer c.blobInformer.Informer().RemoveEventHandler(rer)

	// Set a reasonable timeout
	timeout := 30 * time.Minute
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case updatedBlob, ok := <-statusChan:
			if !ok {
				return fmt.Errorf("blob was cancelled before completion")
			}
			if updatedBlob == nil {
				return fmt.Errorf("blob was deleted before completion")
			}
			switch updatedBlob.Status.Phase {
			case v1alpha1.BlobPhaseSucceeded:
				return nil
			case v1alpha1.BlobPhaseFailed:
				errorMsg := "blob sync failed"
				for _, condition := range updatedBlob.Status.Conditions {
					if condition.Message != "" {
						errorMsg = condition.Message
						break
					}
				}
				return fmt.Errorf("blob sync failed: %s", errorMsg)
			}
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for blob sync: %w", timeoutCtx.Err())
		}
	}
}

func getBlobName(urlPath string) string {
	m := md5.Sum([]byte(urlPath))
	return hex.EncodeToString(m[:])
}

// blobCachePath returns the cache path for a blob digest following the Docker registry v2 layout
func blobCachePath(blob string) string {
	blob = cleanDigest(blob)
	return path.Join("/docker/registry/v2/blobs/sha256", blob[:2], blob, "data")
}

// manifestRevisionsCachePath returns the cache path for a manifest revision
func manifestRevisionsCachePath(host, image, blob string) string {
	blob = cleanDigest(blob)
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/revisions/sha256", blob, "link")
}

// manifestTagCachePath returns the cache path for a manifest tag
func manifestTagCachePath(host, image, tag string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags", tag, "current/link")
}

// cleanDigest removes the "sha256:" prefix if present
func cleanDigest(blob string) string {
	if strings.HasPrefix(blob, "sha256:") {
		return blob[7:]
	}
	return blob
}
