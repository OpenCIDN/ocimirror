package cidn

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8scache "k8s.io/client-go/tools/cache"
)

type CIDN struct {
	Client        versioned.Interface
	BlobInformer  informers.BlobInformer
	ChunkInformer informers.ChunkInformer
	Destination   string
}

type Response struct {
	StatusCode int
	Headers    map[string]string
}

func (c *CIDN) Blob(ctx context.Context, host, image, digest string, forceAcceptRanges bool) error {
	sourceURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, image, digest)
	cachePath := registry.BlobCachePath(digest)

	blobName := blobName(host, image, digest)
	blobs := c.Client.TaskV1alpha1().Blobs()

	var create bool
	if blob, err := c.BlobInformer.Lister().Get(blobName); err == nil {
		switch blob.Status.Phase {
		case v1alpha1.BlobPhaseSucceeded:
			err := blobs.Delete(ctx, blobName, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete blob %s: %w", blobName, err)
			}
			create = true
		case v1alpha1.BlobPhaseFailed:
			return fmt.Errorf("blob sync failed: %s", firstNonEmptyConditionMessage(blob.Status.Conditions, "blob sync failed"))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	} else {
		create = true
	}

	if create {
		displayName := fmt.Sprintf("%s/%s@%s", formatHost(host), image, digest)
		annotations := map[string]string{
			v1alpha1.WebuiDisplayNameAnnotation: displayName,
			v1alpha1.WebuiTagAnnotation:         "blob",
			v1alpha1.ReleaseTTLAnnotation:       "1h",
			v1alpha1.WebuiGroupAnnotation:       fmt.Sprintf("%s/%s", formatHost(host), image),
		}

		_, err := blobs.Create(ctx, &v1alpha1.Blob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        blobName,
				Annotations: annotations,
			},
			Spec: v1alpha1.BlobSpec{
				MaximumRetry:     3,
				MaximumRunning:   3,
				MaximumPending:   1,
				MinimumChunkSize: 128 * 1024 * 1024,
				Source: []v1alpha1.BlobSource{
					{
						URL:        sourceURL,
						BearerName: bearerName(host, image),
					},
				},
				Destination: []v1alpha1.BlobDestination{
					{
						Name:         c.Destination,
						Path:         cachePath,
						SkipIfExists: true,
					},
				},
				ForceAcceptRanges: forceAcceptRanges,
				ContentSha256:     registry.CleanDigest(digest),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	// Wait without extra timeout; rely on ctx
	b, err := waitForBlob(ctx, c.BlobInformer, blobName, 0)
	if err != nil {
		return err
	}
	switch b.Status.Phase {
	case v1alpha1.BlobPhaseSucceeded:
		return nil
	case v1alpha1.BlobPhaseFailed:
		return fmt.Errorf("blob sync failed: %s", firstNonEmptyConditionMessage(b.Status.Conditions, "blob sync failed"))
	default:
		return fmt.Errorf("unexpected blob phase: %s", b.Status.Phase)
	}
}

func (c *CIDN) ManifestTag(ctx context.Context, host, image, tag string) (*Response, error) {
	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", image, tag),
	}
	reqURL := u.String()

	chunkName := manifestName(host, image, tag)
	chunks := c.Client.TaskV1alpha1().Chunks()

	if chunk, err := c.ChunkInformer.Lister().Get(chunkName); err == nil {
		switch chunk.Status.Phase {
		case v1alpha1.ChunkPhaseSucceeded:
			return &Response{
				StatusCode: chunk.Status.SourceResponse.StatusCode,
				Headers:    chunk.Status.SourceResponse.Headers,
			}, nil
		case v1alpha1.ChunkPhaseFailed:
			if !chunk.Status.Retryable {
				return nil, fmt.Errorf("CIDN manifest sync failed: %s", firstNonEmptyConditionMessage(chunk.Status.Conditions, "manifest sync failed"))
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get chunk from informer error: %w", err)
	} else {
		displayName := fmt.Sprintf("%s/%s:%s", formatHost(host), image, tag)

		annotations := map[string]string{
			v1alpha1.WebuiDisplayNameAnnotation: displayName,
			v1alpha1.WebuiTagAnnotation:         "manifest",
			v1alpha1.ReleaseTTLAnnotation:       "1h",
			v1alpha1.WebuiGroupAnnotation:       fmt.Sprintf("%s/%s", formatHost(host), image),
		}

		_, err = chunks.Create(ctx, &v1alpha1.Chunk{
			ObjectMeta: metav1.ObjectMeta{
				Name:        chunkName,
				Annotations: annotations,
			},
			Spec: v1alpha1.ChunkSpec{
				MaximumRetry: 3,
				Priority:     1,
				Source: v1alpha1.ChunkHTTP{
					Request: v1alpha1.ChunkHTTPRequest{
						Method: http.MethodHead,
						URL:    reqURL,
						Headers: map[string]string{
							"Accept": registry.OCIAcceptsValue,
						},
					},
					Response: v1alpha1.ChunkHTTPResponse{
						StatusCode: http.StatusOK,
					},
				},
				BearerName: bearerName(host, image),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create chunk error: %w", err)
		}
	}

	ch, err := waitForChunkCompletion(ctx, c.ChunkInformer, chunkName, 10*time.Minute)
	if err != nil {
		return nil, err
	}
	switch ch.Status.Phase {
	case v1alpha1.ChunkPhaseSucceeded:
		return &Response{
			StatusCode: ch.Status.SourceResponse.StatusCode,
			Headers:    ch.Status.SourceResponse.Headers,
		}, nil
	case v1alpha1.ChunkPhaseFailed:
		return nil, fmt.Errorf("CIDN manifest sync failed: %s", firstNonEmptyConditionMessage(ch.Status.Conditions, "manifest sync failed"))
	default:
		return nil, fmt.Errorf("unexpected chunk phase: %s", ch.Status.Phase)
	}
}

func (c *CIDN) ManifestDigest(ctx context.Context, host, image, digest, manifest string) error {
	sourceURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, image, manifest)
	cachePath := registry.BlobCachePath(digest)

	blobName := manifestName(host, image, digest)
	blobs := c.Client.TaskV1alpha1().Blobs()

	var create bool
	if blob, err := c.BlobInformer.Lister().Get(blobName); err == nil {
		switch blob.Status.Phase {
		case v1alpha1.BlobPhaseSucceeded:
			err := blobs.Delete(ctx, blobName, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete blob %s: %w", blobName, err)
			}
			create = true
		case v1alpha1.BlobPhaseFailed:
			return fmt.Errorf("blob sync failed: %s", firstNonEmptyConditionMessage(blob.Status.Conditions, "blob sync failed"))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	} else {
		create = true
	}

	if create {
		displayName := fmt.Sprintf("%s/%s@%s", formatHost(host), image, digest)
		annotations := map[string]string{
			v1alpha1.WebuiDisplayNameAnnotation: displayName,
			v1alpha1.WebuiTagAnnotation:         "manifest",
			v1alpha1.ReleaseTTLAnnotation:       "1h",
			v1alpha1.WebuiGroupAnnotation:       fmt.Sprintf("%s/%s", formatHost(host), image),
		}

		_, err := blobs.Create(ctx, &v1alpha1.Blob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        blobName,
				Annotations: annotations,
			},
			Spec: v1alpha1.BlobSpec{
				MaximumRetry:   3,
				MaximumRunning: 1,
				MaximumPending: 1,
				ChunksNumber:   1,
				Priority:       1,
				Source: []v1alpha1.BlobSource{
					{
						URL:        sourceURL,
						BearerName: bearerName(host, image),
						Headers: map[string]string{
							"Accept": registry.OCIAcceptsValue,
						},
					},
				},
				Destination: []v1alpha1.BlobDestination{
					{
						Name:         c.Destination,
						Path:         cachePath,
						SkipIfExists: true,
					},
				},
				ContentSha256: registry.CleanDigest(digest),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create blob error: %w", err)
		}
	}

	// Wait with a 10m timeout like original
	b, err := waitForBlob(ctx, c.BlobInformer, blobName, 10*time.Minute)
	if err != nil {
		return err
	}
	switch b.Status.Phase {
	case v1alpha1.BlobPhaseSucceeded:
		return nil
	case v1alpha1.BlobPhaseFailed:
		return fmt.Errorf("blob sync failed: %s", firstNonEmptyConditionMessage(b.Status.Conditions, "blob sync failed"))
	default:
		return fmt.Errorf("unexpected blob phase: %s", b.Status.Phase)
	}
}

func manifestName(host, image, reference string) string {
	return fmt.Sprintf("manifest:%s:%s:%s", host, strings.ReplaceAll(image, "/", ":"), reference)
}

func blobName(host string, image string, digest string) string {
	return fmt.Sprintf("%s:%s:%s", host, strings.ReplaceAll(image, "/", ":"), digest)
}

func bearerName(host, image string) string {
	return fmt.Sprintf("%s:%s", host, strings.ReplaceAll(image, "/", ":"))
}

func firstNonEmptyConditionMessage(conditions []v1alpha1.Condition, fallback string) string {
	for _, c := range conditions {
		if c.Message != "" {
			return c.Message
		}
	}
	return fallback
}

func waitForBlob(ctx context.Context, informer informers.BlobInformer, name string, timeout time.Duration) (*v1alpha1.Blob, error) {
	statusChan := make(chan *v1alpha1.Blob, 1)

	handler := k8scache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if b, ok := obj.(*v1alpha1.Blob); ok && b.Name == name {
				statusChan <- b
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if b, ok := newObj.(*v1alpha1.Blob); ok && b.Name == name {
				statusChan <- b
			}
		},
		DeleteFunc: func(obj interface{}) {
			if b, ok := obj.(*v1alpha1.Blob); ok && b.Name == name {
				statusChan <- nil
			}
		},
	}
	reg, err := informer.Informer().AddEventHandler(handler)
	if err != nil {
		return nil, err
	}
	defer informer.Informer().RemoveEventHandler(reg)

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case b, ok := <-statusChan:
			if !ok {
				return nil, fmt.Errorf("chunk was cancel before completion")
			}
			if b == nil {
				return nil, fmt.Errorf("chunk was deleted before completion")
			}
			switch b.Status.Phase {
			case v1alpha1.BlobPhaseSucceeded, v1alpha1.BlobPhaseFailed:
				return b, nil
			}
		}
	}
}

func waitForChunkCompletion(ctx context.Context, informer informers.ChunkInformer, name string, timeout time.Duration) (*v1alpha1.Chunk, error) {
	statusChan := make(chan *v1alpha1.Chunk, 1)

	handler := k8scache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if ch, ok := obj.(*v1alpha1.Chunk); ok && ch.Name == name {
				statusChan <- ch
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if ch, ok := newObj.(*v1alpha1.Chunk); ok && ch.Name == name {
				statusChan <- ch
			}
		},
		DeleteFunc: func(obj interface{}) {
			if ch, ok := obj.(*v1alpha1.Chunk); ok && ch.Name == name {
				statusChan <- nil
			}
		},
	}
	reg, err := informer.Informer().AddEventHandler(handler)
	if err != nil {
		return nil, err
	}
	defer func() { _ = informer.Informer().RemoveEventHandler(reg) }()

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ch, ok := <-statusChan:
			if !ok {
				return nil, fmt.Errorf("blob was cancel before completion")
			}
			if ch == nil {
				return nil, fmt.Errorf("blob was deleted before completion")
			}
			switch ch.Status.Phase {
			case v1alpha1.ChunkPhaseSucceeded:
				return ch, nil
			case v1alpha1.ChunkPhaseFailed:
				if !ch.Status.Retryable {
					return ch, nil
				}
			}
		}
	}
}

var (
	legacyDefaultDomain = map[string]struct{}{
		"index.docker.io":      {},
		"registry-1.docker.io": {},
	}
)

func isLegacyDefaultDomain(name string) bool {
	_, ok := legacyDefaultDomain[name]
	return ok
}

func formatHost(host string) string {
	if isLegacyDefaultDomain(host) {
		return "docker.io"
	}
	return host
}
