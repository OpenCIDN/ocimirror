package cidn

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	typedv1alpha1 "github.com/OpenCIDN/cidn/pkg/clientset/versioned/typed/task/v1alpha1"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	listers "github.com/OpenCIDN/cidn/pkg/listers/task/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8scache "k8s.io/client-go/tools/cache"
)

const (
	testImage  = "library/alpine"
	testSha256 = "4abcf20661432fb2d719aaf90656f55c287f8ca915dc1c92ec14ff61e67fbaf8"
	testDigest = "sha256:" + testSha256
	testPath   = "/docker/registry/v2/blobs/sha256/4a/" + testSha256 + "/data"
)

var errCreateCaptured = errors.New("create captured")

type captureClient struct {
	versioned.Interface
	typedv1alpha1.TaskV1alpha1Interface
	typedv1alpha1.BlobInterface
	created *v1alpha1.Blob
}

func (c *captureClient) TaskV1alpha1() typedv1alpha1.TaskV1alpha1Interface { return c }

func (c *captureClient) Blobs() typedv1alpha1.BlobInterface { return c }

// Failing the create makes Blob() return before it waits on the informer.
func (c *captureClient) Create(_ context.Context, blob *v1alpha1.Blob, _ metav1.CreateOptions) (*v1alpha1.Blob, error) {
	c.created = blob
	return nil, errCreateCaptured
}

type emptyBlobInformer struct {
	informers.BlobInformer
}

func (emptyBlobInformer) Lister() listers.BlobLister {
	return listers.NewBlobLister(k8scache.NewIndexer(k8scache.MetaNamespaceKeyFunc, k8scache.Indexers{}))
}

func TestBlobCreatesObject(t *testing.T) {
	tests := []struct {
		host     string
		priority int64
		want     *v1alpha1.Blob
	}{
		{
			host:     "ollama.com",
			priority: 2,
			want: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ollama.com:library:alpine:" + testDigest,
					Annotations: map[string]string{
						v1alpha1.WebuiDisplayNameAnnotation: testDigest,
						v1alpha1.WebuiTagAnnotation:         "blob,P2",
						v1alpha1.ReleaseTTLAnnotation:       "1h",
						v1alpha1.WebuiGroupAnnotation:       "ollama.com/library/alpine",
					},
				},
				Spec: v1alpha1.BlobSpec{
					MaximumRetry:     3,
					MaximumRunning:   5,
					MaximumPending:   1,
					MinimumChunkSize: 256 << 20,
					Priority:         2,
					Source: []v1alpha1.BlobSource{{
						URL:        "https://ollama.com/v2/library/alpine/blobs/" + testDigest,
						BearerName: "ollama.com:library:alpine",
					}},
					Destination: []v1alpha1.BlobDestination{{
						Name:         "s3",
						Path:         testPath,
						SkipIfExists: true,
					}},
					ForceAcceptRanges: true,
					ContentSha256:     testSha256,
				},
			},
		},
		{
			host: "registry-1.docker.io",
			want: &v1alpha1.Blob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "registry-1.docker.io:library:alpine:" + testDigest,
					Annotations: map[string]string{
						v1alpha1.WebuiDisplayNameAnnotation: testDigest,
						v1alpha1.WebuiTagAnnotation:         "blob",
						v1alpha1.ReleaseTTLAnnotation:       "1h",
						v1alpha1.WebuiGroupAnnotation:       "docker.io/library/alpine",
					},
				},
				Spec: v1alpha1.BlobSpec{
					MaximumRetry:     3,
					MaximumRunning:   5,
					MaximumPending:   1,
					MinimumChunkSize: 256 << 20,
					Source: []v1alpha1.BlobSource{{
						URL:        "https://registry-1.docker.io/v2/library/alpine/blobs/" + testDigest,
						BearerName: "registry-1.docker.io:library:alpine",
					}},
					Destination: []v1alpha1.BlobDestination{{
						Name:         "s3",
						Path:         testPath,
						SkipIfExists: true,
					}},
					ContentSha256: testSha256,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			client := &captureClient{}
			c := &CIDN{
				client:           client,
				blobInformer:     emptyBlobInformer{},
				destination:      "s3",
				maximumRunning:   5,
				minimumChunkSize: 256 << 20,
			}
			err := c.Blob(context.Background(), tt.host, testImage, testDigest, tt.priority)
			if !errors.Is(err, errCreateCaptured) {
				t.Fatalf("Blob() error = %v, want %v", err, errCreateCaptured)
			}
			if !reflect.DeepEqual(client.created, tt.want) {
				t.Errorf("Blob() created\n%+v\nwant\n%+v", client.created, tt.want)
			}
		})
	}
}

func TestNewBlob(t *testing.T) {
	type transfer struct {
		ChunksNumber      int64
		MaximumRunning    int64
		MinimumChunkSize  int64
		ForceAcceptRanges bool
	}
	var (
		defaultTransfer = transfer{MaximumRunning: 5, MinimumChunkSize: 256 << 20}
		forceRange      = transfer{MaximumRunning: 5, MinimumChunkSize: 256 << 20, ForceAcceptRanges: true}
		singleRequest   = transfer{ChunksNumber: 1, MaximumRunning: 1}
	)
	tests := []struct {
		host  string
		group string
		want  transfer
	}{
		{host: "public.ecr.aws", group: "public.ecr.aws/library/alpine", want: singleRequest},
		{host: "ollama.com", group: "ollama.com/library/alpine", want: forceRange},
		{host: "registry-1.docker.io", group: "docker.io/library/alpine", want: defaultTransfer},
		{host: "ghcr.io", group: "ghcr.io/library/alpine", want: defaultTransfer},
		{host: "123456789012.dkr.ecr.us-east-1.amazonaws.com", group: "123456789012.dkr.ecr.us-east-1.amazonaws.com/library/alpine", want: defaultTransfer},
		{host: "ecr.aws", group: "ecr.aws/library/alpine", want: defaultTransfer},
		{host: "xpublic.ecr.aws", group: "xpublic.ecr.aws/library/alpine", want: defaultTransfer},
	}
	c := &CIDN{destination: "s3", maximumRunning: 5, minimumChunkSize: 256 << 20}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			blob := c.newBlob(tt.host, testImage, testDigest, 2)
			spec := blob.Spec

			got := transfer{spec.ChunksNumber, spec.MaximumRunning, spec.MinimumChunkSize, spec.ForceAcceptRanges}
			if got != tt.want {
				t.Errorf("transfer = %+v, want %+v", got, tt.want)
			}

			wantSource := []v1alpha1.BlobSource{{
				URL:        "https://" + tt.host + "/v2/library/alpine/blobs/" + testDigest,
				BearerName: tt.host + ":library:alpine",
			}}
			if !reflect.DeepEqual(spec.Source, wantSource) {
				t.Errorf("Source = %+v, want %+v", spec.Source, wantSource)
			}
			wantDestination := []v1alpha1.BlobDestination{{Name: "s3", Path: testPath, SkipIfExists: true}}
			if !reflect.DeepEqual(spec.Destination, wantDestination) {
				t.Errorf("Destination = %+v, want %+v", spec.Destination, wantDestination)
			}
			if spec.ContentSha256 != testSha256 {
				t.Errorf("ContentSha256 = %q, want %q", spec.ContentSha256, testSha256)
			}
			if spec.MaximumRetry != 3 {
				t.Errorf("MaximumRetry = %d, want 3", spec.MaximumRetry)
			}
			if spec.MaximumPending != 1 {
				t.Errorf("MaximumPending = %d, want 1", spec.MaximumPending)
			}
			if spec.Priority != 2 {
				t.Errorf("Priority = %d, want 2", spec.Priority)
			}
			wantAnnotations := map[string]string{
				v1alpha1.WebuiDisplayNameAnnotation: testDigest,
				v1alpha1.WebuiTagAnnotation:         "blob,P2",
				v1alpha1.ReleaseTTLAnnotation:       "1h",
				v1alpha1.WebuiGroupAnnotation:       tt.group,
			}
			if !reflect.DeepEqual(blob.Annotations, wantAnnotations) {
				t.Errorf("Annotations = %v, want %v", blob.Annotations, wantAnnotations)
			}
		})
	}
}
