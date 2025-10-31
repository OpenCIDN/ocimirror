package blobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/apis/task/v1alpha1"
	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/ocimirror/internal/seeker"
	"github.com/OpenCIDN/ocimirror/internal/throttled"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/token"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"golang.org/x/time/rate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8scache "k8s.io/client-go/tools/cache"
)

var (
	prefix = "/v2/"
)

type BlobInfo struct {
	Host  string
	Image string

	Blobs string
}

type Blobs struct {
	httpClient *http.Client
	logger     *slog.Logger
	cache      *cache.Cache

	blobCacheDuration time.Duration
	blobCache         *blobsCache
	authenticator     *token.Authenticator

	noRedirect bool

	cidnClient       versioned.Interface
	cidnBlobInformer informers.BlobInformer
	cidnDestination  string
}

type Option func(c *Blobs) error

func WithCache(cache *cache.Cache) Option {
	return func(c *Blobs) error {
		c.cache = cache
		return nil
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *Blobs) error {
		c.logger = logger
		return nil
	}
}

func WithAuthenticator(authenticator *token.Authenticator) Option {
	return func(c *Blobs) error {
		c.authenticator = authenticator
		return nil
	}
}

func WithClient(client *http.Client) Option {
	return func(c *Blobs) error {
		c.httpClient = client
		return nil
	}
}

func WithNoRedirect(noRedirect bool) Option {
	return func(c *Blobs) error {
		c.noRedirect = noRedirect
		return nil
	}
}

func WithBlobCacheDuration(blobCacheDuration time.Duration) Option {
	return func(c *Blobs) error {
		if blobCacheDuration < 10*time.Second {
			blobCacheDuration = 10 * time.Second
		}
		c.blobCacheDuration = blobCacheDuration
		return nil
	}
}

func WithCIDNClient(cidnClient versioned.Interface, blobInformer informers.BlobInformer, destination string) Option {
	return func(c *Blobs) error {
		c.cidnClient = cidnClient
		c.cidnBlobInformer = blobInformer
		c.cidnDestination = destination
		return nil
	}
}

func NewBlobs(opts ...Option) (*Blobs, error) {
	c := &Blobs{
		logger:            slog.Default(),
		httpClient:        http.DefaultClient,
		blobCacheDuration: time.Hour,
	}

	for _, opt := range opts {
		opt(c)
	}

	ctx := context.Background()

	c.blobCache = newBlobsCache(c.blobCacheDuration)
	c.blobCache.Start(ctx, c.logger)

	return c, nil
}

// /v2/{source}/{path...}/blobs/sha256:{digest}

func parsePath(path string) (string, string, string, bool) {
	path = strings.TrimPrefix(path, prefix)
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		return "", "", "", false
	}
	if parts[len(parts)-2] != "blobs" {
		return "", "", "", false
	}
	source := parts[0]
	image := strings.Join(parts[1:len(parts)-2], "/")
	digest := parts[len(parts)-1]
	if !strings.HasPrefix(digest, "sha256:") {
		return "", "", "", false
	}
	return source, image, digest, true
}

func (b *Blobs) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	oriPath := r.URL.Path
	if !strings.HasPrefix(oriPath, prefix) {
		http.NotFound(rw, r)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		utils.ServeError(rw, r, errcode.ErrorCodeUnsupported, 0)
		return
	}

	if oriPath == prefix {
		utils.ResponseAPIBase(rw, r)
		return
	}

	source, image, digest, ok := parsePath(r.URL.Path)
	if !ok {
		http.NotFound(rw, r)
		return
	}

	info := &BlobInfo{
		Host:  source,
		Image: image,
		Blobs: digest,
	}

	var t token.Token
	var err error
	if b.authenticator != nil {
		t, err = b.authenticator.Authorization(r)
		if err != nil {
			utils.ServeError(rw, r, errcode.ErrorCodeDenied.WithMessage(err.Error()), 0)
			return
		}
	}

	if t.Block {
		if t.BlockMessage != "" {
			utils.ServeError(rw, r, errcode.ErrorCodeDenied.WithMessage(t.BlockMessage), 0)
		} else {
			utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
		}
		return
	}

	b.Serve(rw, r, info, &t)
}

func (b *Blobs) serveCache(rw http.ResponseWriter, r *http.Request, info *BlobInfo, t *token.Token) bool {
	ctx := r.Context()

	// When CIDN is configured, use the CIDN informer cache instead of our own cache
	if b.cidnClient != nil {
		blobName := info.Blobs
		blob, err := b.cidnBlobInformer.Lister().Get(blobName)
		if err == nil && blob.Status.Phase == v1alpha1.BlobPhaseSucceeded {
			// Blob exists in CIDN and is succeeded, check if it's in storage
			stat, err := b.cache.StatBlob(ctx, info.Blobs)
			if err == nil {
				if b.serveCachedBlobHead(rw, r, stat.Size()) {
					return true
				}

				b.serveCachedBlob(rw, r, info, t, stat.ModTime(), stat.Size())
				return true
			}
		}
		// Blob not found in CIDN informer or not succeeded yet
		return false
	}

	// Fall back to local cache when CIDN is not configured
	value, ok := b.blobCache.Get(info.Blobs)
	if ok {
		if value.Error != nil {
			utils.ServeError(rw, r, value.Error, 0)
			return true
		}

		if b.serveCachedBlobHead(rw, r, value.Size) {
			return true
		}

		b.serveCachedBlob(rw, r, info, t, value.ModTime, value.Size)
		return true
	}

	stat, err := b.cache.StatBlob(ctx, info.Blobs)
	if err == nil {
		if b.serveCachedBlobHead(rw, r, stat.Size()) {
			return true
		}

		b.serveCachedBlob(rw, r, info, t, stat.ModTime(), stat.Size())
		return true
	}
	return false
}

func (b *Blobs) Serve(rw http.ResponseWriter, r *http.Request, info *BlobInfo, t *token.Token) {
	if b.serveCache(rw, r, info, t) {
		return
	}

	// Use CIDN for blob syncing if configured
	if b.cidnClient != nil {
		err := b.cacheBlobWithCIDN(r.Context(), info)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "status code: got 404") {
				utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
				return
			} else if strings.Contains(errStr, "status code: got 403") {
				utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
				return
			} else if strings.Contains(errStr, "status code: got 401") {
				utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
				return
			}
			b.logger.Warn("failed to sync blob with CIDN", "error", err)
			utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
			return
		}
	} else {
		// Synchronously cache the blob on first request
		sc, err := b.cacheBlob(info)
		if err != nil {
			b.logger.Warn("failed download file", "info", info, "error", err)
			b.blobCache.PutError(info.Blobs, err, sc)
			utils.ServeError(rw, r, err, sc)
			return
		}
		b.logger.Info("finish caching blob", "info", info)
	}

	if b.serveCache(rw, r, info, t) {
		return
	}

	b.logger.Error("here should never be executed", "info", info)
	utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
}

func (b *Blobs) cacheBlob(info *BlobInfo) (int, error) {
	ctx := context.Background()
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", info.Image, info.Blobs),
	}
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		b.logger.Warn("failed to new request", "url", u.String(), "error", err)
		return 0, err
	}

	forwardReq.Header.Set("Accept", "*/*")

	resp, err := b.httpClient.Do(forwardReq)
	if err != nil {
		var tErr *transport.Error
		if errors.As(err, &tErr) {
			return http.StatusForbidden, errcode.ErrorCodeDenied
		}
		b.logger.Warn("failed to request", "url", u.String(), "error", err)
		return 0, errcode.ErrorCodeUnknown
	}

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		resp.Body.Close()
		return 0, errcode.ErrorCodeDenied
	}

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		resp.Body.Close()
		b.logger.Error("upstream denied", "statusCode", resp.StatusCode, "url", u.String())
		return 0, errcode.ErrorCodeDenied
	}
	if resp.StatusCode < http.StatusOK ||
		(resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest) {
		resp.Body.Close()
		b.logger.Error("upstream unkown code", "statusCode", resp.StatusCode, "url", u.String())
		return 0, errcode.ErrorCodeUnknown
	}

	if resp.StatusCode >= http.StatusBadRequest {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
		if err != nil {
			b.logger.Error("failed to get body", "statusCode", resp.StatusCode, "url", u.String(), "error", err)
			return 0, errcode.ErrorCodeUnknown
		}
		if !json.Valid(body) {
			b.logger.Error("invalid body", "statusCode", resp.StatusCode, "url", u.String(), "body", string(body))
			return 0, errcode.ErrorCodeDenied
		}
		var retErrs errcode.Errors
		err = retErrs.UnmarshalJSON(body)
		if err != nil {
			b.logger.Error("failed to unmarshal body", "statusCode", resp.StatusCode, "url", u.String(), "body", string(body))
			return 0, errcode.ErrorCodeUnknown
		}
		return resp.StatusCode, retErrs
	}

	// Download and cache the blob inline
	defer resp.Body.Close()

	size, err := b.cache.PutBlob(ctx, info.Blobs, resp.Body)
	if err != nil {
		return 0, err
	}

	stat, err := b.cache.StatBlob(ctx, info.Blobs)
	if err != nil {
		return 0, err
	}

	if size != stat.Size() {
		return 0, fmt.Errorf("size mismatch: expected %d, got %d", stat.Size(), size)
	}
	b.blobCache.Put(info.Blobs, stat.ModTime(), size)
	return 0, nil
}

func (b *Blobs) cacheBlobWithCIDN(ctx context.Context, info *BlobInfo) error {
	sourceURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", info.Host, info.Image, info.Blobs)
	cachePath := blobCachePath(info.Blobs)

	blobName := info.Blobs
	blobs := b.cidnClient.TaskV1alpha1().Blobs()

	blob, err := b.cidnBlobInformer.Lister().Get(blobName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			b.logger.Warn("error getting blob from informer", "error", err)
			return err
		}

		displayName := fmt.Sprintf("%s/%s@%s", info.Host, info.Image, info.Blobs)

		blob, err = blobs.Create(ctx, &v1alpha1.Blob{
			ObjectMeta: metav1.ObjectMeta{
				Name: blobName,
				Annotations: map[string]string{
					v1alpha1.BlobDisplayNameAnnotation: displayName,
				},
			},
			Spec: v1alpha1.BlobSpec{
				MaximumRunning:   3,
				MinimumChunkSize: 128 * 1024 * 1024,
				Source: []v1alpha1.BlobSource{
					{
						URL:        sourceURL,
						BearerName: fmt.Sprintf("%s:%s", info.Host, strings.ReplaceAll(info.Image, "/", ":")),
					},
				},
				Destination: []v1alpha1.BlobDestination{
					{
						Name:         b.cidnDestination,
						Path:         cachePath,
						SkipIfExists: true,
					},
				},
				ContentSha256: cleanDigest(info.Blobs),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else {
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
			return fmt.Errorf("CIDN blob sync failed: %s", errorMsg)
		}
	}

	// Create a channel to receive blob status updates
	statusChan := make(chan *v1alpha1.Blob, 1)
	defer close(statusChan)

	// Add event handler to watch for blob status changes
	handler := k8scache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newBlob, ok := newObj.(*v1alpha1.Blob)
			if !ok {
				return
			}
			if newBlob.Name == blobName {
				select {
				case statusChan <- newBlob:
				default:
				}
			}
		},
	}

	registration, err := b.cidnBlobInformer.Informer().AddEventHandler(handler)
	if err != nil {
		return err
	}
	defer func() {
		_ = b.cidnBlobInformer.Informer().RemoveEventHandler(registration)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blob := <-statusChan:
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
				return fmt.Errorf("CIDN blob sync failed: %s", errorMsg)
			}
		}
	}
}

func cleanDigest(blob string) string {
	return strings.TrimPrefix(blob, "sha256:")
}

func blobCachePath(blob string) string {
	blob = cleanDigest(blob)
	return path.Join("/docker/registry/v2/blobs/sha256", blob[:2], blob, "data")
}

func (b *Blobs) serveCachedBlobHead(rw http.ResponseWriter, r *http.Request, size int64) bool {
	if size != 0 && r.Method == http.MethodHead {
		rw.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		rw.Header().Set("Content-Type", "application/octet-stream")
		return true
	}
	return false
}

func (b *Blobs) serveCachedBlob(rw http.ResponseWriter, r *http.Request, info *BlobInfo, t *token.Token, modTime time.Time, size int64) {
	if t.AlwaysRedirect {
		b.serveCachedBlobRedirect(rw, r, info, t, modTime, size)
		return
	}

	if b.noRedirect {
		b.serveCachedBlobDirect(rw, r, info, t, modTime, size)
		return
	}

	b.serveCachedBlobRedirect(rw, r, info, t, modTime, size)
}

func (b *Blobs) serveCachedBlobDirect(rw http.ResponseWriter, r *http.Request, info *BlobInfo, t *token.Token, modTime time.Time, size int64) {

	ctx := r.Context()
	rw.Header().Set("Content-Type", "application/octet-stream")

	rs := seeker.NewReadSeekCloser(func(start int64) (io.ReadCloser, error) {
		data, err := b.cache.GetBlobWithOffset(ctx, info.Blobs, start)
		if err != nil {
			return nil, err
		}

		var body io.Reader = data
		if t.RateLimitPerSecond > 0 {
			limit := rate.NewLimiter(rate.Limit(t.RateLimitPerSecond), 16*1024)
			body = throttled.NewThrottledReader(ctx, body, limit)
		}

		return struct {
			io.Reader
			io.Closer
		}{
			Reader: body,
			Closer: data,
		}, nil
	}, size)
	defer rs.Close()

	http.ServeContent(rw, r, "", modTime, rs)

	b.blobCache.Put(info.Blobs, modTime, size)
}

func (b *Blobs) serveCachedBlobRedirect(rw http.ResponseWriter, r *http.Request, info *BlobInfo, t *token.Token, modTime time.Time, size int64) {
	referer := r.RemoteAddr
	if info != nil {
		referer = fmt.Sprintf("%d-%d:%s:%s/%s", t.RegistryID, t.TokenID, referer, info.Host, info.Image)
	}

	u, err := b.cache.RedirectBlob(r.Context(), info.Blobs, referer)
	if err != nil {
		b.logger.Info("failed to redirect blob", "digest", info.Blobs, "error", err)
		b.blobCache.Remove(info.Blobs)
		utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
		return
	}

	b.blobCache.Put(info.Blobs, modTime, size)

	b.logger.Info("Cache hit", "digest", info.Blobs, "url", u)
	http.Redirect(rw, r, u, http.StatusTemporaryRedirect)
}
