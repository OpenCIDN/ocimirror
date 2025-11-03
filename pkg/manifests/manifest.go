package manifests

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
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/token"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8scache "k8s.io/client-go/tools/cache"
)

type Manifests struct {
	httpClient *http.Client
	logger     *slog.Logger
	cache      *cache.Cache

	manifestCacheDuration time.Duration
	manifestCache         *manifestCache

	acceptsItems []string
	acceptsStr   string
	accepts      map[string]struct{}

	cidnClient        versioned.Interface
	cidnChunkInformer informers.ChunkInformer
	cidnBlobInformer  informers.BlobInformer
	cidnDestination   string
}

type Option func(c *Manifests)

func WithClient(client *http.Client) Option {
	return func(c *Manifests) {
		c.httpClient = client
	}
}

func WithManifestCacheDuration(manifestCacheDuration time.Duration) Option {
	return func(c *Manifests) {
		if manifestCacheDuration < 10*time.Second {
			manifestCacheDuration = 10 * time.Second
		}
		c.manifestCacheDuration = manifestCacheDuration
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *Manifests) {
		c.logger = logger
	}
}

func WithCache(cache *cache.Cache) Option {
	return func(c *Manifests) {
		c.cache = cache
	}
}

func WithCIDNClient(cidnClient versioned.Interface, cidnBlobInformer informers.BlobInformer, chunkInformer informers.ChunkInformer, destination string) Option {
	return func(c *Manifests) {
		c.cidnClient = cidnClient
		c.cidnChunkInformer = chunkInformer
		c.cidnBlobInformer = cidnBlobInformer
		c.cidnDestination = destination
	}
}

func NewManifests(opts ...Option) (*Manifests, error) {
	c := &Manifests{
		logger:     slog.Default(),
		httpClient: http.DefaultClient,
		acceptsItems: []string{
			"application/vnd.oci.image.index.v1+json",
			"application/vnd.docker.distribution.manifest.list.v2+json",
			"application/vnd.oci.image.manifest.v1+json",
			"application/vnd.docker.distribution.manifest.v2+json",
		},
		accepts:               map[string]struct{}{},
		manifestCacheDuration: time.Minute,
	}

	for _, item := range c.acceptsItems {
		c.accepts[item] = struct{}{}
	}
	c.acceptsStr = strings.Join(c.acceptsItems, ",")

	for _, opt := range opts {
		opt(c)
	}

	ctx := context.Background()
	c.manifestCache = newManifestCache(c.manifestCacheDuration)
	c.manifestCache.Start(ctx, c.logger)

	return c, nil
}

func (c *Manifests) Serve(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	done := c.tryFirstServeCachedManifest(rw, r, info, t)
	if done {
		return
	}

	ok, _ := c.cache.StatManifest(r.Context(), info.Host, info.Image, info.Manifests)
	if ok {
		// Use CIDN for manifest syncing if configured
		if c.cidnClient != nil {
			sc, err := c.cacheManifestWithCIDN(info)
			if err != nil {
				c.manifestCache.PutError(info, err, sc)
			}
		} else {
			// Synchronously cache the manifest
			sc, err := c.cacheManifest(info)
			if err != nil {
				c.manifestCache.PutError(info, err, sc)
			}
		}
		if c.serveCachedManifest(rw, r, info, true, "cached") {
			return
		}
	} else {
		// Use CIDN for manifest syncing if configured
		if c.cidnClient != nil {
			// Synchronously cache the manifest
			sc, err := c.cacheManifestWithCIDN(info)
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
				} else if strings.Contains(errStr, "status code: got 412") {
					// For gcr.io
					// unexpected status code 412 Precondition Failed: Container Registry is deprecated and shutting down, please use the auto migration tool to migrate to Artifact Registry (gcloud artifacts docker upgrade migrate --projects='arrikto'). For more details see: https://cloud.google.com/artifact-registry/docs/transition/auto-migrate-gcr-ar"
					utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
					return
				} else if strings.Contains(errStr, "unsupported target response") {
					utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
					return
				} else if strings.Contains(errStr, "DENIED") {
					utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
					return
				}

				c.logger.Warn("failed to cache manifest with cidn", "info", info, "error", err)
				c.manifestCache.PutError(info, err, sc)
				utils.ServeError(rw, r, err, sc)
				return
			}
			c.logger.Info("finish caching manifest", "info", info)
		} else {
			// Synchronously cache the manifest
			sc, err := c.cacheManifest(info)
			if err != nil {
				c.logger.Warn("failed to cache manifest", "info", info, "error", err)
				c.manifestCache.PutError(info, err, sc)
				utils.ServeError(rw, r, err, sc)
				return
			}
			c.logger.Info("finish caching manifest", "info", info)
		}
		if c.missServeCachedManifest(rw, r, info) {
			return
		}
	}

	c.logger.Error("here should never be executed", "info", info)
	utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
}

func (c *Manifests) cacheManifest(info *PathInfo) (int, error) {
	ctx := context.Background()
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", info.Image, info.Manifests),
	}

	if !info.IsDigestManifests && info.Host != "ollama.com" {
		forwardReq, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return 0, err
		}
		// Never trust a client's Accept !!!
		forwardReq.Header.Set("Accept", c.acceptsStr)

		resp, err := c.httpClient.Do(forwardReq)
		if err != nil {
			var tErr *transport.Error
			if errors.As(err, &tErr) {
				return http.StatusForbidden, errcode.ErrorCodeDenied
			}
			c.logger.Warn("failed to request", "url", u.String(), "error", err)
			return 0, errcode.ErrorCodeUnknown
		}
		if resp.Body != nil {
			resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return 0, errcode.ErrorCodeDenied
		}
		if resp.StatusCode < http.StatusOK ||
			(resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest) {
			return 0, errcode.ErrorCodeUnknown
		}

		digest := resp.Header.Get("Docker-Content-Digest")
		if digest == "" {
			return 0, errcode.ErrorCodeDenied
		}

		err = c.cache.RelinkManifest(ctx, info.Host, info.Image, info.Manifests, digest)
		if err == nil {
			c.logger.Info("relink manifest", "url", u.String())
			c.manifestCache.Put(info, cacheValue{
				Digest: digest,
			})
			return 0, nil
		}
		u.Path = fmt.Sprintf("/v2/%s/manifests/%s", info.Image, digest)
	}

	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	// Never trust a client's Accept !!!
	forwardReq.Header.Set("Accept", c.acceptsStr)

	resp, err := c.httpClient.Do(forwardReq)
	if err != nil {
		var tErr *transport.Error
		if errors.As(err, &tErr) {
			return http.StatusForbidden, errcode.ErrorCodeDenied
		}
		c.logger.Warn("failed to request", "url", u.String(), "error", err)
		return 0, errcode.ErrorCodeUnknown
	}
	defer func() {
		resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		c.logger.Error("upstream denied", "statusCode", resp.StatusCode, "url", u.String(), "response", dumpResponse(resp))
		return 0, errcode.ErrorCodeDenied
	}
	if resp.StatusCode < http.StatusOK ||
		(resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest) {
		c.logger.Error("upstream unkown code", "statusCode", resp.StatusCode, "url", u.String(), "response", dumpResponse(resp))
		return 0, errcode.ErrorCodeUnknown
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		c.logger.Error("failed to get body", "statusCode", resp.StatusCode, "url", u.String(), "error", err)
		return 0, errcode.ErrorCodeUnknown
	}
	if !json.Valid(body) {
		c.logger.Error("invalid body", "statusCode", resp.StatusCode, "url", u.String(), "body", string(body))
		return 0, errcode.ErrorCodeDenied
	}

	if resp.StatusCode >= http.StatusBadRequest {
		var retErrs errcode.Errors
		err = retErrs.UnmarshalJSON(body)
		if err != nil {
			c.logger.Error("failed to unmarshal body", "statusCode", resp.StatusCode, "url", u.String(), "body", string(body))
			return 0, errcode.ErrorCodeUnknown
		}
		return resp.StatusCode, retErrs
	}

	size, digest, mediaType, err := c.cache.PutManifestContent(ctx, info.Host, info.Image, info.Manifests, body)
	if err != nil {
		return 0, err
	}

	c.manifestCache.Put(info, cacheValue{
		Digest:    digest,
		MediaType: mediaType,
		Length:    strconv.FormatInt(size, 10),
	})
	return 0, nil
}

func (c *Manifests) cacheManifestWithCIDN(info *PathInfo) (int, error) {
	ctx := context.Background()

	if !info.IsDigestManifests && info.Host != "ollama.com" {
		resp, err := c.requestWithCIDN(ctx, info, http.MethodHead)
		if err != nil {
			return 0, fmt.Errorf("request with cidn error: %w", err)
		}

		digest := resp.Headers["docker-content-digest"]
		if digest == "" {
			return 0, errcode.ErrorCodeDenied
		}

		err = c.cache.RelinkManifest(ctx, info.Host, info.Image, info.Manifests, digest)
		if err == nil {
			c.logger.Info("relink manifest", "info", info)
			c.manifestCache.Put(info, cacheValue{
				Digest: digest,
			})
			return 0, nil
		}

		err = c.cacheBlobWithCIDN(ctx, &PathInfo{
			Host:              info.Host,
			Image:             info.Image,
			Manifests:         digest,
			IsDigestManifests: true,
		})
		if err != nil {
			return 0, fmt.Errorf("cache blob with cidn error: %w", err)
		}
		err = c.cache.RelinkManifest(ctx, info.Host, info.Image, info.Manifests, digest)
		if err == nil {
			c.logger.Info("relink manifest", "info", info)
			c.manifestCache.Put(info, cacheValue{
				Digest: digest,
			})
			return 0, nil
		}

		return 0, fmt.Errorf("failed to relink manifest after caching blob with CIDN")
	} else {
		err := c.cacheBlobWithCIDN(ctx, info)
		if err != nil {
			return 0, fmt.Errorf("cache blob with cidn error: %w", err)
		}
		err = c.cache.RelinkManifest(ctx, info.Host, info.Image, "", info.Manifests)
		if err == nil {
			c.logger.Info("relink manifest", "info", info)
			c.manifestCache.Put(info, cacheValue{
				Digest: info.Manifests,
			})
			return 0, nil
		}

		return 0, fmt.Errorf("failed to relink manifest after caching blob with CIDN")
	}
}

type response struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

func (c *Manifests) requestWithCIDN(ctx context.Context, info *PathInfo, method string) (*response, error) {
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", info.Image, info.Manifests),
	}

	url := u.String()

	chunkName := fmt.Sprintf("manifest:%s:%s:%s", info.Host, strings.ReplaceAll(info.Image, "/", ":"), info.Manifests)
	chunks := c.cidnClient.TaskV1alpha1().Chunks()

	chunk, err := c.cidnChunkInformer.Lister().Get(chunkName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			c.logger.Warn("error getting blob from informer", "error", err)
			return nil, fmt.Errorf("get chunk from informer error: %w", err)
		}

		chunk, err = chunks.Create(ctx, &v1alpha1.Chunk{
			ObjectMeta: metav1.ObjectMeta{
				Name: chunkName,
			},
			Spec: v1alpha1.ChunkSpec{
				MaximumRetry: 3,
				Source: v1alpha1.ChunkHTTP{
					Request: v1alpha1.ChunkHTTPRequest{
						Method: method,
						URL:    url,
						Headers: map[string]string{
							"Accept": c.acceptsStr,
						},
					},
					Response: v1alpha1.ChunkHTTPResponse{
						StatusCode: http.StatusOK,
					},
				},
				BearerName: fmt.Sprintf("%s:%s", info.Host, strings.ReplaceAll(info.Image, "/", ":")),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create chunk error: %w", err)
		}

	} else {
		switch chunk.Status.Phase {
		case v1alpha1.ChunkPhaseSucceeded:
			return &response{
				StatusCode: chunk.Status.SourceResponse.StatusCode,
				Headers:    chunk.Status.SourceResponse.Headers,
				Body:       chunk.Status.ResponseBody,
			}, nil
		case v1alpha1.ChunkPhaseFailed:
			if !chunk.Status.Retryable {
				errorMsg := "manifest sync failed"
				for _, condition := range chunk.Status.Conditions {
					if condition.Message != "" {
						errorMsg = condition.Message
						break
					}
				}
				return nil, fmt.Errorf("CIDN manifest sync failed: %s", errorMsg)
			}
		}
	}

	// Create a channel to receive blob status updates
	statusChan := make(chan *v1alpha1.Chunk, 1)
	defer close(statusChan)

	// Add event handler to watch for blob status changes
	handler := k8scache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newChunk, ok := newObj.(*v1alpha1.Chunk)
			if !ok {
				return
			}
			if newChunk.Name == chunk.Name {
				select {
				case statusChan <- newChunk:
				default:
				}
			}
		},
	}

	registration, err := c.cidnChunkInformer.Informer().AddEventHandler(handler)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = c.cidnChunkInformer.Informer().RemoveEventHandler(registration)
	}()

	// Wait for blob to complete or fail
	timeout := time.After(10 * time.Minute)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for CIDN manifest sync")
		case chunk := <-statusChan:
			switch chunk.Status.Phase {
			case v1alpha1.ChunkPhaseSucceeded:
				return &response{
					StatusCode: chunk.Status.SourceResponse.StatusCode,
					Headers:    chunk.Status.SourceResponse.Headers,
					Body:       chunk.Status.ResponseBody,
				}, nil
			case v1alpha1.ChunkPhaseFailed:
				if !chunk.Status.Retryable {
					errorMsg := "manifest sync failed"
					for _, condition := range chunk.Status.Conditions {
						if condition.Message != "" {
							errorMsg = condition.Message
							break
						}
					}
					return nil, fmt.Errorf("CIDN manifest sync failed: %s", errorMsg)
				}
			}
		}
	}
}

func (c *Manifests) cacheBlobWithCIDN(ctx context.Context, info *PathInfo) error {
	sourceURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", info.Host, info.Image, info.Manifests)
	cachePath := blobCachePath(info.Manifests)

	blobName := info.Manifests
	blobs := c.cidnClient.TaskV1alpha1().Blobs()

	blob, err := c.cidnBlobInformer.Lister().Get(blobName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			c.logger.Warn("error getting blob from informer", "error", err)
			return err
		}

		displayName := fmt.Sprintf("%s/%s@%s", info.Host, info.Image, info.Manifests)

		blob, err = blobs.Create(ctx, &v1alpha1.Blob{
			ObjectMeta: metav1.ObjectMeta{
				Name: blobName,
				Annotations: map[string]string{
					v1alpha1.BlobDisplayNameAnnotation: displayName,
				},
			},
			Spec: v1alpha1.BlobSpec{
				MaximumRunning: 1,
				ChunksNumber:   1,
				Source: []v1alpha1.BlobSource{
					{
						URL:        sourceURL,
						BearerName: fmt.Sprintf("%s:%s", info.Host, strings.ReplaceAll(info.Image, "/", ":")),
					},
				},
				Destination: []v1alpha1.BlobDestination{
					{
						Name:         c.cidnDestination,
						Path:         cachePath,
						SkipIfExists: true,
					},
				},
				ContentSha256: cleanDigest(info.Manifests),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create blob error: %w", err)
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

	registration, err := c.cidnBlobInformer.Informer().AddEventHandler(handler)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.cidnBlobInformer.Informer().RemoveEventHandler(registration)
	}()

	// Wait for blob to complete or fail
	timeout := time.After(10 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for CIDN blob sync")

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

func (c *Manifests) getCachedManifest(info *PathInfo) (cacheValue, bool) {
	// When CIDN is configured, use the CIDN informer cache instead of our own cache
	if c.cidnClient != nil {
		chunkName := fmt.Sprintf("manifest:%s:%s:%s", info.Host, strings.ReplaceAll(info.Image, "/", ":"), info.Manifests)
		chunk, err := c.cidnChunkInformer.Lister().Get(chunkName)
		if err == nil && chunk.Status.Phase == v1alpha1.ChunkPhaseSucceeded {
			// Chunk exists in CIDN and is succeeded, return the cached response
			return cacheValue{
				Digest:    chunk.Status.SourceResponse.Headers["docker-content-digest"],
				MediaType: chunk.Status.SourceResponse.Headers["content-type"],
				Length:    chunk.Status.SourceResponse.Headers["content-length"],
				Body:      chunk.Status.ResponseBody,
			}, true
		}
		// Chunk not found in CIDN informer or not succeeded yet
		return cacheValue{}, false
	}

	// Fall back to local cache when CIDN is not configured
	return c.manifestCache.Get(info)
}

func (c *Manifests) tryFirstServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) (done bool) {
	val, ok := c.getCachedManifest(info)
	if ok {
		if val.Error != nil {
			utils.ServeError(rw, r, val.Error, val.StatusCode)
			return true
		}

		if val.MediaType == "" || val.Length == "" {
			if c.serveCachedManifest(rw, r, info, true, "hit and mark") {
				return true
			}
			if c.cidnClient == nil {
				c.manifestCache.Remove(info)
			}
			return false
		}

		if r.Method == http.MethodHead {
			rw.Header().Set("Docker-Content-Digest", val.Digest)
			rw.Header().Set("Content-Type", val.MediaType)
			rw.Header().Set("Content-Length", val.Length)
			return true
		}

		if len(val.Body) != 0 {
			rw.Header().Set("Docker-Content-Digest", val.Digest)
			rw.Header().Set("Content-Type", val.MediaType)
			rw.Header().Set("Content-Length", val.Length)
			rw.Write(val.Body)
			return true
		}

		if c.serveCachedManifest(rw, r, info, false, "hit") {
			return true
		}
		if c.cidnClient == nil {
			c.manifestCache.Remove(info)
		}
		return false
	}

	if info.IsDigestManifests {
		return c.serveCachedManifest(rw, r, info, true, "try")
	}

	if t.CacheFirst {
		return c.serveCachedManifest(rw, r, info, true, "try")
	}

	return false
}

func (c *Manifests) missServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) (done bool) {
	val, ok := c.getCachedManifest(info)
	if ok {
		if val.Error != nil {
			utils.ServeError(rw, r, val.Error, val.StatusCode)
			return true
		}

		if val.MediaType == "" || val.Length == "" {
			return c.serveCachedManifest(rw, r, info, true, "miss and mark")
		}

		if r.Method == http.MethodHead {
			rw.Header().Set("Docker-Content-Digest", val.Digest)
			rw.Header().Set("Content-Type", val.MediaType)
			rw.Header().Set("Content-Length", val.Length)
			return true
		}

		if len(val.Body) != 0 {
			rw.Header().Set("Docker-Content-Digest", val.Digest)
			rw.Header().Set("Content-Type", val.MediaType)
			rw.Header().Set("Content-Length", val.Length)
			rw.Write(val.Body)
			return true
		}

		return c.serveCachedManifest(rw, r, info, true, "miss")
	}

	return false
}

func (c *Manifests) serveCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo, recache bool, phase string) bool {
	ctx := r.Context()

	content, digest, mediaType, err := c.cache.GetManifestContent(ctx, info.Host, info.Image, info.Manifests)
	if err != nil {
		c.logger.Warn("manifest missed", "phase", phase, "host", info.Host, "image", info.Image, "manifest", info.Manifests, "error", err)
		return false
	}

	c.logger.Info("manifest hit", "phase", phase, "host", info.Host, "image", info.Image, "manifest", info.Manifests, "digest", digest)

	length := strconv.FormatInt(int64(len(content)), 10)

	if recache {
		c.manifestCache.Put(info, cacheValue{
			Digest:    digest,
			MediaType: mediaType,
			Length:    length,
		})
	}

	rw.Header().Set("Docker-Content-Digest", digest)
	rw.Header().Set("Content-Type", mediaType)
	rw.Header().Set("Content-Length", length)

	if r.Method != http.MethodHead {
		rw.Write(content)
	}

	return true
}
