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
	"strconv"
	"strings"
	"time"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	informers "github.com/OpenCIDN/cidn/pkg/informers/externalversions/task/v1alpha1"
	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/cache"
	"github.com/OpenCIDN/ocimirror/pkg/cidn"
	"github.com/OpenCIDN/ocimirror/pkg/token"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

type Manifests struct {
	httpClient *http.Client
	logger     *slog.Logger
	cache      *cache.Cache

	manifestCacheDuration time.Duration
	manifestCache         *manifestCache

	cidn cidn.CIDN
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
		c.cidn.Client = cidnClient
		c.cidn.ChunkInformer = chunkInformer
		c.cidn.BlobInformer = cidnBlobInformer
		c.cidn.Destination = destination
	}
}

func NewManifests(opts ...Option) (*Manifests, error) {
	c := &Manifests{
		logger:                slog.Default(),
		httpClient:            http.DefaultClient,
		manifestCacheDuration: time.Minute,
	}

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
		if c.cidn.Client != nil {
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
		if c.cidn.Client != nil {
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
		forwardReq.Header.Set("Accept", registry.OCIAcceptsValue)

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
	forwardReq.Header.Set("Accept", registry.OCIAcceptsValue)

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
		resp, err := c.cidn.ManifestTag(ctx, info.Host, info.Image, info.Manifests)
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

		err = c.cidn.ManifestDigest(ctx, info.Host, info.Image, digest)
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
		err := c.cidn.ManifestDigest(ctx, info.Host, info.Image, info.Manifests)
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

func (c *Manifests) tryFirstServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) (done bool) {
	val, ok := c.manifestCache.Get(info)
	if ok {
		if val.Error != nil {
			c.logger.Warn("cached manifest has error", "info", info, "error", val.Error, "statusCode", val.StatusCode)
			utils.ServeError(rw, r, val.Error, val.StatusCode)
			return true
		}

		if val.MediaType == "" || val.Length == "" {
			if c.serveCachedManifest(rw, r, info, true, "hit and mark") {
				return true
			}
			c.manifestCache.Remove(info)
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
		c.manifestCache.Remove(info)
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
	val, ok := c.manifestCache.Get(info)
	if ok {
		if val.Error != nil {
			c.logger.Warn("cached manifest has error", "info", info, "error", val.Error, "statusCode", val.StatusCode)
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
