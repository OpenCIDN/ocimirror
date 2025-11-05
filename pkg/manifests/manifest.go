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

	cidn cidn.CIDN
}

type Option func(c *Manifests)

func WithClient(client *http.Client) Option {
	return func(c *Manifests) {
		c.httpClient = client
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
		logger:     slog.Default(),
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func (c *Manifests) Serve(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	if c.serveCache(rw, r, info) {
		return
	}

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
			utils.ServeError(rw, r, err, sc)
			return
		}
	} else {
		// Synchronously cache the manifest
		sc, err := c.cacheManifest(info)
		if err != nil {
			c.logger.Warn("failed to cache manifest", "info", info, "error", err)
			utils.ServeError(rw, r, err, sc)
			return
		}
	}

	if c.serveCache(rw, r, info) {
		return
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

	_, _, _, err = c.cache.PutManifestContent(ctx, info.Host, info.Image, info.Manifests, body)
	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (c *Manifests) cacheManifestWithCIDN(info *PathInfo) (int, error) {
	ctx := context.Background()

	var (
		tag    string
		digest string
	)
	if !info.IsDigestManifests && info.Host != "ollama.com" {
		resp, err := c.cidn.ManifestTag(ctx, info.Host, info.Image, info.Manifests)
		if err != nil {
			return 0, fmt.Errorf("request with cidn error: %w", err)
		}

		digest = resp.Headers["docker-content-digest"]
		if digest == "" {
			return 0, errcode.ErrorCodeDenied
		}
		tag = info.Manifests
	} else {
		digest = info.Manifests
	}

	err := c.cache.RelinkManifest(ctx, info.Host, info.Image, tag, digest)
	if err == nil {
		c.logger.Info("relink manifest", "info", info)
		return 0, nil
	}

	err = c.cidn.ManifestDigest(ctx, info.Host, info.Image, digest)
	if err != nil {
		return 0, fmt.Errorf("cache blob with cidn error: %w", err)
	}
	err = c.cache.RelinkManifest(ctx, info.Host, info.Image, tag, digest)
	if err == nil {
		c.logger.Info("relink manifest", "info", info)
		return 0, nil
	}

	return 0, fmt.Errorf("failed to relink manifest after caching blob with CIDN")
}

func (c *Manifests) serveCache(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	content, digest, mediaType, err := c.cache.GetManifestContent(r.Context(), info.Host, info.Image, info.Manifests)
	if err != nil {
		c.logger.Warn("manifest missed", "host", info.Host, "image", info.Image, "manifest", info.Manifests, "error", err)
		return false
	}
	c.logger.Info("manifest hit", "host", info.Host, "image", info.Image, "manifest", info.Manifests, "digest", digest)

	length := strconv.FormatInt(int64(len(content)), 10)

	rw.Header().Set("Docker-Content-Digest", digest)
	rw.Header().Set("Content-Type", mediaType)
	rw.Header().Set("Content-Length", length)

	if r.Method != http.MethodHead {
		rw.Write(content)
	}
	return true
}
