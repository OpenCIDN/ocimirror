package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"

	"github.com/OpenCIDN/ocimirror/internal/registry"
	"github.com/OpenCIDN/ocimirror/internal/throttled"
	"github.com/OpenCIDN/ocimirror/internal/utils"
	"github.com/OpenCIDN/ocimirror/pkg/blobs"
	"github.com/OpenCIDN/ocimirror/pkg/manifests"
	"github.com/OpenCIDN/ocimirror/pkg/token"
	"github.com/docker/distribution/registry/api/errcode"
	"golang.org/x/time/rate"
)

var (
	prefix  = "/v2/"
	catalog = prefix + "_catalog"
)

type ImageInfo struct {
	Host string
	Name string
}

type Gateway struct {
	httpClient      *http.Client
	modify          func(info *ImageInfo) *ImageInfo
	logger          *slog.Logger
	disableTagsList bool

	authenticator *token.Authenticator

	defaultRegistry         string
	overrideDefaultRegistry map[string]string

	blobs     *blobs.Blobs
	manifests *manifests.Manifests
}

type Option func(c *Gateway)

func WithClient(client *http.Client) Option {
	return func(c *Gateway) {
		c.httpClient = client
	}
}

func WithDisableTagsList(b bool) Option {
	return func(c *Gateway) {
		c.disableTagsList = b
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *Gateway) {
		c.logger = logger
	}
}

func WithPathInfoModifyFunc(modify func(info *ImageInfo) *ImageInfo) Option {
	return func(c *Gateway) {
		c.modify = modify
	}
}

func WithAuthenticator(authenticator *token.Authenticator) Option {
	return func(c *Gateway) {
		c.authenticator = authenticator
	}
}

func WithDefaultRegistry(target string) Option {
	return func(c *Gateway) {
		c.defaultRegistry = target
	}
}

func WithOverrideDefaultRegistry(overrideDefaultRegistry map[string]string) Option {
	return func(c *Gateway) {
		c.overrideDefaultRegistry = overrideDefaultRegistry
	}
}

func WithBlobs(a *blobs.Blobs) Option {
	return func(c *Gateway) {
		c.blobs = a
	}
}

func WithManifests(a *manifests.Manifests) Option {
	return func(c *Gateway) {
		c.manifests = a
	}
}

func NewGateway(opts ...Option) (*Gateway, error) {
	c := &Gateway{
		logger:     slog.Default(),
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func (c *Gateway) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	oriPath := r.URL.Path
	if !strings.HasPrefix(oriPath, prefix) {
		http.NotFound(rw, r)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		utils.ServeError(rw, r, errcode.ErrorCodeUnsupported, 0)
		return
	}

	if oriPath == catalog {
		utils.ServeError(rw, r, errcode.ErrorCodeUnsupported, 0)
		return
	}

	r.RemoteAddr = utils.GetIP(r.RemoteAddr)

	var t token.Token
	var err error

	authData := r.Header.Get("Authorization")

	if c.authenticator != nil {
		t, err = c.authenticator.Authorization(r)
		if err != nil {
			c.authenticator.Authenticate(rw, r)
			return
		}
	}

	if oriPath == prefix {
		utils.ResponseAPIBase(rw, r)
		return
	}

	info, ok := parseOriginPathInfo(oriPath)
	if !ok {
		utils.ServeError(rw, r, errcode.ErrorCodeUnsupported, 0)
		return
	}

	if c.authenticator != nil {
		if t.Scope == "" {
			c.authenticator.Authenticate(rw, r)
			return
		}
		if t.Block {
			if t.BlockMessage != "" {
				utils.ServeError(rw, r, errcode.ErrorCodeDenied.WithMessage(t.BlockMessage), 0)
			} else {
				utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
			}
			return
		}
	}

	if t.Attribute.Host != "" {
		info.Host = t.Attribute.Host
	}

	if info.Host == "" {
		info.Host = c.defaultRegistry
		if c.overrideDefaultRegistry != nil {
			r, ok := c.overrideDefaultRegistry[r.Host]
			if ok {
				info.Host = r
			}
		}
	}

	if info.Host == "" {
		utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
		return
	}

	if r.URL.RawQuery != "" {
		q := r.URL.Query()
		if ns := q.Get("ns"); ns != "" && ns != info.Host {
			utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
			return
		}
	}

	if t.Attribute.Image != "" {
		info.Image = t.Attribute.Image
	}

	if c.modify != nil {
		n := c.modify(&ImageInfo{
			Host: info.Host,
			Name: info.Image,
		})
		info.Host = n.Host
		info.Image = n.Name
	}

	if c.disableTagsList && info.TagsList && !t.AllowTagsList {
		utils.ResponseEmptyTagsList(rw, r)
		return
	}

	if info.Blobs != "" {
		c.blob(rw, r, info, &t, authData)
		return
	}

	if info.Manifests != "" {
		c.manifest(rw, r, info, &t)
		return
	}
	c.forward(rw, r, info, &t)
}

func (c *Gateway) forward(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	path, err := info.Path()
	if err != nil {
		c.logger.Warn("failed to get path", "error", err)
		utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
		return
	}
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   path,
	}
	forwardReq, err := http.NewRequestWithContext(r.Context(), r.Method, u.String(), nil)
	if err != nil {
		c.logger.Warn("failed to new request", "error", err)
		utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
		return
	}

	if info.Manifests != "" {
		forwardReq.Header.Set("Accept", registry.OCIAcceptsValue)
	}

	resp, err := c.httpClient.Do(forwardReq)
	if err != nil {
		c.logger.Warn("failed to request", "host", info.Host, "image", info.Image, "error", err)
		utils.ServeError(rw, r, errcode.ErrorCodeUnknown, 0)
		return
	}
	defer func() {
		resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		c.logger.Warn("origin direct response 40x", "host", info.Host, "image", info.Image, "response", dumpResponse(resp))
		utils.ServeError(rw, r, errcode.ErrorCodeDenied, 0)
		return
	}

	resp.Header.Del("Docker-Ratelimit-Source")

	if resp.StatusCode == http.StatusOK {
		oldLink := resp.Header.Get("Link")
		if oldLink != "" {
			resp.Header.Set("Link", addPrefixToImageForPagination(oldLink, info.Host))
		}
	}

	header := rw.Header()
	for k, v := range resp.Header {
		key := textproto.CanonicalMIMEHeaderKey(k)
		header[key] = v
	}
	rw.WriteHeader(resp.StatusCode)

	if forwardReq.Method != http.MethodHead {
		var body io.Reader = resp.Body

		if t.RateLimitPerSecond > 0 {
			limit := rate.NewLimiter(rate.Limit(t.RateLimitPerSecond), 1024*1024)
			body = throttled.NewThrottledReader(r.Context(), body, limit)
		}

		io.Copy(rw, body)
	}
}
