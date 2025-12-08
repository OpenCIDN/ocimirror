package tags

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/OpenCIDN/ocimirror/pkg/cache"
)

type Tags struct {
	logger *slog.Logger
	cache  *cache.Cache
}

type Option func(c *Tags)

func WithCache(cache *cache.Cache) Option {
	return func(c *Tags) {
		c.cache = cache
	}
}

func NewTags(opts ...Option) (*Tags, error) {
	c := &Tags{
		logger: slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.cache == nil {
		return nil, errors.New("cache is required")
	}

	return c, nil
}

type ImageInfo struct {
	Host  string
	Image string
}

type tagsList struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func (c *Tags) Serve(rw http.ResponseWriter, r *http.Request, info *ImageInfo) {
	tags, err := c.cache.ListTags(r.Context(), info.Host, info.Image)
	if err != nil {
		http.Error(rw, fmt.Sprintf("list tags failed: %v", err), http.StatusInternalServerError)
		return
	}

	tagsList := tagsList{
		Name: info.Host + "/" + info.Image,
		Tags: tags,
	}

	jsonData, _ := json.Marshal(tagsList)
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Content-Length", fmt.Sprint(len(jsonData)))
	rw.Write(jsonData)
}
