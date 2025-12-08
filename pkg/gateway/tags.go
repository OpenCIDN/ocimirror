package gateway

import (
	"net/http"

	"github.com/OpenCIDN/ocimirror/pkg/tags"
	"github.com/OpenCIDN/ocimirror/pkg/token"
)

func (c *Gateway) tag(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	if c.tags != nil {
		c.tags.Serve(rw, r, &tags.ImageInfo{
			Host:  info.Host,
			Image: info.Image,
		})
		return
	}

	c.forward(rw, r, info, t)
}
