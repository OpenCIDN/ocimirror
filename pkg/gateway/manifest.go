package gateway

import (
	"net/http"

	"github.com/OpenCIDN/ocimirror/pkg/manifests"
	"github.com/OpenCIDN/ocimirror/pkg/token"
)

func (c *Gateway) manifest(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	if c.manifests != nil {
		c.manifests.Serve(rw, r, &manifests.PathInfo{
			Host:              info.Host,
			Image:             info.Image,
			Manifests:         info.Manifests,
			IsDigestManifests: info.IsDigestManifests,
		}, t)
		return
	}

	c.forward(rw, r, info, t)
}
