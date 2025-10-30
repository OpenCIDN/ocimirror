package gateway

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/OpenCIDN/ocimirror/pkg/blobs"
	"github.com/OpenCIDN/ocimirror/pkg/token"
)

func (c *Gateway) blob(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token, authData string) {
	if t.Attribute.BlobsAgentURL != "" && !t.Attribute.NoBlobsAgent {
		values := url.Values{
			"referer":       {r.RemoteAddr},
			"authorization": {authData},
		}

		blobURL := fmt.Sprintf("%s/v2/%s/%s/blobs/%s?%s", t.Attribute.BlobsAgentURL, info.Host, info.Image, info.Blobs, values.Encode())
		http.Redirect(rw, r, blobURL, http.StatusTemporaryRedirect)
		return
	}

	if c.blobs != nil {
		c.blobs.Serve(rw, r, &blobs.BlobInfo{
			Host:  info.Host,
			Image: info.Image,
			Blobs: info.Blobs,
		}, t)
		return
	}

	c.forward(rw, r, info, t)
}
