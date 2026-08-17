package endpoints

import (
	"fmt"
	"net/http"
	"time"
)

// NewHTTPClient builds the HTTP client every outbound request in this collector
// uses, with redirects refused.
//
// The endpoint list comes from the Cosmos chain registry, which means the
// collector sends requests to hosts operated by third parties. Go follows up to
// ten redirects by default, so a compromised or misconfigured provider could
// redirect a request at an address the collector would never have chosen, such as
// cluster-internal services or the instance metadata endpoint. Nothing here reads
// the response into anything privileged, so this is defence in depth rather than a
// live exploit, but there is no reason for a JSON API call to follow a redirect at
// all.
//
// A provider that genuinely needs a redirect simply fails its health probe and the
// resolver rotates to one of the other twenty or so healthy endpoints, so refusing
// them costs nothing in practice. This was checked against the live registry list
// before being adopted.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			return fmt.Errorf("refusing redirect to %s: chain endpoints must answer directly", req.URL.Redacted())
		},
	}
}
