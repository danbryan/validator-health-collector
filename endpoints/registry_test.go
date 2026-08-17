package endpoints_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danbryan/validator-health-collector/endpoints"
)

const sampleChainJSON = `{
  "chain_name": "cosmoshub",
  "chain_id": "cosmoshub-4",
  "apis": {
    "rpc": [
      {"address": "https://rpc.one.example", "provider": "One"},
      {"address": "http://rpc.insecure.example:26657", "provider": "Insecure"},
      {"address": "https://rpc.two.example/", "provider": "Two"},
      {"address": "   ", "provider": "Blank"}
    ],
    "rest": [
      {"address": "https://rest.one.example", "provider": "One"}
    ]
  }
}`

func TestFetchChainParsesAndFiltersEndpoints(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/cosmoshub/chain.json") {
			t.Errorf("registry request path = %q, want it to end in /cosmoshub/chain.json", r.URL.Path)
		}
		_, _ = w.Write([]byte(sampleChainJSON))
	}))
	defer srv.Close()

	apis, err := endpoints.FetchChain(srv.URL+"/%s/chain.json", "cosmoshub")
	if err != nil {
		t.Fatalf("FetchChain() returned an unexpected error: %v", err)
	}

	// Plain HTTP and blank addresses are dropped, so only the two https RPCs remain.
	if len(apis.RPC) != 2 {
		t.Fatalf("parsed %d RPC endpoints, want 2: %+v", len(apis.RPC), apis.RPC)
	}
	if apis.RPC[0].Address != "https://rpc.one.example" {
		t.Errorf("RPC[0] address = %q, want https://rpc.one.example", apis.RPC[0].Address)
	}
	// A trailing slash must be normalised away, since paths are appended directly.
	if apis.RPC[1].Address != "https://rpc.two.example" {
		t.Errorf("RPC[1] address = %q, want the trailing slash trimmed", apis.RPC[1].Address)
	}
	if len(apis.REST) != 1 {
		t.Fatalf("parsed %d REST endpoints, want 1", len(apis.REST))
	}
	if apis.REST[0].Provider != "One" {
		t.Errorf("REST[0] provider = %q, want One", apis.REST[0].Provider)
	}
}

func TestFetchChainRejectsNon200(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := endpoints.FetchChain(srv.URL+"/%s/chain.json", "nosuchchain"); err == nil {
		t.Error("FetchChain() returned no error for a 404 response")
	}
}

func TestFetchChainRejectsUndecodableBody(t *testing.T) {
	t.Parallel()

	// A proxy returning an HTML error page with a 200 is the exact failure that
	// silently corrupted a snapshot before rotation was added, so it must error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html><body>429 Too Many Requests</body></html>"))
	}))
	defer srv.Close()

	if _, err := endpoints.FetchChain(srv.URL+"/%s/chain.json", "cosmoshub"); err == nil {
		t.Error("FetchChain() accepted an HTML body as valid chain.json")
	}
}
