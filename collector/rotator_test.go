package collector_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/danbryan/validator-health-collector/collector"
)

// Rotation is exercised through the exported clients rather than the unexported
// rotator, so these tests pin the behaviour callers actually depend on.

const poolPath = "/cosmos/staking/v1beta1/pool"

func poolServer(t *testing.T, hits *atomic.Int32, body string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != poolPath {
			t.Errorf("unexpected request path %q", r.URL.Path)
		}
		hits.Add(1)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRESTClientRotatesPastFailingEndpoint(t *testing.T) {
	t.Parallel()

	var badHits, goodHits atomic.Int32

	// A 200 with an HTML body is the silent-failure case public Cosmos endpoints
	// produce under rate limiting, and it must rotate rather than fail the query.
	bad := poolServer(t, &badHits, "<html><body>429 Too Many Requests</body></html>", http.StatusOK)
	good := poolServer(t, &goodHits, `{"pool":{"bonded_tokens":"12345","not_bonded_tokens":"1"}}`, http.StatusOK)

	client := collector.NewRESTClient(bad.URL, good.URL)

	bonded, err := client.QueryStakingPool()
	if err != nil {
		t.Fatalf("QueryStakingPool() failed even though a healthy endpoint was available: %v", err)
	}
	if bonded != 12345 {
		t.Errorf("bonded tokens = %v, want 12345", bonded)
	}
	if badHits.Load() != 1 || goodHits.Load() != 1 {
		t.Errorf("hit counts bad=%d good=%d, want 1 each", badHits.Load(), goodHits.Load())
	}
	// Selection must stick so one cycle reads a consistent view of the chain.
	if client.BaseURL() != good.URL {
		t.Errorf("BaseURL() = %q, want the healthy endpoint %q to stay selected",
			client.BaseURL(), good.URL)
	}
}

func TestRESTClientFailsWhenEveryEndpointIsBad(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	bad := poolServer(t, &hits, "", http.StatusInternalServerError)

	client := collector.NewRESTClient(bad.URL, bad.URL)
	if _, err := client.QueryStakingPool(); err == nil {
		t.Error("QueryStakingPool() succeeded even though every endpoint returned 500")
	}
}

func TestRESTClientWithNoEndpoints(t *testing.T) {
	t.Parallel()

	client := collector.NewRESTClient()
	if got := client.BaseURL(); got != "" {
		t.Errorf("BaseURL() = %q, want empty", got)
	}
	if _, err := client.QueryStakingPool(); err == nil {
		t.Error("QueryStakingPool() succeeded with no endpoints configured")
	}
}

func TestSetEndpointsNormalisesInput(t *testing.T) {
	t.Parallel()

	client := collector.NewRESTClient()
	client.SetEndpoints([]string{"  https://a/  ", "", "https://b", "   "})

	if got := client.BaseURL(); got != "https://a" {
		t.Errorf("BaseURL() = %q, want https://a with whitespace and trailing slash removed", got)
	}
}

func TestSetEndpointsKeepsCurrentSelectionWhenStillPresent(t *testing.T) {
	t.Parallel()

	// A mid-cycle re-resolve must not move a working endpoint just because the
	// ranking shifted.
	var badHits, goodHits atomic.Int32
	bad := poolServer(t, &badHits, "nonsense", http.StatusOK)
	good := poolServer(t, &goodHits, `{"pool":{"bonded_tokens":"1","not_bonded_tokens":"0"}}`, http.StatusOK)

	client := collector.NewRESTClient(bad.URL, good.URL)
	if _, err := client.QueryStakingPool(); err != nil {
		t.Fatalf("setup query failed: %v", err)
	}
	selected := client.BaseURL()

	client.SetEndpoints([]string{"https://newcomer", bad.URL, good.URL})

	if client.BaseURL() != selected {
		t.Errorf("BaseURL() = %q, want the previously selected %q to be retained",
			client.BaseURL(), selected)
	}
}

func TestSetEndpointsResetsWhenSelectionDisappears(t *testing.T) {
	t.Parallel()

	client := collector.NewRESTClient("https://a", "https://b")
	client.SetEndpoints([]string{"https://c", "https://d"})

	if got := client.BaseURL(); got != "https://c" {
		t.Errorf("BaseURL() = %q, want the first entry when the old selection is gone", got)
	}
}

func TestRPCClientRotatesOnFailure(t *testing.T) {
	t.Parallel()

	var badHits, goodHits atomic.Int32

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer bad.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		goodHits.Add(1)
		_, _ = w.Write([]byte(`{"result":{"node_info":{"network":"cosmoshub-4"},` +
			`"sync_info":{"latest_block_height":"100","latest_block_time":"2026-08-17T00:00:00Z"}}}`))
	}))
	defer good.Close()

	client := collector.NewRPCClient(bad.URL, good.URL)

	status, err := client.QueryCometStatus()
	if err != nil {
		t.Fatalf("QueryCometStatus() failed despite a healthy endpoint: %v", err)
	}
	if status.ChainID != "cosmoshub-4" {
		t.Errorf("ChainID = %q, want cosmoshub-4", status.ChainID)
	}
	if status.Height != 100 {
		t.Errorf("Height = %d, want 100", status.Height)
	}
	if badHits.Load() == 0 {
		t.Error("the failing endpoint was never tried, so rotation was not exercised")
	}
}

func TestQueryUpgradePlanReportsNoPlanAsSentinel(t *testing.T) {
	t.Parallel()

	// No scheduled upgrade is the normal state, so it must be distinguishable from
	// a query failure rather than returning a bare nil, nil.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"plan":null}`))
	}))
	defer srv.Close()

	client := collector.NewRESTClient(srv.URL)
	plan, err := client.QueryUpgradePlan()
	if plan != nil {
		t.Errorf("plan = %+v, want nil", plan)
	}
	if !errors.Is(err, collector.ErrNoUpgradePlan) {
		t.Errorf("err = %v, want ErrNoUpgradePlan", err)
	}
}
