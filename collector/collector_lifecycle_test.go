package collector //nolint:testpackage // The regression exercises private metric-retirement boundaries.

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/danbryan/validator-health-collector/endpoints"
)

func analysisTestCollector(restURL string) *Collector {
	return &Collector{
		restClient:            NewRESTClient(restURL),
		metrics:               NewMetrics(),
		proposalHistoryWindow: DefaultProposalHistoryWindow,
	}
}

func liveProposalForTest() Proposal {
	return Proposal{
		ID:              "1051",
		Status:          statusVotingPeriod,
		VotingStartTime: "2026-08-04T00:00:00Z",
		VotingEndTime:   "2026-08-18T00:00:00Z",
	}
}

func TestCollectSnapshotRetiresLiveGateBeforeEndpointFailure(t *testing.T) {
	t.Parallel()

	// The registry is reachable, but its only advertised REST and RPC endpoint
	// fails TLS verification. Resolution therefore returns no healthy endpoints
	// and CollectSnapshot exits before making any chain query.
	unhealthy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer unhealthy.Close()

	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"chain_name":"cosmoshub","chain_id":"cosmoshub-4","apis":{"rest":[{"address":%q,"provider":"test"}],"rpc":[{"address":%q,"provider":"test"}]}}`, unhealthy.URL, unhealthy.URL)
	}))
	defer registry.Close()

	c := New(endpoints.NewResolver(endpoints.Options{
		RegistryURL: registry.URL + "/%s.json",
		Chain:       "cosmoshub",
	}), nil)
	id := "1051"
	c.metrics.GovProposalLive.WithLabelValues(id).Set(1)
	c.metrics.GovTurnout.WithLabelValues(id).Set(0.2)
	c.metrics.GovSecondsRemaining.WithLabelValues(id).Set(3600)

	if err := c.CollectSnapshot(); err == nil {
		t.Fatal("CollectSnapshot() succeeded despite having no healthy endpoints")
	}
	if got := testutil.ToFloat64(c.metrics.GovProposalLive.WithLabelValues(id)); got != 0 {
		t.Errorf("proposal_live = %v, want the previous cycle's gate retired before endpoint resolution", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovTurnout.WithLabelValues(id)); got != 0 {
		t.Errorf("turnout = %v, want the previous cycle's live series retired", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovSecondsRemaining.WithLabelValues(id)); got != 0 {
		t.Errorf("seconds remaining = %v, want the previous cycle's live series retired", got)
	}
}

func TestAnalyzeProposalsRetiresStaleMetricsWhenLiveTallyFails(t *testing.T) {
	t.Parallel()

	failedREST := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer failedREST.Close()

	c := analysisTestCollector(failedREST.URL)
	id := "1051"

	// Simulate values left by the previous successful cycle. In particular, the
	// negative quorum buffer would fire QuorumNotMet if proposal_live survived.
	c.metrics.GovProposalLive.WithLabelValues(id).Set(1)
	c.metrics.GovTurnout.WithLabelValues(id).Set(0.2)
	c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(-0.2)
	c.metrics.GovEntityVote.WithLabelValues(id, "stale-entity", "YES").Set(0.1)

	err := c.analyzeProposals([]Proposal{liveProposalForTest()}, nil, nil, 1000, 0.4, 0.334)
	if err == nil {
		t.Fatal("analyzeProposals() succeeded even though the live tally endpoint failed")
	}
	if got := testutil.ToFloat64(c.metrics.GovProposalLive.WithLabelValues(id)); got != 0 {
		t.Errorf("proposal_live = %v, want the failed proposal gate retired", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovTurnout.WithLabelValues(id)); got != 0 {
		t.Errorf("turnout = %v, want the failed proposal's analysis-derived value retired", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovQuorumBuffer.WithLabelValues(id)); got != 0 {
		t.Errorf("quorum buffer = %v, want the stale analysis retired", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovEntityVote.WithLabelValues(id, "stale-entity", "YES")); got != 0 {
		t.Errorf("stale entity vote = %v, want the old analysis retired", got)
	}
}

func TestAnalyzeProposalsPublishesLiveGateAfterAggregateRefresh(t *testing.T) {
	t.Parallel()

	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cosmos/gov/v1/proposals/1051/tally":
			_, _ = w.Write([]byte(`{"tally":{"yes_count":"250","no_count":"0","abstain_count":"0","no_with_veto_count":"0"}}`))
		case "/cosmos/gov/v1/proposals/1051/votes":
			_, _ = w.Write([]byte(`{"votes":[],"pagination":{"next_key":null}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer rest.Close()

	c := analysisTestCollector(rest.URL)
	id := "1051"
	c.metrics.GovTurnout.WithLabelValues(id).Set(0.9)
	c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(-0.9)

	if err := c.analyzeProposals([]Proposal{liveProposalForTest()}, nil, nil, 1000, 0.4, 0.334); err != nil {
		t.Fatalf("analyzeProposals() returned an unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(c.metrics.GovProposalLive.WithLabelValues(id)); got != 1 {
		t.Errorf("proposal_live = %v, want 1 after a complete aggregate refresh", got)
	}
	if got := testutil.ToFloat64(c.metrics.GovTurnout.WithLabelValues(id)); got != 0.25 {
		t.Errorf("turnout = %v, want the freshly analyzed value 0.25", got)
	}
	wantBuffer := 0.25 - 0.4
	if got := testutil.ToFloat64(c.metrics.GovQuorumBuffer.WithLabelValues(id)); math.Abs(got-wantBuffer) > 1e-12 {
		t.Errorf("quorum buffer = %v, want the freshly analyzed value %v", got, wantBuffer)
	}
	if got := testutil.ToFloat64(c.metrics.GovAttributionComplete.WithLabelValues(id)); got != 0 {
		t.Errorf("attribution_complete = %v, want 0 when a non-zero tally has no current vote rows", got)
	}
}
