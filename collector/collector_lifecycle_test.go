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

func analysisTestCollector(rpcURL string) *Collector {
	return &Collector{
		restClient:              NewRESTClient(),
		metrics:                 NewMetrics(),
		govBackfiller:           NewGovBackfiller(rpcURL),
		historicalProposalCount: 6,
		trackedProposals:        make(map[string]bool),
		observedTurnouts:        make(map[string]float64),
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

func TestAnalyzeProposalsRetiresStaleLiveMetricsWhenAnalysisFails(t *testing.T) {
	t.Parallel()

	failedRPC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer failedRPC.Close()

	c := analysisTestCollector(failedRPC.URL)
	id := "1051"

	// Simulate values left by the previous successful cycle. In particular, the
	// negative quorum buffer would fire QuorumNotMet if proposal_live survived.
	c.metrics.GovProposalLive.WithLabelValues(id).Set(1)
	c.metrics.GovTurnout.WithLabelValues(id).Set(0.2)
	c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(-0.2)
	c.metrics.GovEntityVote.WithLabelValues(id, "stale-entity", "YES").Set(0.1)

	err := c.analyzeProposals([]Proposal{liveProposalForTest()}, nil, 1000, 0.4)
	if err == nil {
		t.Fatal("analyzeProposals() succeeded even though the live analysis endpoint failed")
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

func TestAnalyzeProposalsPublishesLiveGateOnlyAfterSuccessfulAnalysis(t *testing.T) {
	t.Parallel()

	indexedRPC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":{"total_count":"0","txs":[]}}`))
	}))
	defer indexedRPC.Close()

	c := analysisTestCollector(indexedRPC.URL)
	id := "1051"
	prop := liveProposalForTest()
	prop.FinalTallyResult.YesCount = "250"
	c.metrics.GovTurnout.WithLabelValues(id).Set(0.9)
	c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(-0.9)

	if err := c.analyzeProposals([]Proposal{prop}, nil, 1000, 0.4); err != nil {
		t.Fatalf("analyzeProposals() returned an unexpected error: %v", err)
	}
	if got := testutil.ToFloat64(c.metrics.GovProposalLive.WithLabelValues(id)); got != 1 {
		t.Errorf("proposal_live = %v, want 1 after a complete refresh", got)
	}
	if c.trackedProposals[id] {
		t.Error("live proposal was marked immutable before its final closed-state refresh")
	}
	if got := testutil.ToFloat64(c.metrics.GovTurnout.WithLabelValues(id)); got != 0.25 {
		t.Errorf("turnout = %v, want the freshly analyzed value 0.25", got)
	}
	wantBuffer := 0.25 - 0.4
	if got := testutil.ToFloat64(c.metrics.GovQuorumBuffer.WithLabelValues(id)); math.Abs(got-wantBuffer) > 1e-12 {
		t.Errorf("quorum buffer = %v, want the freshly analyzed value %v", got, wantBuffer)
	}
}
