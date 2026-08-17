package collector_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/cosmos/platform/apps/validator-health-collector/collector"
)

// txSearchBody renders a tx_search response with the given match count and no
// actual transactions, which is exactly what a pruning node returns: HTTP 200, no
// RPC error, and total_count 0.
func txSearchBody(total int) string {
	return fmt.Sprintf(`{"result":{"total_count":"%d","txs":[]}}`, total)
}

func TestFetchVotesRotatesPastPrunedEndpoint(t *testing.T) {
	t.Parallel()

	var prunedHits, indexedHits atomic.Int32

	// Answers successfully but has no index coverage for the proposal.
	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		prunedHits.Add(1)
		_, _ = w.Write([]byte(txSearchBody(0)))
	}))
	defer pruned.Close()

	// Reports matches. No txs are returned, so no votes are parsed, but the
	// non-zero total is what proves the index reaches the proposal.
	indexed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		indexedHits.Add(1)
		_, _ = w.Write([]byte(txSearchBody(221)))
	}))
	defer indexed.Close()

	backfiller := collector.NewGovBackfiller(pruned.URL, indexed.URL)

	_, fetch, err := backfiller.FetchVotes("1046", true)
	if err != nil {
		t.Fatalf("FetchVotes() returned an unexpected error: %v", err)
	}
	if !fetch.Complete {
		t.Error("FetchVotes() reported incomplete attribution despite an indexed endpoint being available")
	}
	if fetch.Endpoint != indexed.URL {
		t.Errorf("served by %q, want the indexed endpoint %q", fetch.Endpoint, indexed.URL)
	}
	if fetch.TotalCount != 221 {
		t.Errorf("TotalCount = %d, want 221", fetch.TotalCount)
	}
	if prunedHits.Load() == 0 {
		t.Error("the pruned endpoint was never tried, so rotation was not exercised")
	}
}

func TestFetchVotesAcceptsZeroWhenNoVotesAreExpected(t *testing.T) {
	t.Parallel()

	// A proposal with a zero tally genuinely has no votes, so total_count 0 is the
	// right answer and must not trigger rotation or an incomplete verdict.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(txSearchBody(0)))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL, srv.URL)

	_, fetch, err := backfiller.FetchVotes("999", false)
	if err != nil {
		t.Fatalf("FetchVotes() returned an unexpected error: %v", err)
	}
	if !fetch.Complete {
		t.Error("FetchVotes() flagged a genuinely empty proposal as incomplete")
	}
	if hits.Load() != 1 {
		t.Errorf("made %d requests, want 1 with no rotation", hits.Load())
	}
}

func TestFetchVotesReportsIncompleteWhenEveryEndpointIsPruned(t *testing.T) {
	t.Parallel()

	// This is the case that must never be published as real non-participation.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(txSearchBody(0)))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL, srv.URL, srv.URL)

	votes, fetch, err := backfiller.FetchVotes("1004", true)
	if err != nil {
		t.Fatalf("FetchVotes() should degrade rather than error: %v", err)
	}
	if fetch.Complete {
		t.Error("FetchVotes() claimed complete attribution when no endpoint had the index")
	}
	if len(votes) != 0 {
		t.Errorf("returned %d votes, want none", len(votes))
	}
}

func TestFetchVotesErrorsWhenNoEndpointAnswers(t *testing.T) {
	t.Parallel()

	// A transport-level failure everywhere is different from a pruned index: there
	// is no information at all, so it must surface as an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL, srv.URL)

	if _, _, err := backfiller.FetchVotes("1046", true); err == nil {
		t.Error("FetchVotes() succeeded even though every endpoint returned 502")
	}
}

func TestFetchVotesWithNoEndpointsErrors(t *testing.T) {
	t.Parallel()

	backfiller := collector.NewGovBackfiller()
	if _, _, err := backfiller.FetchVotes("1", true); err == nil {
		t.Error("FetchVotes() succeeded with no endpoints configured")
	}
}

func TestFetchVotesSurfacesRPCError(t *testing.T) {
	t.Parallel()

	// Some nodes do return an explicit error when indexing is disabled. That path
	// should rotate too, not be mistaken for a valid empty result.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":{"message":"internal error","data":"transaction indexing is disabled"}}`))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL, srv.URL)

	if _, _, err := backfiller.FetchVotes("1046", true); err == nil {
		t.Error("FetchVotes() ignored an explicit tx indexing disabled error")
	}
}

func TestAnalyzeProposalUsesLiveTallyForOpenProposal(t *testing.T) {
	t.Parallel()

	// A proposal in its voting period reports a zeroed final_tally_result, so the
	// live tally has to win or the dashboard shows 0 beside a real turnout.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(txSearchBody(0)))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL)

	prop := &collector.Proposal{
		ID:              "1051",
		Status:          "PROPOSAL_STATUS_VOTING_PERIOD",
		VotingStartTime: "2026-08-04T00:00:00Z",
		VotingEndTime:   "2026-08-18T00:00:00Z",
	}
	live := func(string) (float64, float64, float64, float64, error) {
		return 100, 20, 5, 1, nil
	}

	analysis, err := backfiller.AnalyzeProposal(prop, nil, nil, 1000, 0.4, 6.0, live)
	if err != nil {
		t.Fatalf("AnalyzeProposal() returned an unexpected error: %v", err)
	}
	if analysis.Yes != 100 || analysis.No != 20 || analysis.Abstain != 5 || analysis.Veto != 1 {
		t.Errorf("tally = (%v, %v, %v, %v), want the live tally (100, 20, 5, 1)",
			analysis.Yes, analysis.No, analysis.Abstain, analysis.Veto)
	}
	// (100+20+5+1)/1000
	if analysis.FinalTurnout != 0.126 {
		t.Errorf("FinalTurnout = %v, want 0.126", analysis.FinalTurnout)
	}
}

func TestAnalyzeProposalUsesFinalTallyForClosedProposal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(txSearchBody(1)))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL)

	prop := &collector.Proposal{
		ID:              "1046",
		Status:          "PROPOSAL_STATUS_REJECTED",
		VotingStartTime: "2026-07-07T00:00:00Z",
		VotingEndTime:   "2026-07-21T00:00:00Z",
	}
	prop.FinalTallyResult.YesCount = "42"
	prop.FinalTallyResult.NoCount = "7"
	prop.FinalTallyResult.AbstainCount = "0"
	prop.FinalTallyResult.NoWithVetoCount = "0"

	// A closed proposal must ignore the live tally even when one is supplied.
	live := func(string) (float64, float64, float64, float64, error) {
		return 999, 999, 999, 999, nil
	}

	analysis, err := backfiller.AnalyzeProposal(prop, nil, nil, 1000, 0.4, 6.0, live)
	if err != nil {
		t.Fatalf("AnalyzeProposal() returned an unexpected error: %v", err)
	}
	if analysis.Yes != 42 || analysis.No != 7 {
		t.Errorf("tally = (%v, %v), want the final tally (42, 7)", analysis.Yes, analysis.No)
	}
}

func TestAnalyzeProposalFlagsIncompleteAttribution(t *testing.T) {
	t.Parallel()

	// A non-zero tally with no retrievable vote transactions must be surfaced, not
	// published as zero participation.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(txSearchBody(0)))
	}))
	defer srv.Close()

	backfiller := collector.NewGovBackfiller(srv.URL, srv.URL)

	prop := &collector.Proposal{
		ID:              "1004",
		Status:          "PROPOSAL_STATUS_PASSED",
		VotingStartTime: "2026-01-01T00:00:00Z",
		VotingEndTime:   "2026-01-15T00:00:00Z",
	}
	prop.FinalTallyResult.YesCount = "500"

	analysis, err := backfiller.AnalyzeProposal(prop, nil, nil, 1000, 0.4, 6.0, nil)
	if err != nil {
		t.Fatalf("AnalyzeProposal() should degrade rather than error: %v", err)
	}
	if analysis.AttributionComplete {
		t.Error("AttributionComplete = true, want false when no endpoint had the index")
	}
	// The tally still comes from the chain, so it stays valid.
	if analysis.Yes != 500 {
		t.Errorf("Yes = %v, want the tally to remain valid at 500", analysis.Yes)
	}
}
