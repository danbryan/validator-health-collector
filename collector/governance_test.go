package collector

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueryProposalVotesPreservesWeightedOptionsAndPagination(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Query().Get("pagination.key") == "page2" {
			_, _ = w.Write([]byte(`{
  "votes": [{
    "proposal_id": "1052",
    "voter": "cosmos1second",
    "options": [{"option": "VOTE_OPTION_NO", "weight": "1.000000000000000000"}]
  }],
  "pagination": {"next_key": null}
}`))
			return
		}
		_, _ = w.Write([]byte(`{
  "votes": [{
    "proposal_id": "1052",
    "voter": "cosmos1weighted",
    "options": [
      {"option": "VOTE_OPTION_YES", "weight": "0.750000000000000000"},
      {"option": "VOTE_OPTION_NO_WITH_VETO", "weight": "0.250000000000000000"}
    ]
  }],
  "pagination": {"next_key": "page2"}
}`))
	}))
	defer server.Close()

	votes, err := NewRESTClient(server.URL).QueryProposalVotes("1052")
	if err != nil {
		t.Fatalf("QueryProposalVotes() returned an unexpected error: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2 paginated requests", requests.Load())
	}
	weighted := votes["cosmos1weighted"]
	if weighted.Option != "WEIGHTED" || weighted.Options["YES"] != 0.75 || weighted.Options["NO_WITH_VETO"] != 0.25 {
		t.Fatalf("weighted vote = %#v, want preserved 75/25 options", weighted)
	}
	if vote := votes["cosmos1second"]; vote.Option != "NO" || vote.Options["NO"] != 1 {
		t.Fatalf("second vote = %#v, want a full NO vote", vote)
	}
}

func TestQueryRelevantProposalsUsesTimeWindowAndAlwaysIncludesLive(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
  "proposals": [
    {"id":"live","status":"PROPOSAL_STATUS_VOTING_PERIOD","voting_end_time":"2026-09-01T00:00:00Z"},
    {"id":"recent","status":"PROPOSAL_STATUS_PASSED","voting_end_time":"2026-08-10T00:00:00Z"},
    {"id":"old","status":"PROPOSAL_STATUS_REJECTED","voting_end_time":"2026-07-01T00:00:00Z"},
    {"id":"deposit","status":"PROPOSAL_STATUS_DEPOSIT_PERIOD","voting_end_time":"2026-08-20T00:00:00Z"}
  ],
  "pagination": {"next_key": null}
}`))
	}))
	defer server.Close()

	cutoff := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	active, closed, err := NewRESTClient(server.URL).QueryRelevantProposals(cutoff)
	if err != nil {
		t.Fatalf("QueryRelevantProposals() returned an unexpected error: %v", err)
	}
	if len(active) != 1 || active[0].ID != "live" {
		t.Fatalf("active = %#v, want only the live proposal", active)
	}
	if len(closed) != 1 || closed[0].ID != "recent" {
		t.Fatalf("closed = %#v, want only the closed proposal inside the cutoff", closed)
	}
}

func TestAnalyzeProposalUsesLiveTallyAndCurrentVotes(t *testing.T) {
	t.Parallel()

	const (
		account = "cosmos1q6d3d089hg59x6gcx92uumx70s5y5wadntgvtr"
		valoper = "cosmosvaloper1q6d3d089hg59x6gcx92uumx70s5y5wadklue8s"
	)
	proposal := &Proposal{
		ID:              "1052",
		Status:          statusVotingPeriod,
		VotingStartTime: "2026-08-18T00:00:00Z",
		VotingEndTime:   "2026-09-01T00:00:00Z",
	}
	validators := []Validator{{
		OperatorAddress: valoper,
		Tokens:          "200",
		Description:     ValidatorDescription{Moniker: "Example"},
	}}
	liveTally := func(string) (float64, float64, float64, float64, error) {
		return 100, 20, 5, 1, nil
	}
	currentVotes := func(string) (map[string]GovVote, error) {
		return map[string]GovVote{
			account: {Option: "YES", Options: map[string]float64{"YES": 1}},
		}, nil
	}

	analysis, err := analyzeProposal(
		proposal, validators, map[string]string{valoper: "Grouped"}, 1000, 0.4, liveTally, currentVotes,
	)
	if err != nil {
		t.Fatalf("analyzeProposal() returned an unexpected error: %v", err)
	}
	if analysis.Yes != 100 || analysis.No != 20 || analysis.Abstain != 5 || analysis.Veto != 1 {
		t.Fatalf("tally = (%v, %v, %v, %v), want (100, 20, 5, 1)",
			analysis.Yes, analysis.No, analysis.Abstain, analysis.Veto)
	}
	if analysis.FinalTurnout != 0.126 || !analysis.AttributionComplete {
		t.Fatalf("turnout = %v, attribution complete = %v", analysis.FinalTurnout, analysis.AttributionComplete)
	}
	if len(analysis.Voted) != 1 || analysis.Voted[0].Entity != "Grouped" || analysis.Voted[0].VotingPower != 200 {
		t.Fatalf("voted = %#v, want the mapped validator vote", analysis.Voted)
	}
}

func TestAnalyzeProposalClosedUsesFinalTallyWithoutVoteQuery(t *testing.T) {
	t.Parallel()

	proposal := &Proposal{
		ID:              "1046",
		Status:          "PROPOSAL_STATUS_REJECTED",
		VotingStartTime: "2026-07-07T00:00:00Z",
		VotingEndTime:   "2026-07-21T00:00:00Z",
		FinalTallyResult: TallyResult{
			YesCount: "42",
			NoCount:  "7",
		},
	}
	var voteQueries atomic.Int32
	liveTally := func(string) (float64, float64, float64, float64, error) {
		return 999, 999, 999, 999, nil
	}
	currentVotes := func(string) (map[string]GovVote, error) {
		voteQueries.Add(1)
		return nil, nil
	}

	analysis, err := analyzeProposal(proposal, nil, nil, 1000, 0.4, liveTally, currentVotes)
	if err != nil {
		t.Fatalf("analyzeProposal() returned an unexpected error: %v", err)
	}
	if analysis.Yes != 42 || analysis.No != 7 || analysis.FinalTurnout != 0.049 {
		t.Fatalf("closed analysis = %#v, want the final tally", analysis)
	}
	if voteQueries.Load() != 0 || analysis.AttributionComplete {
		t.Fatalf("closed proposal queried current votes or claimed attribution: calls=%d complete=%v",
			voteQueries.Load(), analysis.AttributionComplete)
	}
}

func TestAnalyzeProposalKeepsAggregateWhenCurrentVotesFail(t *testing.T) {
	t.Parallel()

	proposal := &Proposal{
		ID:              "1052",
		Status:          statusVotingPeriod,
		VotingStartTime: "2026-08-18T00:00:00Z",
		VotingEndTime:   "2026-09-01T00:00:00Z",
	}
	liveTally := func(string) (float64, float64, float64, float64, error) {
		return 100, 0, 0, 0, nil
	}
	currentVotes := func(string) (map[string]GovVote, error) {
		return nil, http.ErrHandlerTimeout
	}

	analysis, err := analyzeProposal(proposal, nil, nil, 1000, 0.4, liveTally, currentVotes)
	if err != nil {
		t.Fatalf("analyzeProposal() suppressed valid aggregate state: %v", err)
	}
	if analysis.FinalTurnout != 0.1 || analysis.AttributionComplete {
		t.Fatalf("analysis = %#v, want valid turnout with incomplete attribution", analysis)
	}
}
