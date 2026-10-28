package collector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// undecodableBody is what a v29 node returns for a proposal holding a legacy
// x/params content type it no longer registers.
const undecodableBody = `{"code":13,"message":"no concrete type registered for type URL /cosmos.params.v1beta1.ParameterChangeProposal against interface *v1beta1.Content","details":[]}`

var scanNow = time.Date(2026, 10, 28, 16, 0, 0, 0, time.UTC)

// fakeProposal builds a proposal JSON object submitted daysAgo before scanNow
// with a seven-day vote.
func fakeProposal(id int, status string, daysAgo int) string {
	submit := scanNow.AddDate(0, 0, -daysAgo)
	end := submit.AddDate(0, 0, 7)
	return fmt.Sprintf(`{"id":"%d","status":"%s","submit_time":"%s","voting_start_time":"%s","voting_end_time":"%s"}`,
		id, status, submit.Format(time.RFC3339), submit.Format(time.RFC3339), end.Format(time.RFC3339))
}

// govServer serves list pages and single proposals from fixed tables, and
// records every path it was asked for.
type govServer struct {
	t      *testing.T
	list   func(r *http.Request) (int, string)
	byID   map[string]string // id -> body; missing ids answer gRPC NotFound
	status map[string]int    // id -> non-200 status for the body in byID
	mu     sync.Mutex
	asked  []string
}

func (g *govServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.asked = append(g.asked, r.URL.Path+"?"+r.URL.RawQuery)
	g.mu.Unlock()

	if r.URL.Path == "/cosmos/gov/v1/proposals" {
		if r.URL.Query().Get("pagination.reverse") != "true" {
			g.t.Errorf("proposal list requested oldest first: %s", r.URL.RawQuery)
		}
		code, body := g.list(r)
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/cosmos/gov/v1/proposals/")
	body, ok := g.byID[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `{"code":5,"message":"proposal %s doesn't exist","details":[]}`, id)
		return
	}
	if code, bad := g.status[id]; bad {
		w.WriteHeader(code)
	}
	_, _ = w.Write([]byte(body))
}

func (g *govServer) requested(prefix string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, a := range g.asked {
		if strings.HasPrefix(a, prefix) {
			n++
		}
	}
	return n
}

func ids(props []Proposal) []string {
	out := make([]string, 0, len(props))
	for _, p := range props {
		out = append(out, p.ID)
	}
	return out
}

func TestQueryRelevantProposalsReadsNewestFirstAndStopsAtFloor(t *testing.T) {
	t.Parallel()

	g := &govServer{t: t, list: func(r *http.Request) (int, string) {
		switch r.URL.Query().Get("pagination.key") {
		case "":
			return 200, `{"proposals":[` +
				fakeProposal(1060, statusVotingPeriod, 3) + `,` +
				fakeProposal(1059, "PROPOSAL_STATUS_PASSED", 12) + `,` +
				fakeProposal(1058, statusDepositPeriod, 13) +
				`],"pagination":{"next_key":"page2"}}`
		case "page2":
			return 200, `{"proposals":[` +
				fakeProposal(1057, "PROPOSAL_STATUS_REJECTED", 30) + `,` +
				fakeProposal(1056, "PROPOSAL_STATUS_PASSED", 200) +
				`],"pagination":{"next_key":"page3"}}`
		default:
			// Old pages hold proposals the chain can no longer decode. The scan
			// must stop before it gets here.
			return 500, undecodableBody
		}
	}}
	server := httptest.NewServer(g)
	defer server.Close()

	cutoff := scanNow.Add(-14 * 24 * time.Hour)
	scan, err := NewRESTClient(server.URL).QueryRelevantProposals(cutoff, 21*24*time.Hour, 0)
	if err != nil {
		t.Fatalf("QueryRelevantProposals() error: %v", err)
	}
	if got := ids(scan.Active); len(got) != 1 || got[0] != "1060" {
		t.Fatalf("active = %v, want [1060]", got)
	}
	if got := ids(scan.RecentClosed); len(got) != 1 || got[0] != "1059" {
		t.Fatalf("recent closed = %v, want [1059]", got)
	}
	if scan.HighestID != 1060 || scan.UsedFallback || len(scan.Unreadable) != 0 {
		t.Fatalf("scan = %+v, want highest 1060 with no fallback", scan)
	}
	if n := g.requested("/cosmos/gov/v1/proposals?"); n != 2 {
		t.Fatalf("list pages requested = %d, want 2", n)
	}
}

func TestQueryRelevantProposalsIsolatesUndecodableProposal(t *testing.T) {
	t.Parallel()

	g := &govServer{
		t: t,
		list: func(r *http.Request) (int, string) {
			if r.URL.Query().Get("pagination.limit") == "1" {
				return 200, `{"proposals":[` + fakeProposal(1060, statusVotingPeriod, 3) + `],"pagination":{"next_key":"x"}}`
			}
			return 500, undecodableBody
		},
		byID: map[string]string{
			"1060": `{"proposal":` + fakeProposal(1060, statusVotingPeriod, 3) + `}`,
			"1059": undecodableBody,
			// 1058 is a deleted proposal and answers NotFound.
			"1057": `{"proposal":` + fakeProposal(1057, "PROPOSAL_STATUS_PASSED", 10) + `}`,
			"1056": `{"proposal":` + fakeProposal(1056, "PROPOSAL_STATUS_PASSED", 200) + `}`,
			"1055": undecodableBody,
		},
		status: map[string]int{"1059": 500, "1055": 500},
	}
	server := httptest.NewServer(g)
	defer server.Close()

	cutoff := scanNow.Add(-14 * 24 * time.Hour)
	scan, err := NewRESTClient(server.URL).QueryRelevantProposals(cutoff, 21*24*time.Hour, 0)
	if err != nil {
		t.Fatalf("QueryRelevantProposals() error: %v", err)
	}
	if !scan.UsedFallback {
		t.Fatal("scan did not report the one-by-one fallback")
	}
	if got := ids(scan.Active); len(got) != 1 || got[0] != "1060" {
		t.Fatalf("active = %v, want [1060]", got)
	}
	if got := ids(scan.RecentClosed); len(got) != 1 || got[0] != "1057" {
		t.Fatalf("recent closed = %v, want [1057]", got)
	}
	if len(scan.Unreadable) != 1 || scan.Unreadable[0] != 1059 {
		t.Fatalf("unreadable = %v, want [1059]; 1055 is below the floor", scan.Unreadable)
	}
	if n := g.requested("/cosmos/gov/v1/proposals/1058"); n != 1 {
		t.Fatalf("missing proposal requested %d times, want 1 with no rotation", n)
	}
	if n := g.requested("/cosmos/gov/v1/proposals/1055"); n != 0 {
		t.Fatal("scan read past the floor")
	}
}

func TestQueryRelevantProposalsTreatsProxyErrorsAsFailures(t *testing.T) {
	t.Parallel()

	g := &govServer{
		t: t,
		list: func(*http.Request) (int, string) {
			return 500, undecodableBody
		},
		byID: map[string]string{
			"1060": `{"proposal":` + fakeProposal(1060, statusVotingPeriod, 3) + `}`,
			"1059": `no available nodes found for platform cosmos-rest`,
		},
		status: map[string]int{"1059": 503},
	}
	server := httptest.NewServer(g)
	defer server.Close()

	cutoff := scanNow.Add(-14 * 24 * time.Hour)
	_, err := NewRESTClient(server.URL).QueryRelevantProposals(cutoff, 21*24*time.Hour, 1060)
	if err == nil || !strings.Contains(err.Error(), "reading proposal 1059") {
		t.Fatalf("error = %v, want a failed read of proposal 1059 rather than an unreadable proposal", err)
	}
}

func TestFindNewestProposalIDProbesPastGaps(t *testing.T) {
	t.Parallel()

	g := &govServer{
		t:    t,
		list: func(*http.Request) (int, string) { return 500, undecodableBody },
		byID: map[string]string{
			"1058": `{"proposal":{"id":"1058"}}`,
			// 1059 was deleted after its deposit period.
			"1060": undecodableBody,
		},
		status: map[string]int{"1060": 500},
	}
	server := httptest.NewServer(g)
	defer server.Close()

	top, err := NewRESTClient(server.URL).findNewestProposalID(1058)
	if err != nil {
		t.Fatalf("findNewestProposalID() error: %v", err)
	}
	if top != 1060 {
		t.Fatalf("newest = %d, want 1060 past the gap at 1059", top)
	}

	if _, err := NewRESTClient(server.URL).findNewestProposalID(0); err == nil {
		t.Fatal("findNewestProposalID(0) succeeded with no list and no known ID")
	}
}

func TestRecordProposalScanOnlyFailsForPossiblyOpenProposals(t *testing.T) {
	t.Parallel()

	c := New(nil, nil)
	floor := scanNow.AddDate(0, 0, -60)
	recent := scanNow.AddDate(0, 0, -5).Format(time.RFC3339)
	ended := scanNow.AddDate(0, 0, -1).Format(time.RFC3339)
	c.proposalMemory[1057] = Proposal{ID: "1057", Status: "PROPOSAL_STATUS_PASSED", SubmitTime: recent}
	c.proposalMemory[1058] = Proposal{ID: "1058", Status: statusVotingPeriod, SubmitTime: recent, VotingEndTime: ended}
	c.proposalMemory[1059] = Proposal{ID: "1059", Status: statusDepositPeriod, SubmitTime: recent}

	err := c.recordProposalScan(ProposalScan{HighestID: 1061, Unreadable: []uint64{1057, 1058, 1059, 1061}}, floor)
	if err == nil || !strings.Contains(err.Error(), "[1059 1061]") {
		t.Fatalf("error = %v, want only the deposit-period and unknown proposals", err)
	}
	if c.highestProposalID != 1061 {
		t.Fatalf("highest proposal = %d, want 1061", c.highestProposalID)
	}

	if err := c.recordProposalScan(ProposalScan{Unreadable: []uint64{1057}}, floor); err != nil {
		t.Fatalf("a proposal known to be closed failed the section: %v", err)
	}
}

func TestQueryGovParamsReadsProposalLifespan(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"tally_params":{"quorum":"0.4"},"params":{
  "quorum":"0.400000000000000000","threshold":"0.500000000000000000","veto_threshold":"0.334000000000000000",
  "max_deposit_period":"1209600s","voting_period":"604800s","expedited_voting_period":"259200s"}}`))
	}))
	defer server.Close()

	params, err := NewRESTClient(server.URL).QueryGovParams()
	if err != nil {
		t.Fatalf("QueryGovParams() error: %v", err)
	}
	if params.Quorum != 0.4 || params.VetoThreshold != 0.334 {
		t.Fatalf("params = %+v, want quorum 0.4 and veto 0.334", params)
	}
	if params.ProposalLifespan != 21*24*time.Hour {
		t.Fatalf("lifespan = %v, want 21 days", params.ProposalLifespan)
	}
}
