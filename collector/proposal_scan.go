package collector

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Proposal discovery has to keep working across chain upgrades, which happen
// often on the Hub. An upgrade can drop a module or message type that an old
// proposal still stores, and from then on no node can decode that proposal. The
// v29 upgrade did this to legacy x/params proposals. The list endpoint decodes
// every proposal on a page, so one undecodable proposal failed the whole list.
//
// Two layers contain that:
//
//  1. The list is read newest first and stops at the first proposal submitted
//     before the history window could matter, so old proposals are never read.
//  2. If a page still fails, proposals are read one ID at a time. An undecodable
//     proposal is then isolated and reported instead of hiding every other one.

const (
	// proposalPageSize keeps one bad proposal from costing more than a page.
	proposalPageSize = 50

	// proposalWalkLimit caps the one-by-one fallback so an unexpected chain state
	// cannot stretch one cycle into thousands of requests.
	proposalWalkLimit = 250

	// proposalProbeMisses is how many consecutive missing IDs end the upward
	// search for the newest proposal. Proposals that never reach their deposit
	// are deleted, so the ID sequence has gaps.
	proposalProbeMisses = 10

	// defaultProposalLifespan bounds submit-to-vote-end when the chain does not
	// report its deposit and voting periods.
	defaultProposalLifespan = 60 * 24 * time.Hour

	// proposalLifespanMargin absorbs governance changes that lengthened the
	// periods after an older proposal was submitted.
	proposalLifespanMargin = 7 * 24 * time.Hour
)

// gRPC status codes carried in Cosmos REST error bodies.
const (
	grpcUnknown  = 2
	grpcNotFound = 5
	grpcInternal = 13
)

// ProposalScan is the result of one proposal discovery pass.
type ProposalScan struct {
	Active       []Proposal
	RecentClosed []Proposal

	// Seen holds every proposal decoded inside the scan window, in any status.
	Seen []Proposal

	// Unreadable lists proposal IDs inside the scan window that no endpoint
	// could decode. Any of them could be open for voting.
	Unreadable []uint64

	// HighestID is the newest proposal ID found, kept for the next cycle.
	HighestID uint64

	// UsedFallback reports that the list failed and proposals were read one by one.
	UsedFallback bool

	lowestID uint64
}

// proposalScanFloor is the submit time before which a proposal cannot matter.
// A proposal's vote ends at most one lifespan after submission, so anything
// submitted before cutoff minus the lifespan closed before the history window.
func proposalScanFloor(cutoff time.Time, lifespan time.Duration) time.Time {
	if lifespan <= 0 {
		lifespan = defaultProposalLifespan
	}
	return cutoff.Add(-(lifespan + proposalLifespanMargin))
}

// submittedBefore reports whether a proposal was certainly submitted before t.
// Every lifecycle timestamp is at or after submission, so the earliest readable
// one bounds the submit time from above. ok is false when none can be read.
func submittedBefore(p *Proposal, t time.Time) (before, ok bool) {
	var earliest time.Time
	for _, raw := range []string{p.SubmitTime, p.DepositEndTime, p.VotingStartTime, p.VotingEndTime} {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil || parsed.Year() <= 1 {
			continue
		}
		if earliest.IsZero() || parsed.Before(earliest) {
			earliest = parsed
		}
	}
	if earliest.IsZero() {
		return false, false
	}
	return earliest.Before(t), true
}

// add records one decoded proposal and reports whether the scan has reached
// proposals too old to matter. IDs rise with submit time, so the first proposal
// submitted before the floor means every lower ID is older still.
func (s *ProposalScan) add(p Proposal, cutoff, floor time.Time) (done bool) {
	if id, err := strconv.ParseUint(p.ID, 10, 64); err == nil {
		if id > s.HighestID {
			s.HighestID = id
		}
		if s.lowestID == 0 || id < s.lowestID {
			s.lowestID = id
		}
	}

	// Classify before checking the floor, so a vote that somehow outlasted the
	// lifespan bound is still reported rather than dropped.
	switch p.Status {
	case statusVotingPeriod:
		s.Active = append(s.Active, p)
	case statusDepositPeriod:
	default:
		votingEnd, err := time.Parse(time.RFC3339, p.VotingEndTime)
		if err == nil && !votingEnd.Before(cutoff) {
			s.RecentClosed = append(s.RecentClosed, p)
		}
	}

	before, ok := submittedBefore(&p, floor)
	if ok && before {
		return true
	}
	s.Seen = append(s.Seen, p)
	return false
}

// QueryRelevantProposals returns every proposal in its voting period plus closed
// proposals whose vote ended on or after cutoff.
//
// lifespan is the chain's longest submit-to-vote-end time, from QueryGovParams;
// zero falls back to a conservative default. knownHighest is the newest proposal
// ID from an earlier cycle, used only when the list cannot be read at all.
func (c *RESTClient) QueryRelevantProposals(
	cutoff time.Time,
	lifespan time.Duration,
	knownHighest uint64,
) (ProposalScan, error) {
	floor := proposalScanFloor(cutoff, lifespan)
	var scan ProposalScan

	var key string
	for {
		page, next, err := c.listProposalsNewestFirst(key, proposalPageSize)
		if err != nil {
			log.Printf("WARN: gov: proposal list failed, reading proposals one by one instead: %v", err)
			return c.walkProposals(scan, cutoff, floor, knownHighest, err)
		}
		for _, p := range page {
			if scan.add(p, cutoff, floor) {
				return scan, nil
			}
		}
		if next == "" || next == nullNextKey {
			return scan, nil
		}
		key = next
	}
}

// listProposalsNewestFirst reads one page of the proposal list in descending ID order.
func (c *RESTClient) listProposalsNewestFirst(key string, limit int) ([]Proposal, string, error) {
	params := url.Values{}
	params.Set("pagination.limit", strconv.Itoa(limit))
	params.Set("pagination.reverse", "true")
	if key != "" {
		params.Set("pagination.key", key)
	}
	var resp ProposalsResponse
	if err := c.get("/cosmos/gov/v1/proposals?"+params.Encode(), &resp); err != nil {
		return nil, "", fmt.Errorf("querying proposals: %w", err)
	}
	return resp.Proposals, resp.Pagination.NextKey, nil
}

// walkProposals reads proposals one ID at a time, newest first, after the list
// endpoint failed. It continues from wherever the list stopped.
func (c *RESTClient) walkProposals(
	scan ProposalScan,
	cutoff, floor time.Time,
	knownHighest uint64,
	listErr error,
) (ProposalScan, error) {
	scan.UsedFallback = true

	start := uint64(0)
	if scan.lowestID > 0 {
		// Earlier pages already covered everything newer.
		start = scan.lowestID - 1
	} else {
		top, err := c.findNewestProposalID(knownHighest)
		if err != nil {
			return scan, fmt.Errorf("proposal list failed and the newest proposal could not be located: %w",
				errors.Join(listErr, err))
		}
		start = top
		if top > scan.HighestID {
			scan.HighestID = top
		}
	}

	steps := 0
	for id := start; id > 0; id-- {
		if steps >= proposalWalkLimit {
			return scan, fmt.Errorf("read %d proposals one by one without reaching proposals submitted before %s",
				steps, floor.Format(time.RFC3339))
		}
		steps++

		p, err := c.queryProposalNumber(id)
		switch classifyProposalRead(err) {
		case proposalFound:
			if scan.add(*p, cutoff, floor) {
				return scan, nil
			}
		case proposalMissing:
		case proposalUnreadable:
			log.Printf("WARN: gov: no endpoint can decode proposal %d, reporting it as unreadable: %v", id, err)
			scan.Unreadable = append(scan.Unreadable, id)
		default:
			return scan, fmt.Errorf("reading proposal %d: %w", id, err)
		}
	}
	return scan, nil
}

// findNewestProposalID locates the highest proposal ID when the first list page
// cannot be read.
func (c *RESTClient) findNewestProposalID(knownHighest uint64) (uint64, error) {
	// The newest proposal on its own is usually still readable even when an
	// older one on the same page is not.
	if page, _, err := c.listProposalsNewestFirst("", 1); err == nil && len(page) == 1 {
		if id, parseErr := strconv.ParseUint(page[0].ID, 10, 64); parseErr == nil {
			return id, nil
		}
	}
	if knownHighest == 0 {
		return 0, errors.New("no proposal ID is known from an earlier cycle")
	}

	top := knownHighest
	misses := 0
	for id := knownHighest + 1; misses < proposalProbeMisses; id++ {
		_, err := c.queryProposalNumber(id)
		switch classifyProposalRead(err) {
		case proposalMissing:
			misses++
		case proposalFound, proposalUnreadable:
			top = id
			misses = 0
		default:
			return 0, fmt.Errorf("probing proposal %d: %w", id, err)
		}
	}
	return top, nil
}

// queryProposalNumber fetches one proposal. A missing ID is final and does not
// rotate, because every synced node gives the same answer.
func (c *RESTClient) queryProposalNumber(id uint64) (*Proposal, error) {
	var resp ProposalResponse
	path := "/cosmos/gov/v1/proposals/" + strconv.FormatUint(id, 10)
	err := fetchJSONUnless(&c.rot, c.client, "REST", path, &resp, func(err error) bool {
		return classifyProposalRead(err) == proposalMissing
	})
	if err != nil {
		return nil, err
	}
	return &resp.Proposal, nil
}

type proposalReadOutcome int

const (
	proposalFound proposalReadOutcome = iota
	proposalMissing
	proposalUnreadable
	proposalReadFailed
)

// classifyProposalRead separates the chain's own answers from endpoint trouble.
//
// A missing proposal is a gRPC NotFound. An undecodable one is an HTTP 500 whose
// body is a gRPC Internal or Unknown error, which is how the SDK reports a
// stored value it cannot unmarshal. Proxy errors, rate limits, timeouts and
// non-gRPC bodies are endpoint trouble and fail the read, so a flaky endpoint is
// never mistaken for an unreadable proposal.
func classifyProposalRead(err error) proposalReadOutcome {
	if err == nil {
		return proposalFound
	}
	var se *statusError
	if !errors.As(err, &se) {
		return proposalReadFailed
	}
	code := se.grpcCode()
	switch {
	case code == grpcNotFound:
		return proposalMissing
	case se.Status == http.StatusInternalServerError && (code == grpcInternal || code == grpcUnknown):
		return proposalUnreadable
	default:
		return proposalReadFailed
	}
}
