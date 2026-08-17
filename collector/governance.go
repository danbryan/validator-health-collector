package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danbryan/validator-health-collector/cosmosaddr"
	"github.com/danbryan/validator-health-collector/endpoints"
)

// GovVote is a single validator's vote on a proposal, resolved to an entity.
type GovVote struct {
	ValoperAddress string
	Moniker        string
	Entity         string
	Option         string // YES, NO, ABSTAIN, NO_WITH_VETO
	VotingPower    float64
	Height         int64
	BlockTime      time.Time
}

// ProposalAnalysis is the full picture of one proposal's voting behavior.
type ProposalAnalysis struct {
	ID                     string
	Title                  string
	Status                 string
	VotingStart            time.Time
	VotingEnd              time.Time
	Quorum                 float64
	FinalTurnout           float64
	TotalBonded            float64
	Voted                  []GovVote
	NonVoters              []GovVote // entities in the active set that never voted
	TurnoutTimeline        []TurnoutPoint
	Yes, No, Abstain, Veto float64
	IsLive                 bool

	// AttributionComplete is false when the proposal has a tally but no endpoint
	// could return its vote transactions, so Voted and NonVoters are unusable.
	AttributionComplete bool
	// VoteEndpoint is the RPC that served the vote backfill.
	VoteEndpoint string
}

// TurnoutPoint is cumulative turnout at a point during the voting window.
type TurnoutPoint struct {
	Time       time.Time
	DayOffset  float64
	Cumulative float64 // ratio of bonded
}

// txEventAttribute is one key/value pair on a transaction event.
type txEventAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// txEvent is one event emitted by a transaction.
type txEvent struct {
	Type       string             `json:"type"`
	Attributes []txEventAttribute `json:"attributes"`
}

// txResult holds the events a transaction emitted.
type txResult struct {
	Events []txEvent `json:"events"`
}

// txSearchTx is one transaction in a tx_search page.
type txSearchTx struct {
	Height   string   `json:"height"`
	TxResult txResult `json:"tx_result"`
}

// txSearchResult is the body of a tx_search response.
type txSearchResult struct {
	TotalCount string       `json:"total_count"`
	Txs        []txSearchTx `json:"txs"`
}

// rpcError is a CometBFT JSON-RPC error. Nodes with transaction indexing disabled
// report it here rather than through the HTTP status.
type rpcError struct {
	Message string `json:"message"`
	Data    string `json:"data"`
}

// txSearchResponse wraps a tx_search response.
type txSearchResponse struct {
	Result txSearchResult `json:"result"`
	Error  *rpcError      `json:"error"`
}

// voteOption is one weighted choice within a gov vote event.
type voteOption struct {
	Option int    `json:"option"`
	Weight string `json:"weight"`
}

// voteOptionName maps the numeric gov option to a readable name.
func voteOptionName(opt int) string {
	switch opt {
	case 1:
		return "YES"
	case 2:
		return "ABSTAIN"
	case 3:
		return "NO"
	case 4:
		return "NO_WITH_VETO"
	default:
		return "UNKNOWN"
	}
}

// accToValoper converts a cosmos1... account address to cosmosvaloper1...
func accToValoper(acc string) (string, error) {
	return cosmosaddr.Reprefix(acc, "cosmosvaloper")
}

// maxTxAttempts caps how many RPC endpoints one proposal's vote backfill will
// try. Higher than the REST budget because a wrong answer here is silent: a
// pruning node returns an empty result set rather than an error.
const maxTxAttempts = 4

// GovBackfiller pulls historical vote data from an indexed RPC node, rotating
// through candidates when one cannot serve the proposal.
//
// It keeps its own rotator rather than sharing the collector's RPC client, because
// the two select on different criteria: any synced node can answer /status, but
// only one whose transaction index still covers the proposal can answer its votes.
type GovBackfiller struct {
	rot    rotator
	client *http.Client
}

func NewGovBackfiller(rpcURLs ...string) *GovBackfiller {
	g := &GovBackfiller{client: endpoints.NewHTTPClient(60 * time.Second)}
	g.SetEndpoints(rpcURLs)
	return g
}

// SetEndpoints replaces the candidate list.
func (g *GovBackfiller) SetEndpoints(rpcURLs []string) { g.rot.set(rpcURLs) }

// RPCURL reports the endpoint currently selected.
func (g *GovBackfiller) RPCURL() string { return g.rot.base() }

// rpcGet queries the currently selected endpoint, with rotation on failure.
func (g *GovBackfiller) rpcGet(path string, params url.Values, dst any) error {
	return fetchJSON(&g.rot, g.client, "RPC", path+"?"+params.Encode(), dst)
}

// rpcGetFrom queries one specific endpoint with no rotation, so the caller can
// decide what a zero result means before moving on.
func (g *GovBackfiller) rpcGetFrom(base, path string, params url.Values, dst any) error {
	if base == "" {
		return errors.New("no RPC endpoint available")
	}
	return getJSONOnce(g.client, base+path+"?"+params.Encode(), dst)
}

// blockTime fetches the timestamp of a single block height.
func (g *GovBackfiller) blockTime(height int64) (time.Time, error) {
	params := url.Values{}
	params.Set("height", strconv.FormatInt(height, 10))
	var resp BlockResponse
	if err := g.rpcGet("/block", params, &resp); err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, resp.Result.Block.Header.Time)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing block time at height %d: %w", height, err)
	}
	return t, nil
}

// heightClock converts block heights to wall-clock times by interpolating between
// two measured anchor blocks.
//
// The alternative, querying /block once per vote, cost one HTTP request per voter.
// A single Hub proposal can carry over 3,000 votes, so a full cycle made thousands
// of sequential requests against public rate-limited endpoints and routinely
// stalled on 429s. Two anchors per proposal give the same chart to within a few
// seconds, because the block interval is stable over a voting window even though
// it drifts over months.
type heightClock struct {
	lowHeight  int64
	lowTime    time.Time
	secsPerBlk float64
	ok         bool
}

// newHeightClock anchors on the lowest and highest heights in the range, falling
// back to a caller-supplied interval when only one anchor can be read.
func (g *GovBackfiller) newHeightClock(lowHeight, highHeight int64, fallbackInterval float64) heightClock {
	clk := heightClock{lowHeight: lowHeight, secsPerBlk: fallbackInterval}

	lowTime, err := g.blockTime(lowHeight)
	if err != nil {
		log.Printf("WARN: gov: could not read the anchor block at height %d, "+
			"vote timeline will be omitted: %v", lowHeight, err)
		return clk
	}
	clk.lowTime = lowTime
	clk.ok = true

	if highHeight <= lowHeight {
		return clk
	}

	highTime, err := g.blockTime(highHeight)
	if err != nil {
		log.Printf("WARN: gov: could not read the closing anchor block at height %d, "+
			"falling back to the measured chain interval: %v", highHeight, err)
		return clk
	}

	span := highTime.Sub(lowTime).Seconds()
	if blocks := float64(highHeight - lowHeight); span > 0 && blocks > 0 {
		clk.secsPerBlk = span / blocks
	}
	return clk
}

// at returns the interpolated time for a height, or the zero time when the clock
// has no usable anchor.
func (c heightClock) at(height int64) time.Time {
	if !c.ok || c.secsPerBlk <= 0 {
		return time.Time{}
	}
	offset := float64(height-c.lowHeight) * c.secsPerBlk
	return c.lowTime.Add(time.Duration(offset * float64(time.Second)))
}

// VoteFetch records how a proposal's vote backfill was served, so the caller can
// tell a genuinely unanimous silence from an endpoint that simply cannot answer.
type VoteFetch struct {
	// Endpoint is the RPC that produced the result.
	Endpoint string
	// TotalCount is tx_search's reported match count.
	TotalCount int
	// Complete is false when the proposal has an on-chain tally but no endpoint
	// could return any vote transactions for it. Per-voter attribution is then
	// unusable and must not be presented as zero participation.
	Complete bool
}

// FetchVotes pulls every proposal_vote event for a proposal via tx_search,
// rotating endpoints until one can actually serve it.
//
// expectVotes must be true when the proposal has a non-zero on-chain tally. A
// node whose transaction index no longer covers the proposal's blocks answers
// with HTTP 200, no RPC error, and total_count 0, which is indistinguishable from
// "nobody voted" unless the tally is consulted. This was observed on
// cosmos-rpc.polkachu.com for proposal 1046 and on a 1M-block-retention node for
// proposal 1004, while an archive node returned 221 votes for the same query.
//
// Returns a map of voter account address to the last vote they cast, since voters
// can change their vote.
func (g *GovBackfiller) FetchVotes(proposalID string, expectVotes bool) (map[string]GovVote, VoteFetch, error) {
	if g.RPCURL() == "" {
		return nil, VoteFetch{}, errors.New("no RPC endpoint available")
	}

	attempts := maxTxAttempts
	if n := g.rot.size(); n < attempts {
		attempts = n
	}

	var lastErr error
	served := false

	for range attempts {
		base := g.RPCURL()

		votes, total, err := g.fetchVotesFrom(base, proposalID)
		if err != nil {
			lastErr = err
			log.Printf("WARN: gov: tx_search for proposal %s failed on %s, rotating: %v",
				proposalID, base, err)
			g.rot.advance()
			continue
		}
		served = true

		if expectVotes && total == 0 {
			log.Printf("WARN: gov: %s returned no vote transactions for proposal %s "+
				"despite a non-zero tally, so its transaction index does not cover it; rotating",
				base, proposalID)
			g.rot.advance()
			continue
		}

		return votes, VoteFetch{Endpoint: base, TotalCount: total, Complete: true}, nil
	}

	if !served {
		return nil, VoteFetch{}, fmt.Errorf("tx_search failed on all %d endpoints: %w", attempts, lastErr)
	}

	// Every endpoint answered but none had the index. Report the gap rather than
	// letting an empty vote set be read as real non-participation.
	log.Printf("WARN: gov: no endpoint could attribute votes for proposal %s; "+
		"per-voter attribution is unavailable for it", proposalID)
	return map[string]GovVote{}, VoteFetch{Endpoint: g.RPCURL(), TotalCount: 0, Complete: false}, nil
}

// fetchVotesFrom runs the paginated tx_search against a single endpoint.
func (g *GovBackfiller) fetchVotesFrom(base, proposalID string) (map[string]GovVote, int, error) {
	votes := make(map[string]GovVote)
	perPage := 100
	var total int

	for page := 1; ; page++ {
		params := url.Values{}
		params.Set("query", fmt.Sprintf("\"proposal_vote.proposal_id=%s\"", proposalID))
		params.Set("per_page", strconv.Itoa(perPage))
		params.Set("page", strconv.Itoa(page))
		params.Set("order_by", "\"asc\"")

		var resp txSearchResponse
		if err := g.rpcGetFrom(base, "/tx_search", params, &resp); err != nil {
			return nil, 0, fmt.Errorf("tx_search page %d: %w", page, err)
		}
		if resp.Error != nil {
			return nil, 0, fmt.Errorf("tx_search error: %s (%s)", resp.Error.Message, resp.Error.Data)
		}

		total, _ = strconv.Atoi(resp.Result.TotalCount)
		if len(resp.Result.Txs) == 0 {
			break
		}

		for _, tx := range resp.Result.Txs {
			height, _ := strconv.ParseInt(tx.Height, 10, 64)
			for _, ev := range tx.TxResult.Events {
				if ev.Type != "proposal_vote" {
					continue
				}
				var voter, optionRaw, pid string
				for _, a := range ev.Attributes {
					switch a.Key {
					case "voter":
						voter = a.Value
					case "option":
						optionRaw = a.Value
					case "proposal_id":
						pid = a.Value
					default:
						// Other attributes on the event are not needed here.
					}
				}
				if pid != proposalID || voter == "" {
					continue
				}
				// option is a JSON array like [{"option":1,"weight":"1.000..."}]
				var opts []voteOption
				if err := json.Unmarshal([]byte(optionRaw), &opts); err != nil || len(opts) == 0 {
					continue
				}
				// Later votes overwrite earlier ones (voters can change their vote).
				votes[voter] = GovVote{
					Option: voteOptionName(opts[0].Option),
					Height: height,
				}
			}
		}

		if page*perPage >= total {
			break
		}
	}
	return votes, total, nil
}

// tallyOf reads a proposal's vote tally, which includes delegator votes and is
// therefore authoritative rather than the sum of validator votes.
//
// A proposal still in its voting period reports a zeroed final_tally_result; the
// running tally lives on a separate endpoint, so that is read instead and an open
// proposal shows real turnout rather than 0%.
func tallyOf(
	prop *Proposal,
	liveTally func(string) (float64, float64, float64, float64, error),
) (float64, float64, float64, float64) {
	yes, _ := strconv.ParseFloat(prop.FinalTallyResult.YesCount, 64)
	no, _ := strconv.ParseFloat(prop.FinalTallyResult.NoCount, 64)
	abstain, _ := strconv.ParseFloat(prop.FinalTallyResult.AbstainCount, 64)
	veto, _ := strconv.ParseFloat(prop.FinalTallyResult.NoWithVetoCount, 64)

	if prop.Status == statusVotingPeriod && liveTally != nil {
		ly, ln, la, lv, err := liveTally(prop.ID)
		if err == nil && (ly+ln+la+lv) > 0 {
			return ly, ln, la, lv
		}
	}
	return yes, no, abstain, veto
}

// AnalyzeProposal builds the full picture for one proposal: who voted, who did
// not, and how turnout accumulated across the voting window.
func (g *GovBackfiller) AnalyzeProposal(
	prop *Proposal,
	validators []Validator,
	entityMap map[string]string,
	bondedTokens float64,
	quorum float64,
	chainSecondsPerBlock float64,
	liveTally func(string) (yes, no, abstain, veto float64, err error),
) (*ProposalAnalysis, error) {
	votingStart, _ := time.Parse(time.RFC3339, prop.VotingStartTime)
	votingEnd, _ := time.Parse(time.RFC3339, prop.VotingEndTime)

	analysis := &ProposalAnalysis{
		ID:          prop.ID,
		Title:       prop.Title,
		Status:      strings.TrimPrefix(prop.Status, "PROPOSAL_STATUS_"),
		VotingStart: votingStart,
		VotingEnd:   votingEnd,
		Quorum:      quorum,
		TotalBonded: bondedTokens,
		IsLive:      prop.Status == statusVotingPeriod,
	}

	// The tally is read before the per-voter backfill on purpose. It is the
	// authoritative measure of whether anyone voted, and the backfill needs to
	// know that in order to tell "nobody voted" apart from "this node's
	// transaction index no longer reaches the proposal".
	yes, no, abstain, veto := tallyOf(prop, liveTally)
	analysis.Yes, analysis.No, analysis.Abstain, analysis.Veto = yes, no, abstain, veto
	if bondedTokens > 0 {
		analysis.FinalTurnout = (yes + no + abstain + veto) / bondedTokens
	}

	rawVotes, fetch, err := g.FetchVotes(prop.ID, yes+no+abstain+veto > 0)
	if err != nil {
		return nil, err
	}
	analysis.AttributionComplete = fetch.Complete
	analysis.VoteEndpoint = fetch.Endpoint

	// Build valoper -> validator lookup so we can attach power + moniker.
	valoperInfo := make(map[string]Validator, len(validators))
	for _, v := range validators {
		valoperInfo[v.OperatorAddress] = v
	}

	// Resolve each vote's account address into its validator identity.
	votedValopers := make(map[string]bool)

	for accAddr, vote := range rawVotes {
		valoper, err := accToValoper(accAddr)
		if err != nil {
			continue
		}
		v, ok := valoperInfo[valoper]
		if !ok {
			// Voter is a delegator, not an active-set validator. Their stake still
			// counts toward turnout, but we cannot attribute it to a set member.
			continue
		}
		power, _ := strconv.ParseFloat(v.Tokens, 64)
		entity := entityMap[valoper]
		if entity == "" {
			entity = v.Description.Moniker
		}
		vote.ValoperAddress = valoper
		vote.Moniker = v.Description.Moniker
		vote.Entity = entity
		vote.VotingPower = power

		analysis.Voted = append(analysis.Voted, vote)
		votedValopers[valoper] = true
	}

	// Non-voters: active set members with no recorded vote.
	for _, v := range validators {
		if votedValopers[v.OperatorAddress] {
			continue
		}
		power, _ := strconv.ParseFloat(v.Tokens, 64)
		entity := entityMap[v.OperatorAddress]
		if entity == "" {
			entity = v.Description.Moniker
		}
		analysis.NonVoters = append(analysis.NonVoters, GovVote{
			ValoperAddress: v.OperatorAddress,
			Moniker:        v.Description.Moniker,
			Entity:         entity,
			Option:         "DID_NOT_VOTE",
			VotingPower:    power,
		})
	}

	// Sort both lists by voting power descending.
	sort.Slice(analysis.Voted, func(i, j int) bool {
		return analysis.Voted[i].VotingPower > analysis.Voted[j].VotingPower
	})
	sort.Slice(analysis.NonVoters, func(i, j int) bool {
		return analysis.NonVoters[i].VotingPower > analysis.NonVoters[j].VotingPower
	})

	// Turnout timeline: accumulate validator voting power in vote order, then
	// scale so the final point matches the authoritative on-chain turnout.
	timelineVotes := make([]GovVote, len(analysis.Voted))
	copy(timelineVotes, analysis.Voted)
	sort.Slice(timelineVotes, func(i, j int) bool {
		return timelineVotes[i].Height < timelineVotes[j].Height
	})

	var validatorPowerTotal float64
	for _, v := range timelineVotes {
		validatorPowerTotal += v.VotingPower
	}
	scale := 1.0
	if validatorPowerTotal > 0 && analysis.FinalTurnout > 0 {
		scale = (analysis.FinalTurnout * bondedTokens) / validatorPowerTotal
	}

	// Timestamp the votes from two anchor blocks rather than one lookup per vote.
	// See heightClock for why.
	var clk heightClock
	if len(timelineVotes) > 0 {
		clk = g.newHeightClock(
			timelineVotes[0].Height,
			timelineVotes[len(timelineVotes)-1].Height,
			chainSecondsPerBlock,
		)
	}

	var cumulative float64
	windowDays := votingEnd.Sub(votingStart).Hours() / 24
	for i, v := range timelineVotes {
		cumulative += v.VotingPower * scale
		if bondedTokens == 0 {
			continue
		}
		blockTime := clk.at(v.Height)
		if blockTime.IsZero() {
			continue
		}
		timelineVotes[i].BlockTime = blockTime

		dayOffset := blockTime.Sub(votingStart).Hours() / 24
		if dayOffset < 0 {
			dayOffset = 0
		}
		if windowDays > 0 && dayOffset > windowDays {
			dayOffset = windowDays
		}
		analysis.TurnoutTimeline = append(analysis.TurnoutTimeline, TurnoutPoint{
			Time:       blockTime,
			DayOffset:  dayOffset,
			Cumulative: cumulative / bondedTokens,
		})
	}

	return analysis, nil
}
