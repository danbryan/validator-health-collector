package collector

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/danbryan/validator-health-collector/endpoints"
)

const restTimeout = 30 * time.Second

// RESTClient queries a Cosmos REST API, rotating through ranked candidate
// endpoints when one fails.
type RESTClient struct {
	rot    rotator
	client *http.Client
}

// NewRESTClient builds a client over one or more ranked base URLs.
func NewRESTClient(baseURLs ...string) *RESTClient {
	c := &RESTClient{client: endpoints.NewHTTPClient(restTimeout)}
	c.SetEndpoints(baseURLs)
	return c
}

// SetEndpoints replaces the candidate list.
func (c *RESTClient) SetEndpoints(baseURLs []string) { c.rot.set(baseURLs) }

// BaseURL reports the endpoint currently selected.
func (c *RESTClient) BaseURL() string { return c.rot.base() }

func (c *RESTClient) get(path string, dst any) error {
	return fetchJSON(&c.rot, c.client, "REST", path, dst)
}

// RPCClient queries a CometBFT RPC endpoint, rotating on failure.
//
// This exists separately from RESTClient because the two protocols have
// independent candidate lists and rankings: RPC candidates are ranked by how much
// block history they retain, which REST endpoints do not expose.
type RPCClient struct {
	rot    rotator
	client *http.Client
}

// NewRPCClient builds a client over one or more ranked base URLs.
func NewRPCClient(baseURLs ...string) *RPCClient {
	c := &RPCClient{client: endpoints.NewHTTPClient(restTimeout)}
	c.SetEndpoints(baseURLs)
	return c
}

// SetEndpoints replaces the candidate list.
func (c *RPCClient) SetEndpoints(baseURLs []string) { c.rot.set(baseURLs) }

// BaseURL reports the endpoint currently selected.
func (c *RPCClient) BaseURL() string { return c.rot.base() }

func (c *RPCClient) get(path string, dst any) error {
	return fetchJSON(&c.rot, c.client, "RPC", path, dst)
}

// QueryBondedValidators fetches all bonded validators with pagination.
func (c *RESTClient) QueryBondedValidators() ([]Validator, error) {
	return c.queryValidators("BOND_STATUS_BONDED")
}

// QueryAllValidators fetches every validator status with pagination.
func (c *RESTClient) QueryAllValidators() ([]Validator, error) {
	return c.queryValidators("")
}

func (c *RESTClient) queryValidators(status string) ([]Validator, error) {
	var all []Validator
	var nextKey string

	for {
		params := url.Values{}
		params.Set("pagination.limit", "100")
		if status != "" {
			params.Set("status", status)
		}
		if nextKey != "" {
			params.Set("pagination.key", nextKey)
		}

		var resp ValidatorsResponse
		if err := c.get("/cosmos/staking/v1beta1/validators?"+params.Encode(), &resp); err != nil {
			return nil, fmt.Errorf("querying validators: %w", err)
		}
		all = append(all, resp.Validators...)
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return all, nil
}

// QueryStakingPool fetches the staking pool (bonded + not bonded tokens).
func (c *RESTClient) QueryStakingPool() (float64, error) {
	var resp PoolResponse
	if err := c.get("/cosmos/staking/v1beta1/pool", &resp); err != nil {
		return 0, fmt.Errorf("querying pool: %w", err)
	}
	bonded, err := parseUAtom(resp.Pool.BondedTokens)
	if err != nil {
		return 0, fmt.Errorf("parsing bonded tokens: %w", err)
	}
	return bonded, nil
}

// QueryDelegations fetches every positive uatom delegation for an account.
func (c *RESTClient) QueryDelegations(delegator string) ([]ManagedDelegation, error) {
	var all []ManagedDelegation
	var nextKey string
	for {
		params := url.Values{}
		params.Set("pagination.limit", "100")
		if nextKey != "" {
			params.Set("pagination.key", nextKey)
		}
		path := "/cosmos/staking/v1beta1/delegations/" + url.PathEscape(delegator) + "?" + params.Encode()
		var resp DelegationsResponse
		if err := c.get(path, &resp); err != nil {
			return nil, fmt.Errorf("querying delegations for %s: %w", delegator, err)
		}
		for i, item := range resp.DelegationResponses {
			if item.Delegation.ValidatorAddress == "" {
				return nil, fmt.Errorf("delegation %d for %s has no validator address", i, delegator)
			}
			if item.Balance.Denom != "uatom" {
				return nil, fmt.Errorf("delegation %d for %s has unsupported denom %q", i, delegator, item.Balance.Denom)
			}
			amount, err := parseUAtom(item.Balance.Amount)
			if err != nil {
				return nil, fmt.Errorf("delegation %d for %s has invalid amount %q: %w", i, delegator, item.Balance.Amount, err)
			}
			if amount > 0 {
				all = append(all, ManagedDelegation{OperatorAddress: item.Delegation.ValidatorAddress, AmountUAtom: amount})
			}
		}
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return all, nil
}

// QueryReceivingRedelegations fetches every receiving redelegation entry still
// returned by current chain state. The chain response, rather than local time or
// calculated balance, determines whether a delegator-destination pair is locked.
func (c *RESTClient) QueryReceivingRedelegations(delegator string) ([]ReceivingRedelegation, error) {
	var all []ReceivingRedelegation
	var nextKey string
	for {
		params := url.Values{}
		params.Set("pagination.limit", "100")
		if nextKey != "" {
			params.Set("pagination.key", nextKey)
		}
		path := "/cosmos/staking/v1beta1/delegators/" + url.PathEscape(delegator) + "/redelegations?" + params.Encode()
		var resp RedelegationsResponse
		if err := c.get(path, &resp); err != nil {
			return nil, fmt.Errorf("querying receiving redelegations for %s: %w", delegator, err)
		}
		for i, response := range resp.RedelegationResponses {
			if response.Redelegation.DestValidator == "" {
				return nil, fmt.Errorf("redelegation %d for %s has no destination validator", i, delegator)
			}
			for j, entry := range response.Entries {
				amount, err := parseUAtom(entry.Balance)
				if err != nil {
					return nil, fmt.Errorf("redelegation %d entry %d for %s has invalid balance %q: %w", i, j, delegator, entry.Balance, err)
				}
				completion, err := time.Parse(time.RFC3339Nano, entry.Entry.CompletionTime)
				if err != nil {
					return nil, fmt.Errorf("redelegation %d entry %d for %s has invalid completion time %q: %w", i, j, delegator, entry.Entry.CompletionTime, err)
				}
				all = append(all, ReceivingRedelegation{
					DelegatorAddress:    delegator,
					DestinationOperator: response.Redelegation.DestValidator,
					BalanceUAtom:        amount,
					CompletionTime:      completion,
				})
			}
		}
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return all, nil
}

// QueryConsensusSet returns the set of validators actually participating in
// consensus, keyed by their hex consensus address.
//
// This is deliberately not the staking module's bonded list. The staking module
// reports every validator with bonded status (200 on the Hub, matching
// max_validators), but CometBFT only seats the top N by voting power in the
// active consensus set (180 on the Hub). The 20 in between are bonded but are
// not signing blocks, so they must not count toward set-health metrics.
func (c *RPCClient) QueryConsensusSet() (map[string]bool, error) {
	return c.QueryConsensusSetAtHeight(0)
}

// QueryConsensusSetAtHeight returns the consensus validator set at an exact
// height. Height 0 asks CometBFT for the current set.
func (c *RPCClient) QueryConsensusSetAtHeight(height int64) (map[string]bool, error) {
	if height < 0 {
		return nil, fmt.Errorf("querying consensus set: invalid height %d", height)
	}

	set := make(map[string]bool)
	for page := 1; ; page++ {
		params := url.Values{}
		params.Set("page", strconv.Itoa(page))
		params.Set("per_page", "100")
		if height > 0 {
			params.Set("height", strconv.FormatInt(height, 10))
		}
		var cv CometValidatorsResponse
		if err := c.get("/validators?"+params.Encode(), &cv); err != nil {
			if height > 0 {
				return nil, fmt.Errorf("querying consensus set at height %d: %w", height, err)
			}
			return nil, fmt.Errorf("querying consensus set: %w", err)
		}
		if height > 0 {
			returnedHeight, err := strconv.ParseInt(cv.Result.BlockHeight, 10, 64)
			if err != nil || returnedHeight != height {
				return nil, fmt.Errorf("querying consensus set at height %d: response has height %q", height, cv.Result.BlockHeight)
			}
		}
		for _, v := range cv.Result.Validators {
			set[strings.ToUpper(v.Address)] = true
		}

		total, err := strconv.Atoi(cv.Result.Total)
		if err != nil || total < 0 {
			return nil, fmt.Errorf("querying consensus set: invalid total %q", cv.Result.Total)
		}
		if len(set) >= total || len(cv.Result.Validators) == 0 {
			break
		}
	}
	return set, nil
}

// QueryCommitSigners fetches one exact completed commit and returns the
// uppercase hex consensus addresses whose signatures commit that block.
func (c *RPCClient) QueryCommitSigners(height int64) (CommitSigners, error) {
	if height <= 0 {
		return nil, fmt.Errorf("querying commit signers: invalid height %d", height)
	}

	var response CometCommitResponse
	path := "/commit?height=" + strconv.FormatInt(height, 10)
	if err := c.get(path, &response); err != nil {
		return nil, fmt.Errorf("querying commit signers at height %d: %w", height, err)
	}
	returnedHeight, err := strconv.ParseInt(response.Result.SignedHeader.Commit.Height, 10, 64)
	if err != nil || returnedHeight != height {
		return nil, fmt.Errorf("querying commit signers at height %d: response has height %q", height, response.Result.SignedHeader.Commit.Height)
	}

	signers := make(CommitSigners)
	for _, signature := range response.Result.SignedHeader.Commit.Signatures {
		if signature.BlockIDFlag != 2 || signature.ValidatorAddress == "" {
			continue
		}
		signers[strings.ToUpper(signature.ValidatorAddress)] = true
	}
	return signers, nil
}

// SlashingParams holds the on-chain slashing configuration.
type SlashingParams struct {
	SignedBlocksWindow      int64
	MinSignedPerWindow      float64
	MinSignedBlocks         int64 // SDK-rounded window * minSignedPerWindow
	MaxMissedBlocks         int64 // window - minSignedBlocks; jail past this
	DowntimeJailDuration    string
	SlashFractionDoubleSign float64
	SlashFractionDowntime   float64
}

// QuerySlashingParams fetches the on-chain slashing parameters. These drive
// every jail calculation, so they are read from the chain rather than assumed.
func (c *RESTClient) QuerySlashingParams() (*SlashingParams, error) {
	var resp SlashingParamsResponse
	if err := c.get("/cosmos/slashing/v1beta1/params", &resp); err != nil {
		return nil, fmt.Errorf("querying slashing params: %w", err)
	}
	window, err := strconv.ParseInt(resp.Params.SignedBlocksWindow, 10, 64)
	if err != nil || window <= 0 {
		return nil, fmt.Errorf("invalid signed_blocks_window %q", resp.Params.SignedBlocksWindow)
	}
	minRatio, minSigned, err := parseMinSignedBlocks(resp.Params.MinSignedPerWindow, window)
	if err != nil {
		return nil, fmt.Errorf("parsing min_signed_per_window: %w", err)
	}
	doubleSign, err := parseRatio(resp.Params.SlashFractionDoubleSign)
	if err != nil {
		return nil, fmt.Errorf("parsing slash_fraction_double_sign: %w", err)
	}
	downtime, err := parseRatio(resp.Params.SlashFractionDowntime)
	if err != nil {
		return nil, fmt.Errorf("parsing slash_fraction_downtime: %w", err)
	}

	maxMissed := window - minSigned
	if maxMissed <= 0 {
		return nil, fmt.Errorf("slashing params produce non-positive maximum missed blocks: %d", maxMissed)
	}
	return &SlashingParams{
		SignedBlocksWindow:      window,
		MinSignedPerWindow:      minRatio,
		MinSignedBlocks:         minSigned,
		MaxMissedBlocks:         maxMissed,
		DowntimeJailDuration:    resp.Params.DowntimeJailDuration,
		SlashFractionDoubleSign: doubleSign,
		SlashFractionDowntime:   downtime,
	}, nil
}

// MeasureBlockTime samples two block headers to derive the current average
// block interval, rather than assuming a fixed value.
func (c *RPCClient) MeasureBlockTime(sampleBlocks int64) (float64, error) {
	fetch := func(height int64) (int64, time.Time, error) {
		path := "/block"
		if height > 0 {
			path += "?height=" + strconv.FormatInt(height, 10)
		}
		var bh BlockResponse
		if err := c.get(path, &bh); err != nil {
			return 0, time.Time{}, err
		}
		h, err := strconv.ParseInt(bh.Result.Block.Header.Height, 10, 64)
		if err != nil || h <= 0 {
			return 0, time.Time{}, fmt.Errorf("invalid block height %q", bh.Result.Block.Header.Height)
		}
		t, err := time.Parse(time.RFC3339Nano, bh.Result.Block.Header.Time)
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("invalid block time %q: %w", bh.Result.Block.Header.Time, err)
		}
		return h, t, nil
	}

	hLatest, tLatest, err := fetch(0)
	if err != nil {
		return 0, err
	}
	if sampleBlocks <= 0 || hLatest <= sampleBlocks {
		return 0, errors.New("invalid block-time sample range")
	}
	hOld, tOld, err := fetch(hLatest - sampleBlocks)
	if err != nil {
		return 0, err
	}
	if hLatest <= hOld {
		return 0, errors.New("invalid block range")
	}
	return tLatest.Sub(tOld).Seconds() / float64(hLatest-hOld), nil
}

// QueryRelevantProposals scans proposal summaries once and returns everything
// currently in voting plus closed proposals whose voting window ended on or
// after cutoff. This bounds dashboard history by time rather than an arbitrary
// proposal count while avoiding a second governance-list scan.
func (c *RESTClient) QueryRelevantProposals(cutoff time.Time) (active, recentClosed []Proposal, err error) {
	var nextKey string
	for {
		path := "/cosmos/gov/v1/proposals?pagination.limit=100"
		if nextKey != "" {
			path += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		var resp ProposalsResponse
		if getErr := c.get(path, &resp); getErr != nil {
			return nil, nil, fmt.Errorf("querying proposals: %w", getErr)
		}
		for _, proposal := range resp.Proposals {
			switch proposal.Status {
			case statusVotingPeriod:
				active = append(active, proposal)
			case statusDepositPeriod:
				continue
			default:
				votingEnd, parseErr := time.Parse(time.RFC3339, proposal.VotingEndTime)
				if parseErr == nil && !votingEnd.Before(cutoff) {
					recentClosed = append(recentClosed, proposal)
				}
			}
		}
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return active, recentClosed, nil
}

// QueryLiveTally fetches the running tally for a proposal still in its voting
// period. The proposal object's final_tally_result stays zeroed until voting
// closes, so an open proposal must be read from this endpoint instead.
func (c *RESTClient) QueryLiveTally(id string) (float64, float64, float64, float64, error) {
	var resp TallyResponse
	if err := c.get("/cosmos/gov/v1/proposals/"+id+"/tally", &resp); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("querying live tally for %s: %w", id, err)
	}
	yes, _ := strconv.ParseFloat(resp.Tally.YesCount, 64)
	no, _ := strconv.ParseFloat(resp.Tally.NoCount, 64)
	abstain, _ := strconv.ParseFloat(resp.Tally.AbstainCount, 64)
	veto, _ := strconv.ParseFloat(resp.Tally.NoWithVetoCount, 64)
	return yes, no, abstain, veto, nil
}

// QueryProposalVotes fetches the current vote record for each account on a live
// proposal. Unlike tx_search, this does not download transaction history or
// require an archive-indexed RPC node. Closed proposals normally return no rows
// because the governance module removes vote records after finalization.
func (c *RESTClient) QueryProposalVotes(id string) (map[string]GovVote, error) {
	votes := make(map[string]GovVote)
	var nextKey string
	for {
		path := fmt.Sprintf("/cosmos/gov/v1/proposals/%s/votes?pagination.limit=1000", id)
		if nextKey != "" {
			path += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		var resp ProposalVotesResponse
		if err := c.get(path, &resp); err != nil {
			return nil, fmt.Errorf("querying current votes for proposal %s: %w", id, err)
		}
		for _, rawVote := range resp.Votes {
			weights := make(map[string]float64, len(rawVote.Options))
			for _, option := range rawVote.Options {
				weight, parseErr := strconv.ParseFloat(option.Weight, 64)
				if parseErr != nil || weight <= 0 {
					continue
				}
				weights[normalizeVoteOption(option.Option)] += weight
			}
			if rawVote.Voter == "" || len(weights) == 0 {
				continue
			}
			primary := "WEIGHTED"
			if len(weights) == 1 {
				for option := range weights {
					primary = option
				}
			}
			votes[rawVote.Voter] = GovVote{Option: primary, Options: weights}
		}
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return votes, nil
}

// QueryProposalByID fetches a single proposal by ID.
func (c *RESTClient) QueryProposalByID(id string) (*Proposal, error) {
	var resp ProposalResponse
	if err := c.get("/cosmos/gov/v1/proposals/"+id, &resp); err != nil {
		return nil, fmt.Errorf("querying proposal %s: %w", id, err)
	}
	return &resp.Proposal, nil
}

// QueryGovParams fetches governance tallying parameters.
func (c *RESTClient) QueryGovParams() (float64, float64, float64, error) {
	var resp GovParamsResponse
	if err := c.get("/cosmos/gov/v1/params/tallying", &resp); err != nil {
		return 0, 0, 0, fmt.Errorf("querying gov params: %w", err)
	}
	q, _ := strconv.ParseFloat(resp.Params.Quorum, 64)
	t, _ := strconv.ParseFloat(resp.Params.Threshold, 64)
	v, _ := strconv.ParseFloat(resp.Params.VetoThreshold, 64)
	return q, t, v, nil
}

// QuerySigningInfos fetches all signing infos with pagination.
func (c *RESTClient) QuerySigningInfos() ([]SigningInfo, error) {
	var all []SigningInfo
	var nextKey string

	for {
		path := "/cosmos/slashing/v1beta1/signing_infos?pagination.limit=100"
		if nextKey != "" {
			path += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		var resp SigningInfosResponse
		if err := c.get(path, &resp); err != nil {
			return nil, fmt.Errorf("querying signing infos: %w", err)
		}
		all = append(all, resp.Info...)
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return all, nil
}

// QueryAnnualProvisions fetches the mint module's annual uatom provisions.
func (c *RESTClient) QueryAnnualProvisions() (float64, error) {
	var resp AnnualProvisionsResponse
	if err := c.get("/cosmos/mint/v1beta1/annual_provisions", &resp); err != nil {
		return 0, fmt.Errorf("querying annual provisions: %w", err)
	}
	value, err := parseNonNegativeDecimal(resp.AnnualProvisions)
	if err != nil {
		return 0, fmt.Errorf("parsing annual provisions: %w", err)
	}
	return value, nil
}

// QueryCommunityTax fetches the distribution community tax ratio.
func (c *RESTClient) QueryCommunityTax() (float64, error) {
	var resp DistributionParamsResponse
	if err := c.get("/cosmos/distribution/v1beta1/params", &resp); err != nil {
		return 0, fmt.Errorf("querying distribution params: %w", err)
	}
	value, err := parseRatio(resp.Params.CommunityTax)
	if err != nil {
		return 0, fmt.Errorf("parsing community tax: %w", err)
	}
	return value, nil
}

func parseUAtom(raw string) (float64, error) {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return float64(value), nil
}

func parseNonNegativeDecimal(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, fmt.Errorf("invalid non-negative decimal %q", raw)
	}
	return value, nil
}

func parseRatio(raw string) (float64, error) {
	value, err := parseNonNegativeDecimal(raw)
	if err != nil || value > 1 {
		return 0, fmt.Errorf("invalid ratio %q", raw)
	}
	return value, nil
}

// parseMinSignedBlocks reproduces the Cosmos SDK's decimal multiplication and
// RoundInt64 behavior without converting the product through binary floating point.
func parseMinSignedBlocks(raw string, window int64) (float64, int64, error) {
	ratio, ok := new(big.Rat).SetString(raw)
	if !ok || ratio.Sign() < 0 || ratio.Cmp(big.NewRat(1, 1)) > 0 {
		return 0, 0, fmt.Errorf("invalid ratio %q", raw)
	}
	floatRatio, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(floatRatio) || math.IsInf(floatRatio, 0) {
		return 0, 0, fmt.Errorf("invalid ratio %q", raw)
	}

	product := new(big.Rat).Mul(ratio, new(big.Rat).SetInt64(window))
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.QuoRem(product.Num(), product.Denom(), remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(product.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, 0, fmt.Errorf("rounded signed-block minimum is out of int64 range")
	}
	return floatRatio, quotient.Int64(), nil
}

// ErrNoUpgradePlan reports that no chain upgrade is currently scheduled, which is
// the normal state rather than a failure.
var ErrNoUpgradePlan = errors.New("no upgrade plan scheduled")

// QueryUpgradePlan fetches the current upgrade plan, returning ErrNoUpgradePlan
// when none is scheduled.
func (c *RESTClient) QueryUpgradePlan() (*UpgradePlanDetail, error) {
	var resp UpgradePlanResponse
	if err := c.get("/cosmos/upgrade/v1beta1/current_plan", &resp); err != nil {
		return nil, fmt.Errorf("querying upgrade plan: %w", err)
	}
	if resp.Plan == nil {
		return nil, ErrNoUpgradePlan
	}
	return &UpgradePlanDetail{
		Name:   resp.Plan.Name,
		Height: resp.Plan.Height,
	}, nil
}

// ChainStatus is a CometBFT node's view of the chain tip.
type ChainStatus struct {
	ChainID   string
	Height    int64
	BlockTime time.Time
}

// QueryCometStatus fetches node status from the CometBFT RPC endpoint.
func (c *RPCClient) QueryCometStatus() (ChainStatus, error) {
	var status CometStatusResponse
	if err := c.get("/status", &status); err != nil {
		return ChainStatus{}, fmt.Errorf("querying comet status: %w", err)
	}

	height, err := strconv.ParseInt(status.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil || height <= 0 {
		return ChainStatus{}, fmt.Errorf("invalid latest block height %q", status.Result.SyncInfo.LatestBlockHeight)
	}
	blockTime, err := time.Parse(time.RFC3339Nano, status.Result.SyncInfo.LatestBlockTime)
	if err != nil {
		return ChainStatus{}, fmt.Errorf("invalid latest block time %q: %w", status.Result.SyncInfo.LatestBlockTime, err)
	}
	return ChainStatus{
		ChainID:   status.Result.NodeInfo.Network,
		Height:    height,
		BlockTime: blockTime,
	}, nil
}
