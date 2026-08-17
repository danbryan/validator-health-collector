package collector

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	c := &RESTClient{client: &http.Client{Timeout: restTimeout}}
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
	c := &RPCClient{client: &http.Client{Timeout: restTimeout}}
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
	var all []Validator
	var nextKey string

	for {
		path := "/cosmos/staking/v1beta1/validators?status=BOND_STATUS_BONDED&pagination.limit=100"
		if nextKey != "" {
			path += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		var resp ValidatorsResponse
		if err := c.get(path, &resp); err != nil {
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
	bonded, err := strconv.ParseFloat(resp.Pool.BondedTokens, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing bonded tokens: %w", err)
	}
	return bonded, nil
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
	set := make(map[string]bool)

	for page := 1; ; page++ {
		var cv CometValidatorsResponse
		path := fmt.Sprintf("/validators?per_page=100&page=%d", page)
		if err := c.get(path, &cv); err != nil {
			return nil, fmt.Errorf("querying consensus set: %w", err)
		}
		for _, v := range cv.Result.Validators {
			set[strings.ToUpper(v.Address)] = true
		}

		total, _ := strconv.Atoi(cv.Result.Total)
		if len(set) >= total || len(cv.Result.Validators) == 0 {
			break
		}
	}
	return set, nil
}

// SlashingParams holds the on-chain slashing configuration.
type SlashingParams struct {
	SignedBlocksWindow    int64
	MinSignedPerWindow    float64
	MinSignedBlocks       int64 // window * minSignedPerWindow
	MaxMissedBlocks       int64 // window - minSignedBlocks; jail past this
	DowntimeJailDuration  string
	SlashFractionDowntime float64
}

// QuerySlashingParams fetches the on-chain slashing parameters. These drive
// every jail calculation, so they are read from the chain rather than assumed.
func (c *RESTClient) QuerySlashingParams() (*SlashingParams, error) {
	var resp SlashingParamsResponse
	if err := c.get("/cosmos/slashing/v1beta1/params", &resp); err != nil {
		return nil, fmt.Errorf("querying slashing params: %w", err)
	}
	window, err := strconv.ParseInt(resp.Params.SignedBlocksWindow, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing signed_blocks_window: %w", err)
	}
	minRatio, err := strconv.ParseFloat(resp.Params.MinSignedPerWindow, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing min_signed_per_window: %w", err)
	}
	slashFrac, _ := strconv.ParseFloat(resp.Params.SlashFractionDowntime, 64)

	minSigned := int64(float64(window) * minRatio)
	return &SlashingParams{
		SignedBlocksWindow:    window,
		MinSignedPerWindow:    minRatio,
		MinSignedBlocks:       minSigned,
		MaxMissedBlocks:       window - minSigned,
		DowntimeJailDuration:  resp.Params.DowntimeJailDuration,
		SlashFractionDowntime: slashFrac,
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
		h, _ := strconv.ParseInt(bh.Result.Block.Header.Height, 10, 64)
		t, err := time.Parse(time.RFC3339Nano, bh.Result.Block.Header.Time)
		return h, t, err
	}

	hLatest, tLatest, err := fetch(0)
	if err != nil {
		return 0, err
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

// QueryRecentProposals fetches the most recent N proposals plus any currently
// in the voting period, so the dashboard selector reflects live chain state
// instead of a hardcoded list.
func (c *RESTClient) QueryRecentProposals(limit int) ([]Proposal, error) {
	path := fmt.Sprintf("/cosmos/gov/v1/proposals?pagination.reverse=true&pagination.limit=%d", limit)
	var resp ProposalsResponse
	if err := c.get(path, &resp); err != nil {
		return nil, fmt.Errorf("querying recent proposals: %w", err)
	}
	return resp.Proposals, nil
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

// QueryProposalByID fetches a single proposal by ID.
func (c *RESTClient) QueryProposalByID(id string) (*Proposal, error) {
	var resp ProposalResponse
	if err := c.get("/cosmos/gov/v1/proposals/"+id, &resp); err != nil {
		return nil, fmt.Errorf("querying proposal %s: %w", id, err)
	}
	return &resp.Proposal, nil
}

// QueryActiveProposals fetches proposals in voting period.
func (c *RESTClient) QueryActiveProposals() ([]Proposal, error) {
	var all []Proposal
	var nextKey string

	for {
		path := "/cosmos/gov/v1/proposals?pagination.limit=100"
		if nextKey != "" {
			path += "&pagination.key=" + url.QueryEscape(nextKey)
		}

		var resp ProposalsResponse
		if err := c.get(path, &resp); err != nil {
			return nil, fmt.Errorf("querying proposals: %w", err)
		}
		// Filter client-side since the REST API status filter may not work
		for _, p := range resp.Proposals {
			if p.Status == statusVotingPeriod {
				all = append(all, p)
			}
		}
		if resp.Pagination.NextKey == "" || resp.Pagination.NextKey == nullNextKey {
			break
		}
		nextKey = resp.Pagination.NextKey
	}
	return all, nil
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

	height, _ := strconv.ParseInt(status.Result.SyncInfo.LatestBlockHeight, 10, 64)
	blockTime, _ := time.Parse(time.RFC3339, status.Result.SyncInfo.LatestBlockTime)
	return ChainStatus{
		ChainID:   status.Result.NodeInfo.Network,
		Height:    height,
		BlockTime: blockTime,
	}, nil
}
