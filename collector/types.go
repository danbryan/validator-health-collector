package collector

import "time"

// Status strings and pagination sentinels returned by the Cosmos REST API.
const (
	statusVotingPeriod  = "PROPOSAL_STATUS_VOTING_PERIOD"
	statusDepositPeriod = "PROPOSAL_STATUS_DEPOSIT_PERIOD"

	// nullNextKey is what some REST implementations return instead of an empty
	// pagination key, so both spellings have to end the loop.
	nullNextKey = "null"
)

// ValidatorDescription is the operator-supplied metadata on a validator.
type ValidatorDescription struct {
	Moniker  string `json:"moniker"`
	Identity string `json:"identity"`
}

// CommissionRates holds the commission a validator charges.
type CommissionRates struct {
	Rate string `json:"rate"`
}

// Commission wraps the commission rates as the staking module returns them.
type Commission struct {
	CommissionRates CommissionRates `json:"commission_rates"`
}

// Validator is one entry from the staking module's validator set.
type Validator struct {
	OperatorAddress string               `json:"operator_address"`
	ConsensusPubKey map[string]any       `json:"consensus_pubkey"`
	Jailed          bool                 `json:"jailed"`
	Status          string               `json:"status"`
	Tokens          string               `json:"tokens"`
	Description     ValidatorDescription `json:"description"`
	Commission      Commission           `json:"commission"`
}

// StakingPool is the bonded and unbonded token supply.
type StakingPool struct {
	BondedTokens    string `json:"bonded_tokens"`
	NotBondedTokens string `json:"not_bonded_tokens"`
}

// PoolResponse wraps the staking pool query.
type PoolResponse struct {
	Pool StakingPool `json:"pool"`
}

// TallyResult is a proposal's vote tally in raw token amounts.
type TallyResult struct {
	YesCount        string `json:"yes_count"`
	AbstainCount    string `json:"abstain_count"`
	NoCount         string `json:"no_count"`
	NoWithVetoCount string `json:"no_with_veto_count"`
}

// Proposal is one governance proposal as the gov module reports it.
type Proposal struct {
	ID               string      `json:"id"`
	Status           string      `json:"status"`
	Title            string      `json:"title"`
	VotingStartTime  string      `json:"voting_start_time"`
	VotingEndTime    string      `json:"voting_end_time"`
	FinalTallyResult TallyResult `json:"final_tally_result"`
}

// TallyResponse wraps the running-tally endpoint used for open proposals.
type TallyResponse struct {
	Tally TallyResult `json:"tally"`
}

// ProposalVoteOption is one weighted choice in the governance REST response.
type ProposalVoteOption struct {
	Option string `json:"option"`
	Weight string `json:"weight"`
}

// ProposalVote is the current vote recorded for one account.
type ProposalVote struct {
	ProposalID string               `json:"proposal_id"`
	Voter      string               `json:"voter"`
	Options    []ProposalVoteOption `json:"options"`
}

// ProposalVotesResponse wraps the paginated current-votes endpoint.
type ProposalVotesResponse struct {
	Votes      []ProposalVote `json:"votes"`
	Pagination Pagination     `json:"pagination"`
}

// GovTallyParams are the governance thresholds that decide a proposal's outcome.
type GovTallyParams struct {
	Quorum        string `json:"quorum"`
	Threshold     string `json:"threshold"`
	VetoThreshold string `json:"veto_threshold"`
}

// GovParamsResponse wraps the tallying-params query.
type GovParamsResponse struct {
	Params GovTallyParams `json:"params"`
}

// SigningInfo is one validator's slashing-module signing record.
type SigningInfo struct {
	Address             string `json:"address"`
	MissedBlocksCounter string `json:"missed_blocks_counter"`
}

// SlashingParamsRaw is the slashing configuration as strings from the API.
type SlashingParamsRaw struct {
	SignedBlocksWindow    string `json:"signed_blocks_window"`
	MinSignedPerWindow    string `json:"min_signed_per_window"`
	DowntimeJailDuration  string `json:"downtime_jail_duration"`
	SlashFractionDowntime string `json:"slash_fraction_downtime"`
}

// SlashingParamsResponse wraps the slashing-params query.
type SlashingParamsResponse struct {
	Params SlashingParamsRaw `json:"params"`
}

// UpgradePlanDetail is the scheduled chain upgrade, if any.
type UpgradePlanDetail struct {
	Name   string `json:"name"`
	Height string `json:"height"`
}

// UpgradePlanResponse wraps the current-plan query. Plan is nil when no upgrade
// is scheduled, which is the normal state.
type UpgradePlanResponse struct {
	Plan *UpgradePlanDetail `json:"plan"`
}

// ProposalResponse wraps a single-proposal query.
type ProposalResponse struct {
	Proposal Proposal `json:"proposal"`
}

// Pagination is the cursor the Cosmos REST API returns for list queries.
type Pagination struct {
	NextKey string `json:"next_key"`
	Total   string `json:"total"`
}

// ValidatorsResponse wraps a paginated validator list.
type ValidatorsResponse struct {
	Validators []Validator `json:"validators"`
	Pagination Pagination  `json:"pagination"`
}

// ProposalsResponse wraps a paginated proposal list.
type ProposalsResponse struct {
	Proposals  []Proposal `json:"proposals"`
	Pagination Pagination `json:"pagination"`
}

// SigningInfosResponse wraps a paginated signing-info list.
type SigningInfosResponse struct {
	Info       []SigningInfo `json:"info"`
	Pagination Pagination    `json:"pagination"`
}

// CometNodeInfo identifies the chain a CometBFT node is following.
type CometNodeInfo struct {
	Network string `json:"network"`
}

// CometSyncInfo is a CometBFT node's view of the chain tip and its own history.
type CometSyncInfo struct {
	LatestBlockHeight   string `json:"latest_block_height"`
	LatestBlockTime     string `json:"latest_block_time"`
	EarliestBlockHeight string `json:"earliest_block_height"`
	CatchingUp          bool   `json:"catching_up"`
}

// CometStatusResult is the body of a CometBFT /status response.
type CometStatusResult struct {
	NodeInfo CometNodeInfo `json:"node_info"`
	SyncInfo CometSyncInfo `json:"sync_info"`
}

// CometStatusResponse wraps a CometBFT /status response.
type CometStatusResponse struct {
	Result CometStatusResult `json:"result"`
}

// CometValidator is one entry in a CometBFT /validators page. Only the consensus
// address is needed, to decide who is seated in the active set.
type CometValidator struct {
	Address string `json:"address"`
}

// CometValidatorsResult is the body of a CometBFT /validators response.
type CometValidatorsResult struct {
	Total      string           `json:"total"`
	Validators []CometValidator `json:"validators"`
}

// CometValidatorsResponse wraps a CometBFT /validators response.
type CometValidatorsResponse struct {
	Result CometValidatorsResult `json:"result"`
}

// BlockHeader is the subset of a block header the collector reads.
type BlockHeader struct {
	Height string `json:"height"`
	Time   string `json:"time"`
}

// Block wraps a block header.
type Block struct {
	Header BlockHeader `json:"header"`
}

// BlockResult is the body of a CometBFT /block response.
type BlockResult struct {
	Block Block `json:"block"`
}

// BlockResponse wraps a CometBFT /block response.
type BlockResponse struct {
	Result BlockResult `json:"result"`
}

// Snapshot is one complete reading of validator-set health.
type Snapshot struct {
	Timestamp       time.Time
	BlockHeight     int64
	ChainID         string
	Validators      []Validator
	BondedTokens    float64
	ActiveProposals []Proposal
	GovQuorum       float64
	GovThreshold    float64
	SigningInfos    []SigningInfo
	UpgradePlan     *UpgradePlanDetail
	EntityMap       map[string]string
	EndpointREST    string
	EndpointRPC     string
}
