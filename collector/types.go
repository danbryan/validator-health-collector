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
	JailedUntil         string `json:"jailed_until"`
	Tombstoned          bool   `json:"tombstoned"`
	MissedBlocksCounter string `json:"missed_blocks_counter"`
}

// SlashingParamsRaw is the slashing configuration as strings from the API.
type SlashingParamsRaw struct {
	SignedBlocksWindow      string `json:"signed_blocks_window"`
	MinSignedPerWindow      string `json:"min_signed_per_window"`
	DowntimeJailDuration    string `json:"downtime_jail_duration"`
	SlashFractionDoubleSign string `json:"slash_fraction_double_sign"`
	SlashFractionDowntime   string `json:"slash_fraction_downtime"`
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

// Coin is an SDK coin returned by staking delegation queries.
type Coin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

// Delegation identifies a delegator's position with one validator.
type Delegation struct {
	DelegatorAddress string `json:"delegator_address"`
	ValidatorAddress string `json:"validator_address"`
	Shares           string `json:"shares"`
}

// DelegationResponse combines delegation metadata with its current token balance.
type DelegationResponse struct {
	Delegation Delegation `json:"delegation"`
	Balance    Coin       `json:"balance"`
}

// DelegationsResponse wraps a paginated delegator query.
type DelegationsResponse struct {
	DelegationResponses []DelegationResponse `json:"delegation_responses"`
	Pagination          Pagination           `json:"pagination"`
}

// ManagedDelegation is a positive uatom delegation used by managed-account scans.
type ManagedDelegation struct {
	OperatorAddress string
	AmountUAtom     float64
}

// Redelegation identifies one source-to-destination redelegation group.
type Redelegation struct {
	DelegatorAddress string `json:"delegator_address"`
	SourceValidator  string `json:"validator_src_address"`
	DestValidator    string `json:"validator_dst_address"`
}

// RedelegationEntry contains a chain-returned redelegation completion time.
type RedelegationEntry struct {
	CompletionTime string `json:"completion_time"`
}

// RedelegationEntryResponse is the staking module's calculated entry balance.
type RedelegationEntryResponse struct {
	Entry   RedelegationEntry `json:"redelegation_entry"`
	Balance string            `json:"balance"`
}

// RedelegationResponse combines a route with its current chain-returned entries.
type RedelegationResponse struct {
	Redelegation Redelegation                `json:"redelegation"`
	Entries      []RedelegationEntryResponse `json:"entries"`
}

// RedelegationsResponse wraps paginated receiving redelegations for a delegator.
type RedelegationsResponse struct {
	RedelegationResponses []RedelegationResponse `json:"redelegation_responses"`
	Pagination            Pagination             `json:"pagination"`
}

// ReceivingRedelegation is one parsed entry still returned by current chain state.
type ReceivingRedelegation struct {
	DelegatorAddress    string
	DestinationOperator string
	BalanceUAtom        float64
	CompletionTime      time.Time
}

// AnnualProvisionsResponse wraps the mint module's annual issuance estimate.
type AnnualProvisionsResponse struct {
	AnnualProvisions string `json:"annual_provisions"`
}

// DistributionParams contains the community-pool tax applied to rewards.
type DistributionParams struct {
	CommunityTax string `json:"community_tax"`
}

// DistributionParamsResponse wraps distribution module parameters.
type DistributionParamsResponse struct {
	Params DistributionParams `json:"params"`
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
	BlockHeight string           `json:"block_height"`
	Total       string           `json:"total"`
	Validators  []CometValidator `json:"validators"`
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

// CometCommitSignature records one validator's signature state in a commit.
type CometCommitSignature struct {
	BlockIDFlag      int    `json:"block_id_flag"`
	ValidatorAddress string `json:"validator_address"`
}

// CometCommit is the subset of a signed commit needed for recent signing checks.
type CometCommit struct {
	Height     string                 `json:"height"`
	Signatures []CometCommitSignature `json:"signatures"`
}

// CometSignedHeader wraps the commit returned by CometBFT.
type CometSignedHeader struct {
	Commit CometCommit `json:"commit"`
}

// CometCommitResult is the body of a CometBFT /commit response.
type CometCommitResult struct {
	SignedHeader CometSignedHeader `json:"signed_header"`
}

// CometCommitResponse wraps a CometBFT /commit response.
type CometCommitResponse struct {
	Result CometCommitResult `json:"result"`
}

// CommitSigners is the uppercase hex consensus-address set that signed a commit.
type CommitSigners map[string]bool

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
