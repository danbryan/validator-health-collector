package collector

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cosmos/platform/apps/validator-health-collector/endpoints"
)

// consensusAddressFromPubKey derives a cosmosvalcons address from a base64-encoded ed25519 consensus pubkey.
func consensusAddressFromPubKey(pubKeyBase64 string) (string, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKeyBase64)
	if err != nil {
		return "", err
	}
	pk := &ed25519.PubKey{Key: pubKeyBytes}
	addr := pk.Address()
	bech, err := bech32.ConvertAndEncode("cosmosvalcons", addr)
	if err != nil {
		return "", err
	}
	return bech, nil
}

// consensusHexFromPubKey derives the uppercase hex consensus address that
// CometBFT uses to identify a validator in the consensus set.
func consensusHexFromPubKey(pubKeyBase64 string) (string, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKeyBase64)
	if err != nil {
		return "", err
	}
	pk := &ed25519.PubKey{Key: pubKeyBytes}
	return strings.ToUpper(hex.EncodeToString(pk.Address())), nil
}

// defaultSecondsPerBlock is used only until the real interval has been measured
// from the chain, which happens on every cycle before governance runs. Cosmos Hub
// has sat near six seconds for years, so it is a safe starting point rather than
// an assumption the metrics depend on.
const defaultSecondsPerBlock = 6.0

// blockIntervalOrDefault returns the measured block interval, falling back to the
// nominal one before the first successful measurement.
func (c *Collector) blockIntervalOrDefault() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lastBlockTime > 0 {
		return c.lastBlockTime
	}
	return defaultSecondsPerBlock
}

// boolGauge maps a boolean onto the 1/0 a Prometheus gauge expects.
func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// pubKeyOf pulls the base64 ed25519 consensus key out of a validator record.
func pubKeyOf(v Validator) string {
	if v.ConsensusPubKey == nil {
		return ""
	}
	t, ok := v.ConsensusPubKey["@type"].(string)
	if !ok || t != "/cosmos.crypto.ed25519.PubKey" {
		return ""
	}
	k, ok := v.ConsensusPubKey["key"].(string)
	if !ok {
		return ""
	}
	return k
}

const (
	// JailWindow is the sliding window for missed block tracking (on-chain parameter).
	JailWindow = 10000
	// MinSignedPerWindow is the minimum fraction of blocks that must be signed (on-chain: 0.05 = 5%).
	MinSignedPerWindow = 0.05
	// MaxMissedBeforeJail is the maximum missed blocks before jailing: 10000 - (10000 * 0.05) = 9500.
	MaxMissedBeforeJail = 9500
	// MissedBlocksAlertThreshold is the missed block count where we start alerting (20% of window = ~3 hours).
	MissedBlocksAlertThreshold = 2000
	// QuorumBufferPoints is the desired-state margin above the on-chain quorum
	// parameter: turnout should clear quorum by at least 10 percentage points.
	QuorumBufferPoints = 0.10
)

// Metrics holds all Prometheus gauges for the validator health collector.
type Metrics struct {
	LastSuccess            prometheus.Gauge
	QuerySuccess           *prometheus.GaugeVec
	BlockHeight            prometheus.Gauge
	ActiveValidators       prometheus.Gauge
	LargestValShare        prometheus.Gauge
	LargestEntShare        prometheus.Gauge
	HaltCoeff              prometheus.Gauge
	SafetyCoeff            prometheus.Gauge
	GovTurnout             *prometheus.GaugeVec
	GovQuorum              *prometheus.GaugeVec
	GovSecondsRemaining    *prometheus.GaugeVec
	ValMissedBlocks        *prometheus.GaugeVec
	ValMissedBlocksInfo    *prometheus.GaugeVec
	ValMissedRatio         *prometheus.GaugeVec
	ValBlocksToJail        *prometheus.GaugeVec
	ValSecondsToJail       *prometheus.GaugeVec
	ValAtRiskPower         *prometheus.GaugeVec
	SlashingParam          *prometheus.GaugeVec
	ChainBlockTime         prometheus.Gauge
	ValJailed              *prometheus.GaugeVec
	ValInfo                *prometheus.GaugeVec
	EntityShare            *prometheus.GaugeVec
	GovProposalInfo        *prometheus.GaugeVec
	GovProposalQuorum      *prometheus.GaugeVec
	GovProposalTally       *prometheus.GaugeVec
	GovEntityVote          *prometheus.GaugeVec
	GovNonVoterPower       *prometheus.GaugeVec
	GovTurnoutTimeline     *prometheus.GaugeVec
	GovWindow              *prometheus.GaugeVec
	GovParticipationCount  *prometheus.GaugeVec
	GovVetoThreshold       prometheus.Gauge
	EntityCanVetoAlone     *prometheus.GaugeVec
	AnyEntityCanVeto       prometheus.Gauge
	VetoPowerNeeded        prometheus.Gauge
	GovQuorumBuffer        *prometheus.GaugeVec
	GovQuorumTarget        *prometheus.GaugeVec
	UpgradeHeight          *prometheus.GaugeVec
	BondedTokens           prometheus.Gauge
	GovAttributionComplete *prometheus.GaugeVec
	EndpointInfo           *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		LastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_collector_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful snapshot collection.",
		}),
		QuerySuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_collector_query_success",
			Help: "Whether the last query for a protocol succeeded (1) or failed (0).",
		}, []string{"protocol"}),
		BlockHeight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_snapshot_block_height",
			Help: "Latest block height from the snapshot.",
		}),
		ActiveValidators: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_active_validators",
			Help: "Number of active (bonded) validators.",
		}),
		LargestValShare: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_largest_validator_share_ratio",
			Help: "Voting power share of the largest single validator (0-1).",
		}),
		LargestEntShare: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_largest_entity_share_ratio",
			Help: "Voting power share of the largest entity (0-1).",
		}),
		HaltCoeff: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_halt_coefficient",
			Help: "Number of entities needed to reach 1/3 of voting power (halt threshold).",
		}),
		SafetyCoeff: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_safety_coefficient",
			Help: "Number of entities needed to reach 2/3 of voting power (safety threshold).",
		}),
		GovTurnout: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_turnout_ratio",
			Help: "Governance proposal turnout ratio (0-1).",
		}, []string{"proposal_id"}),
		GovQuorum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_quorum_ratio",
			Help: "Governance proposal quorum ratio (0-1).",
		}, []string{"proposal_id"}),
		GovSecondsRemaining: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_seconds_remaining",
			Help: "Seconds remaining until voting ends for a proposal.",
		}, []string{"proposal_id"}),
		ValMissedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_missed_blocks",
			Help: "Missed blocks counter per validator consensus address.",
		}, []string{"validator"}),
		ValMissedBlocksInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_missed_blocks_info",
			Help: "Missed blocks in the current signing window for active-set entities above the alert threshold.",
		}, []string{"entity"}),
		ValMissedRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_missed_ratio",
			Help: "Missed blocks as a fraction of the signing window (0-1).",
		}, []string{"entity"}),
		ValBlocksToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_blocks_to_jail",
			Help: "Blocks a validator can still miss in the current window before being jailed.",
		}, []string{"entity"}),
		ValSecondsToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_seconds_to_jail",
			Help: "Estimated seconds until jailing if the validator keeps missing every block, using the measured block interval.",
		}, []string{"entity"}),
		ValAtRiskPower: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_at_risk_power_ratio",
			Help: "Voting power share held by an entity at risk of being jailed (0-1).",
		}, []string{"entity"}),
		SlashingParam: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_slashing_param",
			Help: "On-chain slashing parameters driving the jail calculation.",
		}, []string{"param"}),
		ChainBlockTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_chain_block_time_seconds",
			Help: "Measured average block interval in seconds.",
		}),
		ValJailed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_jailed",
			Help: "Whether a validator is jailed (1) or not (0).",
		}, []string{"validator"}),
		ValInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_info",
			Help: "Validator metadata mapping consensus address to moniker and operator address.",
		}, []string{"consensus_address", "moniker", "operator_address"}),
		EntityShare: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_entity_share_ratio",
			Help: "Voting power share per entity (0-1), labeled with the validators that make up the entity.",
		}, []string{"entity", "validators"}),
		GovProposalInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_info",
			Help: "Final turnout ratio for a proposal, labeled with title and outcome.",
		}, []string{"proposal_id", "title", "status"}),
		GovProposalQuorum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_quorum",
			Help: "Quorum requirement for a proposal (0-1).",
		}, []string{"proposal_id"}),
		GovProposalTally: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_tally_ratio",
			Help: "Share of bonded stake per vote option for a proposal (0-1).",
		}, []string{"proposal_id", "option"}),
		GovEntityVote: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_entity_vote_power",
			Help: "Voting power per entity on a proposal, labeled with the option chosen.",
		}, []string{"proposal_id", "entity", "option"}),
		GovNonVoterPower: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_non_voter_power_ratio",
			Help: "Share of bonded stake held by entities that did not vote on a proposal (0-1).",
		}, []string{"proposal_id", "entity"}),
		GovTurnoutTimeline: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_turnout_by_day",
			Help: "Cumulative turnout ratio at each calendar date of the voting window (0-1).",
		}, []string{"proposal_id", "date", "unixtime"}),
		GovWindow: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_voting_window_seconds",
			Help: "Unix timestamps for the start and end of a proposal's voting window.",
		}, []string{"proposal_id", "boundary"}),
		GovParticipationCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_participation_count",
			Help: "Count of active-set validators by participation status on a proposal.",
		}, []string{"proposal_id", "status"}),
		GovVetoThreshold: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_governance_veto_threshold",
			Help: "On-chain veto threshold as a fraction of non-abstain votes cast (0-1).",
		}),
		EntityCanVetoAlone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_entity_can_veto_alone",
			Help: "1 if this entity alone could veto a typical proposal, 0 otherwise.",
		}, []string{"entity"}),
		AnyEntityCanVeto: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_any_entity_can_veto_alone",
			Help: "1 if at least one entity could unilaterally veto a typical proposal, 0 if none can.",
		}),
		VetoPowerNeeded: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_veto_power_needed_ratio",
			Help: "Share of bonded stake a single entity needs to veto alone at observed turnout (0-1).",
		}),
		GovQuorumBuffer: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_quorum_buffer",
			Help: "Turnout minus the quorum requirement, in percentage points of bonded stake. Negative means quorum was not met.",
		}, []string{"proposal_id"}),
		GovQuorumTarget: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_quorum_target",
			Help: "Desired-state turnout target: the quorum parameter plus a 10 percentage-point buffer (0-1).",
		}, []string{"proposal_id"}),
		UpgradeHeight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_upgrade_height",
			Help: "Scheduled upgrade height per upgrade name.",
		}, []string{"upgrade"}),
		BondedTokens: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_bonded_tokens",
			Help: "Total bonded tokens (uatom).",
		}),
		GovAttributionComplete: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_vote_attribution_complete",
			Help: "1 when per-voter attribution for a proposal is trustworthy, 0 when no endpoint could serve its vote transactions.",
		}, []string{"proposal_id"}),
		EndpointInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_collector_endpoint_info",
			Help: "The endpoint the collector selected for each protocol, as a labelled constant 1.",
		}, []string{"protocol", "provider", "url"}),
	}
}

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.LastSuccess.Describe(ch)
	m.QuerySuccess.Describe(ch)
	m.BlockHeight.Describe(ch)
	m.ActiveValidators.Describe(ch)
	m.LargestValShare.Describe(ch)
	m.LargestEntShare.Describe(ch)
	m.HaltCoeff.Describe(ch)
	m.SafetyCoeff.Describe(ch)
	m.GovTurnout.Describe(ch)
	m.GovQuorum.Describe(ch)
	m.GovSecondsRemaining.Describe(ch)
	m.ValMissedBlocks.Describe(ch)
	m.ValMissedBlocksInfo.Describe(ch)
	m.ValMissedRatio.Describe(ch)
	m.ValBlocksToJail.Describe(ch)
	m.ValSecondsToJail.Describe(ch)
	m.ValAtRiskPower.Describe(ch)
	m.SlashingParam.Describe(ch)
	m.ChainBlockTime.Describe(ch)
	m.ValJailed.Describe(ch)
	m.ValInfo.Describe(ch)
	m.EntityShare.Describe(ch)
	m.GovProposalInfo.Describe(ch)
	m.GovProposalQuorum.Describe(ch)
	m.GovProposalTally.Describe(ch)
	m.GovEntityVote.Describe(ch)
	m.GovNonVoterPower.Describe(ch)
	m.GovTurnoutTimeline.Describe(ch)
	m.GovWindow.Describe(ch)
	m.GovParticipationCount.Describe(ch)
	m.GovVetoThreshold.Describe(ch)
	m.EntityCanVetoAlone.Describe(ch)
	m.AnyEntityCanVeto.Describe(ch)
	m.VetoPowerNeeded.Describe(ch)
	m.GovQuorumBuffer.Describe(ch)
	m.GovQuorumTarget.Describe(ch)
	m.UpgradeHeight.Describe(ch)
	m.BondedTokens.Describe(ch)
	m.GovAttributionComplete.Describe(ch)
	m.EndpointInfo.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.LastSuccess.Collect(ch)
	m.QuerySuccess.Collect(ch)
	m.BlockHeight.Collect(ch)
	m.ActiveValidators.Collect(ch)
	m.LargestValShare.Collect(ch)
	m.LargestEntShare.Collect(ch)
	m.HaltCoeff.Collect(ch)
	m.SafetyCoeff.Collect(ch)
	m.GovTurnout.Collect(ch)
	m.GovQuorum.Collect(ch)
	m.GovSecondsRemaining.Collect(ch)
	m.ValMissedBlocks.Collect(ch)
	m.ValMissedBlocksInfo.Collect(ch)
	m.ValMissedRatio.Collect(ch)
	m.ValBlocksToJail.Collect(ch)
	m.ValSecondsToJail.Collect(ch)
	m.ValAtRiskPower.Collect(ch)
	m.SlashingParam.Collect(ch)
	m.ChainBlockTime.Collect(ch)
	m.ValJailed.Collect(ch)
	m.ValInfo.Collect(ch)
	m.EntityShare.Collect(ch)
	m.GovProposalInfo.Collect(ch)
	m.GovProposalQuorum.Collect(ch)
	m.GovProposalTally.Collect(ch)
	m.GovEntityVote.Collect(ch)
	m.GovNonVoterPower.Collect(ch)
	m.GovTurnoutTimeline.Collect(ch)
	m.GovWindow.Collect(ch)
	m.GovParticipationCount.Collect(ch)
	m.GovVetoThreshold.Collect(ch)
	m.EntityCanVetoAlone.Collect(ch)
	m.AnyEntityCanVeto.Collect(ch)
	m.VetoPowerNeeded.Collect(ch)
	m.GovQuorumBuffer.Collect(ch)
	m.GovQuorumTarget.Collect(ch)
	m.UpgradeHeight.Collect(ch)
	m.BondedTokens.Collect(ch)
	m.GovAttributionComplete.Collect(ch)
	m.EndpointInfo.Collect(ch)
}

// Collector orchestrates the polling and metric updates.
type Collector struct {
	restClient *RESTClient
	rpcClient  *RPCClient
	resolver   *endpoints.Resolver
	entityMap  map[string]string
	metrics    *Metrics
	mu         sync.RWMutex

	govBackfiller           *GovBackfiller
	historicalProposalCount int

	lastSlashingParams *SlashingParams
	lastBlockTime      float64
	trackedProposals   map[string]bool
	observedTurnouts   map[string]float64
	lastQuorum         float64

	// Previous snapshot for detecting changes
	prevValidators map[string]bool // operator_address -> present
	firstRun       bool
}

// New builds a collector that discovers its endpoints through the resolver at the
// start of every collection cycle.
func New(resolver *endpoints.Resolver, entityMap map[string]string) *Collector {
	return &Collector{
		restClient:              NewRESTClient(),
		rpcClient:               NewRPCClient(),
		resolver:                resolver,
		entityMap:               entityMap,
		metrics:                 NewMetrics(),
		govBackfiller:           NewGovBackfiller(),
		historicalProposalCount: 6,
		trackedProposals:        make(map[string]bool),
		observedTurnouts:        make(map[string]float64),
		prevValidators:          make(map[string]bool),
		firstRun:                true,
	}
}

// resolveEndpoints re-probes the candidate endpoints and points the REST client,
// the CometBFT queries, and the governance backfiller at the healthy ones.
//
// This runs once per collection cycle rather than per request: an hourly poll
// should read a consistent view of the chain, and re-probing 30-odd endpoints on
// every individual query would be far more outbound traffic than the collector
// is meant to generate.
func (c *Collector) resolveEndpoints() error {
	res := c.resolver.Resolve()

	if len(res.RESTAddresses) == 0 {
		c.metrics.QuerySuccess.WithLabelValues("rest").Set(0)
		return fmt.Errorf("no healthy REST endpoint among %d candidates", len(res.REST))
	}
	if len(res.RPCAddresses) == 0 {
		c.metrics.QuerySuccess.WithLabelValues("rpc").Set(0)
		return fmt.Errorf("no healthy RPC endpoint among %d candidates", len(res.RPC))
	}

	c.restClient.SetEndpoints(res.RESTAddresses)
	c.rpcClient.SetEndpoints(res.RPCAddresses)
	c.govBackfiller.SetEndpoints(res.RPCAddresses)

	// Republish from scratch so a rotation does not leave the previous endpoint
	// reporting as current alongside the new one.
	c.metrics.EndpointInfo.Reset()
	c.publishEndpoint("rest", res.REST, c.restClient.BaseURL())
	c.publishEndpoint("rpc", res.RPC, c.rpcClient.BaseURL())
	c.publishEndpoint("txsearch", res.RPC, c.govBackfiller.RPCURL())

	return nil
}

func (c *Collector) publishEndpoint(protocol string, cands []endpoints.Candidate, address string) {
	if address == "" {
		return
	}
	provider := "unknown"
	if cand, ok := endpoints.Lookup(cands, address); ok && cand.Provider != "" {
		provider = cand.Provider
	}
	c.metrics.EndpointInfo.WithLabelValues(protocol, provider, address).Set(1)
}

func (c *Collector) Metrics() *Metrics {
	return c.metrics
}

// CollectSnapshot performs one full collection cycle.
func (c *Collector) CollectSnapshot() error {
	log.Println("Starting snapshot collection...")

	// 0. Endpoint discovery. Every later query depends on this, so a failure here
	// ends the cycle rather than producing a half-populated snapshot.
	if err := c.resolveEndpoints(); err != nil {
		log.Printf("WARN: endpoint resolution failed: %v", err)
		return err
	}

	// 1. CometBFT status
	status, err := c.rpcClient.QueryCometStatus()
	if err != nil {
		c.metrics.QuerySuccess.WithLabelValues("rpc").Set(0)
		log.Printf("WARN: CometBFT status failed: %v", err)
	} else {
		c.metrics.QuerySuccess.WithLabelValues("rpc").Set(1)
		c.metrics.BlockHeight.Set(float64(status.Height))
		log.Printf("Chain: %s, Height: %d, Time: %v", status.ChainID, status.Height, status.BlockTime)
	}

	// 2. Staking validators
	validators, err := c.restClient.QueryBondedValidators()
	if err != nil {
		c.metrics.QuerySuccess.WithLabelValues("rest").Set(0)
		log.Printf("WARN: Staking query failed: %v", err)
		return err
	}
	c.metrics.QuerySuccess.WithLabelValues("rest").Set(1)
	bondedCount := len(validators)

	// Narrow the bonded list to the validators CometBFT has actually seated in
	// the consensus set. On the Hub the staking module reports 200 bonded
	// validators (max_validators), but only the top 180 by voting power are in
	// the consensus set and signing blocks. The other 20 are bonded-but-inactive
	// and would otherwise inflate every set-health metric.
	// A failure here ends the cycle rather than falling back to the bonded list.
	// The two sets differ by 20 validators on the Hub, so quietly substituting one
	// for the other shifts the denominator behind every concentration metric and
	// every Nakamoto coefficient. Publishing those numbers with the wrong set is
	// worse than publishing nothing, and the rotating RPC client has already tried
	// several endpoints by this point.
	consensusSet, err := c.rpcClient.QueryConsensusSet()
	if err != nil {
		c.metrics.QuerySuccess.WithLabelValues("rpc").Set(0)
		return fmt.Errorf("consensus set query failed, abandoning the cycle rather than "+
			"reporting set-health metrics against the %d bonded validators: %w", bondedCount, err)
	}
	if len(consensusSet) == 0 {
		return fmt.Errorf("consensus set came back empty from %s", c.rpcClient.BaseURL())
	}

	active := validators[:0]
	for _, v := range validators {
		hexAddr, hexErr := consensusHexFromPubKey(pubKeyOf(v))
		if hexErr != nil || !consensusSet[hexAddr] {
			continue
		}
		active = append(active, v)
	}
	validators = active
	log.Printf("Active set: %d validators in consensus (%d bonded)", len(validators), bondedCount)

	// 3. Staking pool
	bondedTokens, err := c.restClient.QueryStakingPool()
	if err != nil {
		log.Printf("WARN: Pool query failed: %v", err)
	} else {
		c.metrics.BondedTokens.Set(bondedTokens)
		log.Printf("Bonded tokens: %.0f uatom", bondedTokens)
	}

	// 4. Governance
	activeProps, err := c.restClient.QueryActiveProposals()
	if err != nil {
		log.Printf("WARN: Governance query failed: %v", err)
	}
	govQuorum, _, govVeto, _ := c.restClient.QueryGovParams()
	c.metrics.GovVetoThreshold.Set(govVeto)
	if govQuorum > 0 {
		c.lastQuorum = govQuorum
	}

	// 5. Slashing parameters and block interval. Both drive the jail estimate,
	// so they are read from the chain rather than assumed.
	slashing, err := c.restClient.QuerySlashingParams()
	if err != nil {
		log.Printf("WARN: slashing params query failed, using last known values: %v", err)
		slashing = c.lastSlashingParams
	}
	if slashing == nil {
		log.Print("WARN: no slashing params available, skipping signing risk metrics this cycle")
		return nil
	}
	c.lastSlashingParams = slashing

	blockInterval, err := c.rpcClient.MeasureBlockTime(2000)
	if err != nil || blockInterval <= 0 {
		blockInterval = c.lastBlockTime
		if blockInterval <= 0 {
			blockInterval = 6.0 // conservative fallback until a real measurement lands
		}
	}
	c.lastBlockTime = blockInterval

	signingInfos, err := c.restClient.QuerySigningInfos()
	if err != nil {
		log.Printf("WARN: Signing infos query failed: %v", err)
	}

	// 6. Upgrade plan
	// No scheduled upgrade is the normal state, so it is not logged as a warning.
	upgrade, err := c.restClient.QueryUpgradePlan()
	if err != nil && !errors.Is(err, ErrNoUpgradePlan) {
		log.Printf("WARN: Upgrade plan query failed: %v", err)
	}

	// --- Compute and update metrics ---

	c.metrics.ActiveValidators.Set(float64(len(validators)))

	// Sort validators by tokens descending
	sort.Slice(validators, func(i, j int) bool {
		tI, _ := strconv.ParseFloat(validators[i].Tokens, 64)
		tJ, _ := strconv.ParseFloat(validators[j].Tokens, 64)
		return tI > tJ
	})

	// Per-validator metrics
	totalBonded := 0.0
	for _, v := range validators {
		t, _ := strconv.ParseFloat(v.Tokens, 64)
		totalBonded += t
	}

	if totalBonded > 0 && len(validators) > 0 {
		largestValShare := 0.0
		for _, v := range validators {
			t, _ := strconv.ParseFloat(v.Tokens, 64)
			share := t / totalBonded
			if share > largestValShare {
				largestValShare = share
			}
		}
		c.metrics.LargestValShare.Set(largestValShare)
		log.Printf("Largest validator share: %.4f", largestValShare)
	}

	// Entity grouping. Track the member validators of each entity so the
	// dashboard can show what a grouped entity is actually made of.
	entityPower := make(map[string]float64)
	entityMembers := make(map[string][]string)
	for _, v := range validators {
		t, _ := strconv.ParseFloat(v.Tokens, 64)
		entityName, ok := c.entityMap[v.OperatorAddress]
		if !ok {
			entityName = v.Description.Moniker // default: 1:1 mapping
		}
		entityMembers[entityName] = append(entityMembers[entityName],
			fmt.Sprintf("%s (%s)", v.Description.Moniker, v.OperatorAddress))
		entityPower[entityName] += t
	}

	// Sort entities by power descending
	type entityEntry struct {
		name  string
		power float64
	}
	var entities []entityEntry
	for name, power := range entityPower {
		entities = append(entities, entityEntry{name, power})
	}
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].power > entities[j].power
	})

	// Per-entity share metrics. Reset first so an entity that was merged into a
	// parent (or dropped out of the set) does not linger as a stale series.
	c.metrics.EntityShare.Reset()
	entityPowerShare := make(map[string]float64, len(entities))
	for _, e := range entities {
		share := e.power / totalBonded
		members := entityMembers[e.name]
		sort.Strings(members)
		// Only carry the member list for entities big enough to appear on the
		// dashboard; below that it is pure label cardinality for no benefit.
		memberLabel := ""
		if share >= 0.05 {
			memberLabel = strings.Join(members, " · ")
		}
		c.metrics.EntityShare.WithLabelValues(e.name, memberLabel).Set(share)
		entityPowerShare[e.name] = share
	}

	// Unilateral veto capability.
	//
	// The veto threshold is a share of votes CAST, not of bonded stake, so an
	// entity's raw voting-power share is not directly comparable to 33.4%.
	//
	// An entity casting a veto also adds its own stake to the denominator. With
	// C the entity's stake, O the stake everyone else casts, and v the veto
	// threshold, a veto succeeds when:
	//
	//     C / (C + O) > v      =>      C > O * v / (1-v)
	//
	// At v = 0.334 the multiplier is ~0.5, so an entity needs roughly half of
	// whatever everyone else votes. Turnout is what makes this reachable: when
	// only ~23% of stake participates, ~12% is enough to veto. The same entity
	// would need the full 33.4% only if every token voted.
	c.metrics.EntityCanVetoAlone.Reset()
	othersTurnout := c.referenceTurnout()
	anyCanVeto := 0.0
	if govVeto > 0 && govVeto < 1 && othersTurnout > 0 {
		vetoPowerNeeded := othersTurnout * govVeto / (1 - govVeto)
		c.metrics.VetoPowerNeeded.Set(vetoPowerNeeded)
		for _, e := range entities {
			share := e.power / totalBonded
			if share <= vetoPowerNeeded {
				continue
			}
			c.metrics.EntityCanVetoAlone.WithLabelValues(e.name).Set(1)
			anyCanVeto = 1
			// Resulting veto share if this entity vetoed at that turnout.
			resulting := share / (share + othersTurnout)
			log.Printf("VETO CAPABILITY: %s holds %.2f%% of bonded stake; needs %.2f%% at %.1f%% turnout. A veto would be %.1f%% of votes cast, above the %.1f%% threshold.",
				e.name, share*100, vetoPowerNeeded*100, othersTurnout*100, resulting*100, govVeto*100)
		}
	}
	c.metrics.AnyEntityCanVeto.Set(anyCanVeto)

	if totalBonded > 0 && len(entities) > 0 {
		largestEntShare := entities[0].power / totalBonded
		c.metrics.LargestEntShare.Set(largestEntShare)
		log.Printf("Largest entity share: %.4f (%s)", largestEntShare, entities[0].name)

		// Halt coefficient: entities needed to reach 1/3
		haltThreshold := totalBonded / 3.0
		cumulative := 0.0
		haltCount := 0
		for _, e := range entities {
			cumulative += e.power
			haltCount++
			if cumulative >= haltThreshold {
				break
			}
		}
		c.metrics.HaltCoeff.Set(float64(haltCount))
		log.Printf("Halt coefficient: %d", haltCount)

		// Safety coefficient: entities needed to reach 2/3
		safetyThreshold := totalBonded * 2.0 / 3.0
		cumulative = 0.0
		safetyCount := 0
		for _, e := range entities {
			cumulative += e.power
			safetyCount++
			if cumulative >= safetyThreshold {
				break
			}
		}
		c.metrics.SafetyCoeff.Set(float64(safetyCount))
		log.Printf("Safety coefficient: %d", safetyCount)
	}

	// Governance metrics
	for _, p := range activeProps {
		yes, _ := strconv.ParseFloat(p.FinalTallyResult.YesCount, 64)
		no, _ := strconv.ParseFloat(p.FinalTallyResult.NoCount, 64)
		abstain, _ := strconv.ParseFloat(p.FinalTallyResult.AbstainCount, 64)
		veto, _ := strconv.ParseFloat(p.FinalTallyResult.NoWithVetoCount, 64)
		totalVotes := yes + no + abstain + veto

		if totalBonded > 0 {
			turnout := totalVotes / totalBonded
			c.metrics.GovTurnout.WithLabelValues(p.ID).Set(turnout)
			c.metrics.GovQuorum.WithLabelValues(p.ID).Set(govQuorum)
		}

		endTime, err := time.Parse(time.RFC3339, p.VotingEndTime)
		if err == nil {
			remaining := time.Until(endTime).Seconds()
			if remaining < 0 {
				remaining = 0
			}
			c.metrics.GovSecondsRemaining.WithLabelValues(p.ID).Set(remaining)
		}
	}

	// Build consensus address -> moniker map from validator set
	consensusToMoniker := make(map[string]string)
	monikerToValoper := make(map[string]string, len(validators))
	for _, v := range validators {
		monikerToValoper[v.Description.Moniker] = v.OperatorAddress
	}
	for _, v := range validators {
		if v.ConsensusPubKey == nil {
			continue
		}
		pubKeyType, ok := v.ConsensusPubKey["@type"].(string)
		if !ok || pubKeyType != "/cosmos.crypto.ed25519.PubKey" {
			continue
		}
		keyStr, ok := v.ConsensusPubKey["key"].(string)
		if !ok || keyStr == "" {
			continue
		}
		consAddr, err := consensusAddressFromPubKey(keyStr)
		if err != nil {
			continue
		}
		consensusToMoniker[consAddr] = v.Description.Moniker
	}

	// Slashing metrics. Only active-set validators are published, and only the
	// ones already missing enough blocks to be worth looking at; below that the
	// counter is normal operating noise (restarts, brief maintenance).
	//
	// Everything here is keyed by entity. An entity is the entity-map name when
	// one exists, and the validator's own moniker otherwise, so the label means
	// the same thing on every panel.
	c.metrics.ValMissedBlocksInfo.Reset()
	c.metrics.ValMissedRatio.Reset()
	c.metrics.ValBlocksToJail.Reset()
	c.metrics.ValSecondsToJail.Reset()
	c.metrics.ValAtRiskPower.Reset()

	alertThreshold := float64(slashing.MaxMissedBlocks) * 0.2
	for _, info := range signingInfos {
		mb, _ := strconv.ParseFloat(info.MissedBlocksCounter, 64)
		c.metrics.ValMissedBlocks.WithLabelValues(info.Address).Set(mb)

		moniker, inActiveSet := consensusToMoniker[info.Address]
		if !inActiveSet || mb < alertThreshold {
			continue
		}
		entity := c.entityMap[monikerToValoper[moniker]]
		if entity == "" {
			entity = moniker
		}

		blocksToJail := float64(slashing.MaxMissedBlocks) - mb
		if blocksToJail < 0 {
			blocksToJail = 0
		}

		c.metrics.ValMissedBlocksInfo.WithLabelValues(entity).Set(mb)
		c.metrics.ValMissedRatio.WithLabelValues(entity).Set(mb / float64(slashing.SignedBlocksWindow))
		c.metrics.ValBlocksToJail.WithLabelValues(entity).Set(blocksToJail)
		// Worst case: the validator signs nothing from here on, so every
		// remaining block of headroom is consumed at the measured block rate.
		c.metrics.ValSecondsToJail.WithLabelValues(entity).Set(blocksToJail * blockInterval)
		// How much voting power leaves the set if this entity is jailed.
		if power, ok := entityPowerShare[entity]; ok {
			c.metrics.ValAtRiskPower.WithLabelValues(entity).Set(power)
		}
	}

	// Publish the parameters the jail math depends on so the dashboard can show
	// them rather than restating hardcoded numbers.
	c.metrics.SlashingParam.WithLabelValues("signed_blocks_window").Set(float64(slashing.SignedBlocksWindow))
	c.metrics.SlashingParam.WithLabelValues("min_signed_blocks").Set(float64(slashing.MinSignedBlocks))
	c.metrics.SlashingParam.WithLabelValues("max_missed_blocks").Set(float64(slashing.MaxMissedBlocks))
	c.metrics.SlashingParam.WithLabelValues("min_signed_per_window").Set(slashing.MinSignedPerWindow)
	c.metrics.SlashingParam.WithLabelValues("slash_fraction_downtime").Set(slashing.SlashFractionDowntime)
	c.metrics.ChainBlockTime.Set(blockInterval)

	// Validator info (moniker mapping)
	for _, v := range validators {
		c.metrics.ValInfo.WithLabelValues(
			v.OperatorAddress,
			v.Description.Moniker,
			v.OperatorAddress,
		).Set(1)
	}

	// Jailed status per validator
	for _, v := range validators {
		if v.Jailed {
			c.metrics.ValJailed.WithLabelValues(v.OperatorAddress).Set(1)
		} else {
			c.metrics.ValJailed.WithLabelValues(v.OperatorAddress).Set(0)
		}
	}

	// Upgrade plan
	if upgrade != nil {
		h, _ := strconv.ParseFloat(upgrade.Height, 64)
		c.metrics.UpgradeHeight.WithLabelValues(upgrade.Name).Set(h)
		log.Printf("Upgrade planned: %s at height %.0f", upgrade.Name, h)
	}

	// Active set change detection (skip on first run to avoid noise)
	currentSet := make(map[string]bool)
	for _, v := range validators {
		currentSet[v.OperatorAddress] = true
	}

	if !c.firstRun {
		for addr := range c.prevValidators {
			if !currentSet[addr] {
				log.Printf("Validator exit detected: %s", addr)
			}
		}
		for addr := range currentSet {
			if !c.prevValidators[addr] {
				log.Printf("Validator entry detected: %s", addr)
			}
		}
	}
	c.firstRun = false
	c.prevValidators = currentSet

	// Proposal analysis runs last because it is by far the most expensive step:
	// it reads paginated tx_search results per proposal. Everything above is a
	// handful of requests, so keeping governance at the end means the headline
	// concentration and coefficient metrics publish within seconds even when the
	// governance backfill is slow or an endpoint is rate limiting us.
	//
	// It also needs the block interval measured above to timestamp votes.
	//
	// Selection is dynamic: every currently-voting proposal, plus the most recent
	// closed ones for historical reference. A closed proposal's vote history is
	// immutable, so it is analyzed once and then skipped on later cycles.
	if bondedTokens > 0 {
		c.analyzeProposals(activeProps, validators, bondedTokens, govQuorum)
	}

	c.metrics.LastSuccess.Set(float64(time.Now().Unix()))
	log.Println("Snapshot collection complete.")
	return nil
}

// analyzeProposals discovers which proposals to report on, then publishes
// per-entity vote data and turnout timelines for each.
//
// Selection is dynamic: anything currently in its voting period is always
// analyzed and refreshed every cycle, and the most recent closed proposals are
// analyzed once for historical comparison.
func (c *Collector) analyzeProposals(
	activeProps []Proposal,
	validators []Validator,
	bondedTokens, quorum float64,
) {
	type target struct {
		prop   Proposal
		isLive bool
	}
	var targets []target
	seen := make(map[string]bool)

	// Live proposals first. These change while voting is open, so they are
	// always re-analyzed.
	for _, p := range activeProps {
		targets = append(targets, target{prop: p, isLive: true})
		seen[p.ID] = true
	}

	// Recent closed proposals for historical reference.
	recent, err := c.restClient.QueryRecentProposals(c.historicalProposalCount)
	if err != nil {
		log.Printf("WARN: recent proposal discovery failed: %v", err)
	}
	for _, p := range recent {
		if seen[p.ID] || p.Status == statusVotingPeriod {
			continue
		}
		// Deposit-period proposals have no vote history worth charting.
		if p.Status == statusDepositPeriod {
			continue
		}
		targets = append(targets, target{prop: p, isLive: false})
		seen[p.ID] = true
	}

	for _, t := range targets {
		// A closed proposal's tally and vote history never change, so analyzing
		// it once is enough. Live proposals are refreshed every cycle.
		if !t.isLive && c.trackedProposals[t.prop.ID] {
			continue
		}
		id := t.prop.ID
		prop := &t.prop

		analysis, err := c.govBackfiller.AnalyzeProposal(prop, validators, c.entityMap,
			bondedTokens, quorum, c.blockIntervalOrDefault(), c.restClient.QueryLiveTally)
		if err != nil {
			log.Printf("WARN: proposal %s analysis failed: %v", id, err)
			continue
		}

		title := analysis.Title
		if len(title) > 60 {
			title = title[:60]
		}

		c.metrics.GovProposalInfo.WithLabelValues(id, title, analysis.Status).Set(analysis.FinalTurnout)
		c.metrics.GovProposalQuorum.WithLabelValues(id).Set(quorum)

		// Whether per-voter attribution can be trusted for this proposal. The
		// tally and turnout above stay valid either way, because they come from
		// the chain's own tally rather than from indexed transactions.
		c.metrics.GovAttributionComplete.WithLabelValues(id).Set(boolGauge(analysis.AttributionComplete))
		if !analysis.AttributionComplete {
			log.Printf("WARN: proposal %s has a tally but no endpoint could attribute its votes, "+
				"so per-entity vote breakdowns are omitted for it", id)
		}

		// Desired state: turnout should clear quorum by at least 10 percentage
		// points. The target moves with the on-chain quorum parameter, so a
		// governance change to quorum automatically retargets the buffer.
		c.metrics.GovQuorumTarget.WithLabelValues(id).Set(quorum + QuorumBufferPoints)
		c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(analysis.FinalTurnout - quorum)

		// Feed observed turnout into the veto-capability denominator.
		//
		// Only completed proposals count. A proposal still in its voting period
		// has partial turnout that climbs toward its final value, and it is
		// re-analyzed every cycle, so recording it would both understate turnout
		// and add one sample per cycle until voting closes. Keyed by proposal so
		// a re-analysis replaces rather than duplicates.
		if !analysis.IsLive {
			c.mu.Lock()
			c.observedTurnouts[id] = analysis.FinalTurnout
			c.mu.Unlock()
		}

		// Tally breakdown as a share of bonded stake.
		//
		// Read from the analysis rather than from prop.FinalTallyResult, because a
		// proposal still in its voting period reports a zeroed final tally. Using
		// the raw field made the tally panels show 0 for a live proposal while the
		// turnout panel beside them showed a real number.
		c.metrics.GovProposalTally.WithLabelValues(id, "Yes").Set(analysis.Yes / bondedTokens)
		c.metrics.GovProposalTally.WithLabelValues(id, "No").Set(analysis.No / bondedTokens)
		c.metrics.GovProposalTally.WithLabelValues(id, "Abstain").Set(analysis.Abstain / bondedTokens)
		c.metrics.GovProposalTally.WithLabelValues(id, "No with veto").Set(analysis.Veto / bondedTokens)

		// Aggregate votes by entity so one entity's validators appear as one row.
		type entityVote struct {
			power  float64
			option string
		}
		byEntity := make(map[string]*entityVote)
		for _, v := range analysis.Voted {
			e, ok := byEntity[v.Entity]
			if !ok {
				byEntity[v.Entity] = &entityVote{power: v.VotingPower, option: v.Option}
				continue
			}
			e.power += v.VotingPower
		}
		for entity, ev := range byEntity {
			c.metrics.GovEntityVote.WithLabelValues(id, entity, ev.option).Set(ev.power / bondedTokens)
		}

		// Non-voters aggregated by entity, largest first. These are the entities
		// whose absence moved turnout toward or below quorum.
		nonVoterByEntity := make(map[string]float64)
		for _, v := range analysis.NonVoters {
			nonVoterByEntity[v.Entity] += v.VotingPower
		}
		for entity, power := range nonVoterByEntity {
			share := power / bondedTokens
			// Skip dust entities so the panel stays readable.
			if share < 0.0005 {
				continue
			}
			c.metrics.GovNonVoterPower.WithLabelValues(id, entity).Set(share)
		}

		c.metrics.GovParticipationCount.WithLabelValues(id, "Voted").Set(float64(len(byEntity)))
		c.metrics.GovParticipationCount.WithLabelValues(id, "Did not vote").Set(float64(len(nonVoterByEntity)))

		// Turnout timeline bucketed by calendar date of the voting window.
		c.metrics.GovWindow.WithLabelValues(id, "start").Set(float64(analysis.VotingStart.Unix()))
		c.metrics.GovWindow.WithLabelValues(id, "end").Set(float64(analysis.VotingEnd.Unix()))

		dailyMax := make(map[string]float64)
		for _, pt := range analysis.TurnoutTimeline {
			key := pt.Time.UTC().Format("2006-01-02")
			if pt.Cumulative > dailyMax[key] {
				dailyMax[key] = pt.Cumulative
			}
		}
		// Walk the window one calendar day at a time, carrying the running total
		// forward so the curve stays monotonic across days with no votes.
		var running float64
		endDay := analysis.VotingEnd.UTC().Truncate(24 * time.Hour)
		for d := analysis.VotingStart.UTC().Truncate(24 * time.Hour); !d.After(endDay); d = d.AddDate(0, 0, 1) {
			key := d.Format("2006-01-02")
			if v, ok := dailyMax[key]; ok && v > running {
				running = v
			}
			c.metrics.GovTurnoutTimeline.WithLabelValues(
				id, key, strconv.FormatInt(d.Unix(), 10),
			).Set(running)
		}

		c.trackedProposals[id] = true

		liveTag := ""
		if t.isLive {
			liveTag = " [voting open]"
		}
		log.Printf("Proposal %s (%s)%s: turnout %.2f%% vs quorum %.0f%%, %d entities voted, %d did not",
			id, analysis.Status, liveTag, analysis.FinalTurnout*100, quorum*100, len(byEntity), len(nonVoterByEntity))
	}
}

// referenceTurnout returns the participation level used as the denominator for
// veto-capability math.
//
// This uses the HIGHEST turnout observed on a completed proposal, not the
// median. Veto capability is a claim about what an entity could do on a real
// proposal, so it should hold against the best-participation case. Using a
// median makes the answer swing with whichever low-turnout proposals happen to
// be in the sample, and would flag entities that could only veto if turnout
// collapsed. An entity that clears the bar at peak turnout clears it at every
// lower turnout too, so this is the conservative choice.
//
// The quorum requirement is the floor: a proposal below quorum fails outright,
// so nobody needs to veto it.
func (c *Collector) referenceTurnout() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	highest := 0.0
	for _, v := range c.observedTurnouts {
		if v > highest {
			highest = v
		}
	}
	if highest < c.lastQuorum {
		return c.lastQuorum
	}
	return highest
}

// Run starts the polling loop. It runs one collection immediately, then on the given interval.
func (c *Collector) Run(interval time.Duration) {
	// Run immediately on start
	if err := c.CollectSnapshot(); err != nil {
		log.Printf("Initial collection failed: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := c.CollectSnapshot(); err != nil {
			log.Printf("Collection failed: %v", err)
		}
	}
}
