package collector

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/danbryan/validator-health-collector/cosmosaddr"
	"github.com/danbryan/validator-health-collector/endpoints"
)

// consensusAddressFromPubKey derives a cosmosvalcons address from a base64-encoded ed25519 consensus pubkey.
func consensusAddressFromPubKey(pubKeyBase64 string) (string, error) {
	addr, err := cosmosaddr.FromEd25519PubKey(pubKeyBase64)
	if err != nil {
		return "", err
	}
	return cosmosaddr.Encode("cosmosvalcons", addr)
}

// consensusHexFromPubKey derives the uppercase hex consensus address that
// CometBFT uses to identify a validator in the consensus set.
func consensusHexFromPubKey(pubKeyBase64 string) (string, error) {
	addr, err := cosmosaddr.FromEd25519PubKey(pubKeyBase64)
	if err != nil {
		return "", err
	}
	return cosmosaddr.HexAddress(addr), nil
}

// boolGauge maps a boolean onto the 1/0 a Prometheus gauge expects.
func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// blocksUntilJail reflects the SDK's strict missed > maxMissed jail check.
func blocksUntilJail(maxMissed int64, missed float64) float64 {
	remaining := float64(maxMissed) - missed + 1
	if remaining < 0 {
		return 0
	}
	return remaining
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
	// QuorumBufferPoints is the desired-state margin above the on-chain quorum
	// parameter: turnout should clear quorum by at least 10 percentage points.
	QuorumBufferPoints = 0.10
	// DefaultProposalHistoryWindow matches the shortest metrics retention in the
	// platform-dev monitoring stack: Mimir retains 14 days and Prometheus 15.
	DefaultProposalHistoryWindow = 14 * 24 * time.Hour
)

// Metrics holds all Prometheus gauges for the validator health collector.
type Metrics struct {
	LastSuccess                  prometheus.Gauge
	QuerySuccess                 *prometheus.GaugeVec
	BlockHeight                  prometheus.Gauge
	ActiveValidators             prometheus.Gauge
	LargestValShare              prometheus.Gauge
	LargestEntShare              prometheus.Gauge
	HaltCoeff                    prometheus.Gauge
	SafetyCoeff                  prometheus.Gauge
	GovTurnout                   *prometheus.GaugeVec
	GovQuorum                    *prometheus.GaugeVec
	GovSecondsRemaining          *prometheus.GaugeVec
	ValMissedBlocks              *prometheus.GaugeVec
	ValMissedBlocksInfo          *prometheus.GaugeVec
	ValMissedRatio               *prometheus.GaugeVec
	ValBlocksToJail              *prometheus.GaugeVec
	ValSecondsToJail             *prometheus.GaugeVec
	ValAtRiskPower               *prometheus.GaugeVec
	SlashingParam                *prometheus.GaugeVec
	ChainBlockTime               prometheus.Gauge
	ValJailed                    *prometheus.GaugeVec
	ValInfo                      *prometheus.GaugeVec
	EntityShare                  *prometheus.GaugeVec
	GovProposalInfo              *prometheus.GaugeVec
	GovProposalQuorum            *prometheus.GaugeVec
	GovProposalTally             *prometheus.GaugeVec
	GovEntityVote                *prometheus.GaugeVec
	GovProposalVetoRatio         *prometheus.GaugeVec
	GovProposalQuorumMet         *prometheus.GaugeVec
	GovProposalVetoState         *prometheus.GaugeVec
	GovEntityVetoCapability      *prometheus.GaugeVec
	GovNonVoterPower             *prometheus.GaugeVec
	GovWindow                    *prometheus.GaugeVec
	GovParticipationCount        *prometheus.GaugeVec
	GovVetoThreshold             prometheus.Gauge
	EntityCanVetoAlone           *prometheus.GaugeVec
	AnyEntityCanVeto             prometheus.Gauge
	VetoPowerNeeded              prometheus.Gauge
	GovQuorumBuffer              *prometheus.GaugeVec
	GovQuorumTarget              *prometheus.GaugeVec
	UpgradeHeight                *prometheus.GaugeVec
	BondedTokens                 prometheus.Gauge
	GovAttributionComplete       *prometheus.GaugeVec
	EndpointInfo                 *prometheus.GaugeVec
	GovProposalLive              *prometheus.GaugeVec
	SectionSuccess               *prometheus.GaugeVec
	RedelegationAlertThreshold   prometheus.Gauge
	RedelegationOutflowRatio     *prometheus.GaugeVec
	RedelegationOutflowATOM      *prometheus.GaugeVec
	RedelegationFlowRatio        *prometheus.GaugeVec
	RedelegationFlowATOM         *prometheus.GaugeVec
	RedelegationEventCount       *prometheus.GaugeVec
	RedelegationLatestEvent      *prometheus.GaugeVec
	RedelegationThresholdCrossed *prometheus.GaugeVec
	RedelegationScanSuccess      prometheus.Gauge
	RedelegationScanLastSuccess  prometheus.Gauge

	managedMu                               sync.RWMutex
	ManagedDelegationScanSuccess            prometheus.Gauge
	ManagedDelegationScanLastSuccess        prometheus.Gauge
	ManagedDelegationConfiguredAccounts     prometheus.Gauge
	ManagedDelegationWarningJailProgress    prometheus.Gauge
	ManagedDelegationCriticalJailProgress   prometheus.Gauge
	ManagedDelegationAccountATOM            *prometheus.GaugeVec
	ManagedValidatorDelegationATOM          *prometheus.GaugeVec
	ManagedValidatorPortfolioShare          *prometheus.GaugeVec
	ManagedValidatorMissedBlocks            *prometheus.GaugeVec
	ManagedValidatorMissedRatio             *prometheus.GaugeVec
	ManagedValidatorJailProgress            *prometheus.GaugeVec
	ManagedValidatorBlocksToJail            *prometheus.GaugeVec
	ManagedValidatorSecondsToJail           *prometheus.GaugeVec
	ManagedValidatorRecentMissRate          *prometheus.GaugeVec
	ManagedValidatorActive                  *prometheus.GaugeVec
	ManagedValidatorJailed                  *prometheus.GaugeVec
	ManagedValidatorTombstoned              *prometheus.GaugeVec
	ManagedValidatorDowntimeSlashExposure   *prometheus.GaugeVec
	ManagedValidatorDoubleSignSlashExposure *prometheus.GaugeVec
	ManagedValidatorEstimatedRewardsLost    *prometheus.GaugeVec
	ManagedValidatorRedelegationLocked      *prometheus.GaugeVec
	ManagedValidatorEstimatedRedelegatable  *prometheus.GaugeVec
	ManagedValidatorNextRedelegationUnlock  *prometheus.GaugeVec
	ManagedValidatorFinalRedelegationUnlock *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	managedValidatorLabels := []string{"operator_address", "moniker", "entity", "consensus_address", "accounts"}
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
			Help: "Missed blocks in the current signing window for active-set validators above the alert threshold.",
		}, []string{"operator_address", "moniker", "entity", "consensus_address"}),
		ValMissedRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_missed_ratio",
			Help: "Missed blocks as a fraction of the signing window (0-1).",
		}, []string{"operator_address", "moniker", "entity", "consensus_address"}),
		ValBlocksToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_blocks_to_jail",
			Help: "Blocks a validator can still miss in the current window before being jailed.",
		}, []string{"operator_address", "moniker", "entity", "consensus_address"}),
		ValSecondsToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_validator_seconds_to_jail",
			Help: "Estimated seconds until jailing if the validator keeps missing every block, using the measured block interval.",
		}, []string{"operator_address", "moniker", "entity", "consensus_address"}),
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
			Help: "Current or final turnout ratio for a proposal, labeled with title, status, and selector text.",
		}, []string{"proposal_id", "title", "status", "selector"}),
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
			Help: "Attributed validator voting power per entity and vote option as a share of bonded power.",
		}, []string{"proposal_id", "entity", "option"}),
		GovProposalVetoRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_veto_ratio",
			Help: "Authoritative NoWithVeto share of all votes cast on a proposal (0-1).",
		}, []string{"proposal_id"}),
		GovProposalQuorumMet: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_quorum_met",
			Help: "1 when proposal turnout is at or above the on-chain quorum, 0 otherwise.",
		}, []string{"proposal_id"}),
		GovProposalVetoState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_veto_state",
			Help: "Proposal veto state: -1 quorum not met, 0 quorum met below veto threshold, 1 quorum met above veto threshold.",
		}, []string{"proposal_id"}),
		GovEntityVetoCapability: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_entity_veto_capability_ratio",
			Help: "Actual or hypothetical share of votes attributable to an entity that is individually sufficient to veto, labeled actual or potential. Potential assumes the entity casts or changes all attributed validator power to NoWithVeto now and no later dilution.",
		}, []string{"proposal_id", "entity", "capability"}),
		GovNonVoterPower: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_non_voter_power_ratio",
			Help: "Share of bonded stake held by entities that did not vote on a proposal (0-1).",
		}, []string{"proposal_id", "entity"}),
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
			Help: "On-chain veto threshold as a fraction of all votes cast (0-1).",
		}),
		EntityCanVetoAlone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_entity_can_veto_alone",
			Help: "1 if this entity's bonded voting-power share is strictly greater than the on-chain veto threshold, 0 otherwise.",
		}, []string{"entity"}),
		AnyEntityCanVeto: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_any_entity_can_veto_alone",
			Help: "1 if at least one entity holds enough bonded voting power to veto even at full turnout, 0 if none does.",
		}),
		VetoPowerNeeded: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_veto_power_needed_ratio",
			Help: "Bonded voting-power share a single entity must strictly exceed to veto even at full turnout (0-1).",
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
			Help: "1 when current validator votes for a live proposal were available for entity attribution, 0 otherwise.",
		}, []string{"proposal_id"}),
		EndpointInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_collector_endpoint_info",
			Help: "The endpoint the collector selected for each protocol, as a labelled constant 1.",
		}, []string{"protocol", "provider", "url"}),
		GovProposalLive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_governance_proposal_live",
			Help: "1 while a proposal's voting period is open and this cycle confirmed it. Absent for closed proposals and cleared when the governance query fails, so time-bound alerts cannot fire on stale data.",
		}, []string{"proposal_id"}),
		SectionSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_collector_section_success",
			Help: "Whether the last attempt at a section of the collection cycle succeeded (1) or failed (0).",
		}, []string{"section"}),
		RedelegationAlertThreshold: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_alert_threshold_ratio",
			Help: "Configured rolling redelegation outflow alert threshold as a ratio (0-1).",
		}),
		RedelegationOutflowRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_outflow_ratio",
			Help: "Successful uatom redelegated from a source validator during the rolling window, divided by the current bonded staking pool.",
		}, []string{"source_validator", "source_moniker", "source_entity"}),
		RedelegationOutflowATOM: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_outflow_atom",
			Help: "ATOM successfully redelegated from a source validator during the rolling window.",
		}, []string{"source_validator", "source_moniker", "source_entity"}),
		RedelegationFlowRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_flow_ratio",
			Help: "Successful source-to-destination uatom redelegation flow during the rolling window, divided by the current bonded staking pool.",
		}, []string{"source_validator", "source_moniker", "source_entity", "destination_validator", "destination_moniker", "destination_entity"}),
		RedelegationFlowATOM: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_flow_atom",
			Help: "Successful source-to-destination redelegation flow in ATOM during the rolling window.",
		}, []string{"source_validator", "source_moniker", "source_entity", "destination_validator", "destination_moniker", "destination_entity"}),
		RedelegationEventCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_event_count",
			Help: "Count of successful source-to-destination uatom redelegation messages during the rolling window.",
		}, []string{"source_validator", "source_moniker", "source_entity", "destination_validator", "destination_moniker", "destination_entity"}),
		RedelegationLatestEvent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_latest_event_timestamp_seconds",
			Help: "Unix timestamp of the latest successful uatom redelegation from a source validator in the rolling window.",
		}, []string{"source_validator", "source_moniker", "source_entity"}),
		RedelegationThresholdCrossed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_threshold_crossed_timestamp_seconds",
			Help: "Unix timestamp of the most recent below-to-at-or-above configured-threshold transition for a source validator's rolling redelegation outflow.",
		}, []string{"source_validator", "source_moniker", "source_entity"}),
		RedelegationScanSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_scan_success",
			Help: "Whether the last redelegation scan completed without partial data (1) or failed (0).",
		}),
		RedelegationScanLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_redelegation_scan_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last complete redelegation scan.",
		}),
		ManagedDelegationScanSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_scan_success",
			Help: "Whether the last managed-delegation scan completed without partial core data (1) or failed (0).",
		}),
		ManagedDelegationScanLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_scan_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last complete managed-delegation scan.",
		}),
		ManagedDelegationConfiguredAccounts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_configured_accounts",
			Help: "Number of configured managed delegator account addresses.",
		}),
		ManagedDelegationWarningJailProgress: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_warning_jail_progress_ratio",
			Help: "Configured warning threshold for managed-validator jail progress (0-1).",
		}),
		ManagedDelegationCriticalJailProgress: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_critical_jail_progress_ratio",
			Help: "Configured critical threshold for managed-validator jail progress (0-1).",
		}),
		ManagedDelegationAccountATOM: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_delegation_account_atom",
			Help: "Current positive managed delegation in ATOM by configured account and validator.",
		}, []string{"account", "delegator_address", "operator_address", "moniker", "entity", "consensus_address"}),
		ManagedValidatorDelegationATOM: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_delegation_atom",
			Help: "Current positive managed delegation in ATOM aggregated by validator.",
		}, managedValidatorLabels),
		ManagedValidatorPortfolioShare: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_portfolio_share_ratio",
			Help: "Share of all currently managed delegation held with this validator (0-1).",
		}, managedValidatorLabels),
		ManagedValidatorMissedBlocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_missed_blocks",
			Help: "Current slashing-module missed-block counter for a managed validator.",
		}, managedValidatorLabels),
		ManagedValidatorMissedRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_missed_ratio",
			Help: "Managed validator missed blocks divided by the live signed-blocks window (0-1).",
		}, managedValidatorLabels),
		ManagedValidatorJailProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_jail_progress_ratio",
			Help: "Managed validator missed blocks divided by the live maximum missed blocks before jail, clamped to 0-1.",
		}, managedValidatorLabels),
		ManagedValidatorBlocksToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_blocks_to_jail",
			Help: "Blocks of live jail headroom remaining if every subsequent block is missed.",
		}, managedValidatorLabels),
		ManagedValidatorSecondsToJail: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_seconds_to_jail",
			Help: "Estimated seconds of jail headroom if every subsequent block is missed, using the measured block interval.",
		}, managedValidatorLabels),
		ManagedValidatorRecentMissRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_recent_miss_rate_ratio",
			Help: "Fraction of eligible sampled blocks missed by a managed validator across the latest 10 completed CometBFT commits; validators eligible for none report 0.",
		}, managedValidatorLabels),
		ManagedValidatorActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_active",
			Help: "Whether a validator holding managed delegation is in the current CometBFT consensus set (1) or not (0).",
		}, managedValidatorLabels),
		ManagedValidatorJailed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_jailed",
			Help: "Whether a validator holding managed delegation is currently jailed (1) or not (0).",
		}, managedValidatorLabels),
		ManagedValidatorTombstoned: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_tombstoned",
			Help: "Whether a validator holding managed delegation is tombstoned (1) or not (0).",
		}, managedValidatorLabels),
		ManagedValidatorDowntimeSlashExposure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_downtime_slash_exposure_atom",
			Help: "Current managed ATOM exposed to the live downtime slash fraction.",
		}, managedValidatorLabels),
		ManagedValidatorDoubleSignSlashExposure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_double_sign_slash_exposure_atom",
			Help: "Current managed ATOM exposed to the live double-sign slash fraction.",
		}, managedValidatorLabels),
		ManagedValidatorEstimatedRewardsLost: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_estimated_rewards_lost_per_hour_atom",
			Help: "Estimated ATOM rewards lost per hour while this managed delegation earns no rewards, using live annual provisions, bonded tokens, community tax, and validator commission; omitted when optional inputs are unavailable.",
		}, managedValidatorLabels),
		ManagedValidatorRedelegationLocked: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_redelegation_locked_atom",
			Help: "Managed ATOM locked from another redelegation because current chain state returns a receiving redelegation entry for its delegator-account and destination-validator pair.",
		}, managedValidatorLabels),
		ManagedValidatorEstimatedRedelegatable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_estimated_redelegatable_atom",
			Help: "Managed ATOM currently redelegatable after excluding entire account-validator positions with receiving redelegation entries returned by current chain state.",
		}, managedValidatorLabels),
		ManagedValidatorNextRedelegationUnlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_next_redelegation_unlock_timestamp_seconds",
			Help: "Earliest final unlock timestamp among locked managed account-validator positions, or 0 when none is locked.",
		}, managedValidatorLabels),
		ManagedValidatorFinalRedelegationUnlock: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "validator_health_managed_validator_final_redelegation_unlock_timestamp_seconds",
			Help: "Latest final unlock timestamp among locked managed account-validator positions, or 0 when none is locked.",
		}, managedValidatorLabels),
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
	m.GovProposalVetoRatio.Describe(ch)
	m.GovProposalQuorumMet.Describe(ch)
	m.GovProposalVetoState.Describe(ch)
	m.GovEntityVetoCapability.Describe(ch)
	m.GovNonVoterPower.Describe(ch)
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
	m.GovProposalLive.Describe(ch)
	m.SectionSuccess.Describe(ch)
	m.RedelegationAlertThreshold.Describe(ch)
	m.RedelegationOutflowRatio.Describe(ch)
	m.RedelegationOutflowATOM.Describe(ch)
	m.RedelegationFlowRatio.Describe(ch)
	m.RedelegationFlowATOM.Describe(ch)
	m.RedelegationEventCount.Describe(ch)
	m.RedelegationLatestEvent.Describe(ch)
	m.RedelegationThresholdCrossed.Describe(ch)
	m.RedelegationScanSuccess.Describe(ch)
	m.RedelegationScanLastSuccess.Describe(ch)
	m.ManagedDelegationScanSuccess.Describe(ch)
	m.ManagedDelegationScanLastSuccess.Describe(ch)
	m.ManagedDelegationConfiguredAccounts.Describe(ch)
	m.ManagedDelegationWarningJailProgress.Describe(ch)
	m.ManagedDelegationCriticalJailProgress.Describe(ch)
	m.ManagedDelegationAccountATOM.Describe(ch)
	m.ManagedValidatorDelegationATOM.Describe(ch)
	m.ManagedValidatorPortfolioShare.Describe(ch)
	m.ManagedValidatorMissedBlocks.Describe(ch)
	m.ManagedValidatorMissedRatio.Describe(ch)
	m.ManagedValidatorJailProgress.Describe(ch)
	m.ManagedValidatorBlocksToJail.Describe(ch)
	m.ManagedValidatorSecondsToJail.Describe(ch)
	m.ManagedValidatorRecentMissRate.Describe(ch)
	m.ManagedValidatorActive.Describe(ch)
	m.ManagedValidatorJailed.Describe(ch)
	m.ManagedValidatorTombstoned.Describe(ch)
	m.ManagedValidatorDowntimeSlashExposure.Describe(ch)
	m.ManagedValidatorDoubleSignSlashExposure.Describe(ch)
	m.ManagedValidatorEstimatedRewardsLost.Describe(ch)
	m.ManagedValidatorRedelegationLocked.Describe(ch)
	m.ManagedValidatorEstimatedRedelegatable.Describe(ch)
	m.ManagedValidatorNextRedelegationUnlock.Describe(ch)
	m.ManagedValidatorFinalRedelegationUnlock.Describe(ch)
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
	m.GovProposalVetoRatio.Collect(ch)
	m.GovProposalQuorumMet.Collect(ch)
	m.GovProposalVetoState.Collect(ch)
	m.GovEntityVetoCapability.Collect(ch)
	m.GovNonVoterPower.Collect(ch)
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
	m.GovProposalLive.Collect(ch)
	m.SectionSuccess.Collect(ch)
	m.RedelegationAlertThreshold.Collect(ch)
	m.RedelegationOutflowRatio.Collect(ch)
	m.RedelegationOutflowATOM.Collect(ch)
	m.RedelegationFlowRatio.Collect(ch)
	m.RedelegationFlowATOM.Collect(ch)
	m.RedelegationEventCount.Collect(ch)
	m.RedelegationLatestEvent.Collect(ch)
	m.RedelegationThresholdCrossed.Collect(ch)
	m.RedelegationScanSuccess.Collect(ch)
	m.RedelegationScanLastSuccess.Collect(ch)

	m.managedMu.RLock()
	defer m.managedMu.RUnlock()
	m.ManagedDelegationScanSuccess.Collect(ch)
	m.ManagedDelegationScanLastSuccess.Collect(ch)
	m.ManagedDelegationConfiguredAccounts.Collect(ch)
	m.ManagedDelegationWarningJailProgress.Collect(ch)
	m.ManagedDelegationCriticalJailProgress.Collect(ch)
	m.ManagedDelegationAccountATOM.Collect(ch)
	m.ManagedValidatorDelegationATOM.Collect(ch)
	m.ManagedValidatorPortfolioShare.Collect(ch)
	m.ManagedValidatorMissedBlocks.Collect(ch)
	m.ManagedValidatorMissedRatio.Collect(ch)
	m.ManagedValidatorJailProgress.Collect(ch)
	m.ManagedValidatorBlocksToJail.Collect(ch)
	m.ManagedValidatorSecondsToJail.Collect(ch)
	m.ManagedValidatorRecentMissRate.Collect(ch)
	m.ManagedValidatorActive.Collect(ch)
	m.ManagedValidatorJailed.Collect(ch)
	m.ManagedValidatorTombstoned.Collect(ch)
	m.ManagedValidatorDowntimeSlashExposure.Collect(ch)
	m.ManagedValidatorDoubleSignSlashExposure.Collect(ch)
	m.ManagedValidatorEstimatedRewardsLost.Collect(ch)
	m.ManagedValidatorRedelegationLocked.Collect(ch)
	m.ManagedValidatorEstimatedRedelegatable.Collect(ch)
	m.ManagedValidatorNextRedelegationUnlock.Collect(ch)
	m.ManagedValidatorFinalRedelegationUnlock.Collect(ch)
}

// Collector orchestrates the polling and metric updates.
type Collector struct {
	restClient *RESTClient
	rpcClient  *RPCClient
	resolver   *endpoints.Resolver
	entityMap  map[string]string
	metrics    *Metrics

	proposalHistoryWindow time.Duration

	lastSlashingParams *SlashingParams
	lastBlockTime      float64

	// Previous snapshot for detecting changes
	prevValidators map[string]bool // operator_address -> present
	firstRun       bool

	redelegation          *RedelegationScanner
	redelegationInterval  time.Duration
	redelegationReady     chan struct{}
	redelegationReadyOnce sync.Once

	managedDelegations         *ManagedDelegationScanner
	managedDelegationInterval  time.Duration
	managedDelegationReady     chan struct{}
	managedDelegationReadyOnce sync.Once
}

// New builds a collector that discovers its endpoints through the resolver at the
// start of every collection cycle.
func New(resolver *endpoints.Resolver, entityMap map[string]string) *Collector {
	return &Collector{
		restClient:             NewRESTClient(),
		rpcClient:              NewRPCClient(),
		resolver:               resolver,
		entityMap:              entityMap,
		metrics:                NewMetrics(),
		proposalHistoryWindow:  DefaultProposalHistoryWindow,
		prevValidators:         make(map[string]bool),
		firstRun:               true,
		redelegationReady:      make(chan struct{}),
		managedDelegationReady: make(chan struct{}),
	}
}

// ConfigureRedelegation enables the independent rolling redelegation scanner.
func (c *Collector) ConfigureRedelegation(thresholdRatio float64, interval, window time.Duration) {
	c.redelegationInterval = interval
	c.redelegation = NewRedelegationScanner(c.metrics, c.entityMap, window, 2*interval, thresholdRatio)
}

// SetProposalHistoryWindow sets the maximum age of closed proposals exported to
// the dashboard. Live proposals are always included.
func (c *Collector) SetProposalHistoryWindow(window time.Duration) {
	if window > 0 {
		c.proposalHistoryWindow = window
	}
}

// resolveEndpoints re-probes the candidate endpoints and points the REST and
// CometBFT clients at healthy nodes.
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
	if c.redelegation != nil {
		c.redelegation.SetEndpoints(res.RESTAddresses, res.TxSearchAddresses)
		c.redelegationReadyOnce.Do(func() { close(c.redelegationReady) })
	}
	if c.managedDelegations != nil {
		c.managedDelegations.SetEndpoints(res.RESTAddresses, res.RPCAddresses)
		c.managedDelegationReadyOnce.Do(func() { close(c.managedDelegationReady) })
	}

	// Republish from scratch so a rotation does not leave the previous endpoint
	// reporting as current alongside the new one.
	c.metrics.EndpointInfo.Reset()
	c.publishEndpoint("rest", res.REST, c.restClient.BaseURL())
	c.publishEndpoint("rpc", res.RPC, c.rpcClient.BaseURL())
	if len(res.TxSearchAddresses) > 0 {
		c.publishEndpoint("tx_search", res.TxSearch, res.TxSearchAddresses[0])
	}

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

	// Fail closed before any network work. Every operational governance alert is
	// gated on GovProposalLive, so retiring the previous cycle's gate here makes
	// every return path safe, including endpoint, staking, and consensus failures
	// that happen before governance can be queried. A proposal is republished as
	// live only after its full analysis succeeds near the end of the cycle.
	c.retireLiveProposalSeries()

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
	//
	// A failure here is not just logged. Every governance series that describes an
	// open voting period is retired immediately, because leaving the previous
	// cycle's values in place lets time-bound alerts keep firing on a snapshot
	// nobody can refresh. The cycle also stops reporting itself successful at the
	// end, so CollectorStale eventually surfaces the blindness.
	proposalCutoff := time.Now().Add(-c.proposalHistoryWindow)
	activeProps, recentClosedProps, govErr := c.restClient.QueryRelevantProposals(proposalCutoff)
	if govErr != nil {
		log.Printf("WARN: Governance query failed; live proposal series remain retired: %v", govErr)
	}
	govQuorum, _, govVeto, govParamsErr := c.restClient.QueryGovParams()
	if govParamsErr != nil {
		log.Printf("WARN: Governance params query failed; retaining the previous veto threshold: %v", govParamsErr)
	} else {
		c.metrics.GovVetoThreshold.Set(govVeto)
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

	// Turnout-independent unilateral veto capability. The SDK compares
	// NoWithVeto voting power to all voting power that was cast and uses a strict
	// greater-than check. Comparing an entity's bonded share to the live threshold
	// answers the stronger operational question: can it veto even if every bonded
	// token votes? Lower-turnout scenarios are intentionally not labeled as an
	// entity holding unilateral veto power.
	c.metrics.EntityCanVetoAlone.Reset()
	anyCanVeto := 0.0
	if govParamsErr == nil && govVeto > 0 && govVeto < 1 {
		c.metrics.VetoPowerNeeded.Set(govVeto)
		for _, e := range entities {
			share := e.power / totalBonded
			if !hasTurnoutIndependentVetoPower(share, govVeto) {
				continue
			}
			c.metrics.EntityCanVetoAlone.WithLabelValues(e.name).Set(1)
			anyCanVeto = 1
			log.Printf("VETO CAPABILITY: %s holds %.2f%% of bonded voting power, above the %.1f%% on-chain threshold; it can veto even at full turnout.",
				e.name, share*100, govVeto*100)
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

	// Governance metrics for proposals whose voting is still open.
	//
	// The whole set is retired and republished each cycle rather than updated in
	// place. A proposal that closes simply stops appearing in activeProps, and
	// without this its last values would persist forever: seconds-remaining
	// clamped to zero and turnout frozen below quorum, which permanently satisfies
	// GovernanceQuorumRisk, QuorumBufferBelowTarget and QuorumNotMet. That would
	// page the ecosystem about a vote that has already ended and can no longer be
	// influenced.
	//
	// Closed proposal summaries are refreshed separately inside the bounded history
	// window. Prometheus retains their prior samples, while alerts are gated on
	// validator_health_governance_proposal_live so history cannot page.
	for _, p := range activeProps {
		endTime, err := time.Parse(time.RFC3339, p.VotingEndTime)
		if err == nil {
			remaining := time.Until(endTime).Seconds()
			if remaining < 0 {
				remaining = 0
			}
			c.metrics.GovSecondsRemaining.WithLabelValues(p.ID).Set(remaining)
		}
	}

	// Build consensus address -> validator metadata from the active set.
	consensusToValidator := make(map[string]Validator)
	for _, v := range validators {
		consAddr, err := consensusAddressFromPubKey(pubKeyOf(v))
		if err != nil {
			continue
		}
		consensusToValidator[consAddr] = v
	}

	// Slashing metrics. Only active-set validators are published, and only the
	// ones already missing enough blocks to be worth looking at; below that the
	// counter is normal operating noise (restarts, brief maintenance).
	//
	// Every risk series carries exact validator identity plus entity attribution.
	// An entity is the entity-map name when one exists, and the validator's own
	// moniker otherwise.
	c.metrics.ValMissedBlocksInfo.Reset()
	c.metrics.ValMissedRatio.Reset()
	c.metrics.ValBlocksToJail.Reset()
	c.metrics.ValSecondsToJail.Reset()
	c.metrics.ValAtRiskPower.Reset()

	alertThreshold := float64(slashing.MaxMissedBlocks) * 0.2
	for _, info := range signingInfos {
		mb, _ := strconv.ParseFloat(info.MissedBlocksCounter, 64)
		c.metrics.ValMissedBlocks.WithLabelValues(info.Address).Set(mb)

		validator, inActiveSet := consensusToValidator[info.Address]
		if !inActiveSet || mb < alertThreshold {
			continue
		}
		moniker := validator.Description.Moniker
		entity := c.entityMap[validator.OperatorAddress]
		if entity == "" {
			entity = moniker
		}
		labels := []string{validator.OperatorAddress, moniker, entity, info.Address}

		blocksToJail := blocksUntilJail(slashing.MaxMissedBlocks, mb)

		c.metrics.ValMissedBlocksInfo.WithLabelValues(labels...).Set(mb)
		c.metrics.ValMissedRatio.WithLabelValues(labels...).Set(mb / float64(slashing.SignedBlocksWindow))
		c.metrics.ValBlocksToJail.WithLabelValues(labels...).Set(blocksToJail)
		// Worst case: the validator signs nothing from here on, so every
		// remaining block of headroom is consumed at the measured block rate.
		c.metrics.ValSecondsToJail.WithLabelValues(labels...).Set(blocksToJail * blockInterval)
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

	// Validator info (consensus, moniker, and operator mapping).
	for _, v := range validators {
		consensusAddress, consensusErr := consensusAddressFromPubKey(pubKeyOf(v))
		if consensusErr != nil {
			continue
		}
		c.metrics.ValInfo.WithLabelValues(
			consensusAddress,
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

	// Proposal analysis runs last. Aggregate state for live and recent closed
	// proposals comes from lightweight REST summaries and tallies. Only live
	// proposals request current vote records for optional entity attribution; no
	// transaction-history scan or archive RPC dependency is involved.
	var analysisErr error
	if govErr == nil && govParamsErr == nil {
		if bondedTokens > 0 {
			analysisErr = c.analyzeProposals(
				activeProps, recentClosedProps, validators, bondedTokens, govQuorum, govVeto,
			)
		} else if len(activeProps) > 0 {
			analysisErr = errors.New("cannot analyze active proposals without the bonded staking pool")
		}
	}
	governanceErr := errors.Join(govErr, govParamsErr, analysisErr)
	c.metrics.SectionSuccess.WithLabelValues("governance").Set(boolGauge(governanceErr == nil))

	// A partial snapshot is published, because concentration and signing metrics are
	// independent of governance and still accurate. It is not reported as a success:
	// advancing LastSuccess here would keep CollectorStale quiet while governance
	// values silently aged, which is the combination that lets stale data page.
	if governanceErr != nil {
		log.Println("Snapshot collection finished with a governance failure; not marking it successful.")
		return fmt.Errorf("governance collection failed: %w", governanceErr)
	}

	c.metrics.LastSuccess.Set(float64(time.Now().Unix()))
	log.Println("Snapshot collection complete.")
	return nil
}

// retireLiveProposalSeries drops every governance series that is only meaningful
// while a proposal's voting period is open. Prometheus retains old samples for
// historical range queries, but they disappear from current alert evaluation.
func (c *Collector) retireLiveProposalSeries() {
	c.metrics.GovProposalLive.Reset()
	c.metrics.GovSecondsRemaining.Reset()
	c.metrics.GovTurnout.Reset()
	c.metrics.GovQuorum.Reset()
}

// resetProposalAnalysisSeries makes the collector's current metric set exactly
// match the live proposals and retention-bounded closed proposals found this
// cycle. Prometheus and Mimir keep prior samples according to their own retention
// policies, so no historical data is deleted by resetting the in-process gauges.
func (c *Collector) resetProposalAnalysisSeries() {
	c.metrics.GovProposalInfo.Reset()
	c.metrics.GovProposalQuorum.Reset()
	c.metrics.GovProposalTally.Reset()
	c.metrics.GovEntityVote.Reset()
	c.metrics.GovProposalVetoRatio.Reset()
	c.metrics.GovProposalQuorumMet.Reset()
	c.metrics.GovProposalVetoState.Reset()
	c.metrics.GovEntityVetoCapability.Reset()
	c.metrics.GovNonVoterPower.Reset()
	c.metrics.GovWindow.Reset()
	c.metrics.GovParticipationCount.Reset()
	c.metrics.GovAttributionComplete.Reset()
	c.metrics.GovQuorumBuffer.Reset()
	c.metrics.GovQuorumTarget.Reset()
	c.retireLiveProposalSeries()
}

// analyzeProposals publishes lightweight aggregate state for every live proposal
// and each closed proposal inside the configured history window. Current vote
// records are requested only for live proposals, where entity-level action is
// still possible.
func (c *Collector) analyzeProposals(
	activeProps []Proposal,
	recentClosedProps []Proposal,
	validators []Validator,
	bondedTokens, quorum, vetoThreshold float64,
) error {
	c.resetProposalAnalysisSeries()

	var targets []Proposal
	seen := make(map[string]bool)
	var liveErrors []error

	for _, proposal := range activeProps {
		targets = append(targets, proposal)
		seen[proposal.ID] = true
	}
	for _, proposal := range recentClosedProps {
		if seen[proposal.ID] {
			continue
		}
		targets = append(targets, proposal)
		seen[proposal.ID] = true
	}

	for i := range targets {
		proposal := &targets[i]
		id := proposal.ID
		analysis, err := analyzeProposal(
			proposal,
			validators,
			c.entityMap,
			bondedTokens,
			quorum,
			c.restClient.QueryLiveTally,
			c.restClient.QueryProposalVotes,
		)
		if err != nil {
			log.Printf("WARN: proposal %s analysis failed: %v", id, err)
			if proposal.Status == statusVotingPeriod {
				liveErrors = append(liveErrors, fmt.Errorf("active proposal %s: %w", id, err))
			}
			continue
		}

		title := analysis.Title
		if len(title) > 60 {
			title = title[:60]
		}

		selectorStatus := analysis.Status
		if analysis.IsLive {
			selectorStatus = "LIVE"
		}
		selector := fmt.Sprintf("#%s | %s | %s", id, selectorStatus, title)
		c.metrics.GovProposalInfo.WithLabelValues(id, title, analysis.Status, selector).Set(analysis.FinalTurnout)
		c.metrics.GovProposalQuorum.WithLabelValues(id).Set(quorum)

		// Entity attribution is actionable only while voting is open. A failure of
		// the current-votes endpoint does not invalidate the authoritative tally.
		if analysis.IsLive {
			c.metrics.GovAttributionComplete.WithLabelValues(id).Set(boolGauge(analysis.AttributionComplete))
			if !analysis.AttributionComplete {
				log.Printf("WARN: proposal %s current votes are unavailable, "+
					"so per-entity vote breakdowns are omitted", id)
			}
		}

		// Desired state: turnout should clear quorum by at least 10 percentage
		// points. The target moves with the on-chain quorum parameter, so a
		// governance change to quorum automatically retargets the buffer.
		c.metrics.GovQuorumTarget.WithLabelValues(id).Set(quorum + QuorumBufferPoints)
		c.metrics.GovQuorumBuffer.WithLabelValues(id).Set(analysis.FinalTurnout - quorum)

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

		vetoRatio, quorumMet, vetoState, capabilities := analyzeProposalVeto(analysis, vetoThreshold)
		c.metrics.GovProposalVetoRatio.WithLabelValues(id).Set(vetoRatio)
		c.metrics.GovProposalQuorumMet.WithLabelValues(id).Set(boolGauge(quorumMet))
		c.metrics.GovProposalVetoState.WithLabelValues(id).Set(vetoState)
		for _, capability := range capabilities {
			c.metrics.GovEntityVetoCapability.WithLabelValues(
				id, capability.Entity, capability.Kind,
			).Set(capability.Ratio)
		}

		type entityOption struct {
			entity string
			option string
		}
		byEntity := make(map[string]float64)
		byEntityOption := make(map[entityOption]float64)
		for _, v := range analysis.Voted {
			byEntity[v.Entity] += v.VotingPower
			for option, weight := range voteWeights(v) {
				byEntityOption[entityOption{entity: v.Entity, option: option}] += v.VotingPower * weight
			}
		}
		for key, power := range byEntityOption {
			c.metrics.GovEntityVote.WithLabelValues(id, key.entity, key.option).Set(power / bondedTokens)
		}

		nonVoterByEntity := make(map[string]float64)
		for _, v := range analysis.NonVoters {
			nonVoterByEntity[v.Entity] += v.VotingPower
		}
		for entity, power := range nonVoterByEntity {
			share := power / bondedTokens
			if share < 0.0005 {
				continue
			}
			c.metrics.GovNonVoterPower.WithLabelValues(id, entity).Set(share)
		}

		c.metrics.GovParticipationCount.WithLabelValues(id, "Voted").Set(float64(len(byEntity)))
		c.metrics.GovParticipationCount.WithLabelValues(id, "Did not vote").Set(float64(len(nonVoterByEntity)))

		// Grafana uses the normal hourly samples of proposal_info to draw the
		// prospective turnout timeline. The window bounds keep the chart focused on
		// the period when an outcome could still be influenced.
		c.metrics.GovWindow.WithLabelValues(id, "start").Set(float64(analysis.VotingStart.Unix()))
		c.metrics.GovWindow.WithLabelValues(id, "end").Set(float64(analysis.VotingEnd.Unix()))

		if analysis.IsLive {
			// Live proposal records expose a zeroed final_tally_result. Publish the
			// authoritative turnout calculated from QueryLiveTally here, alongside
			// the quorum used by the same analysis, then publish the alert gate last.
			c.metrics.GovTurnout.WithLabelValues(id).Set(analysis.FinalTurnout)
			c.metrics.GovQuorum.WithLabelValues(id).Set(quorum)
			c.metrics.GovProposalLive.WithLabelValues(id).Set(1)
		}

		if analysis.IsLive && analysis.AttributionComplete {
			log.Printf("Proposal %s (%s) [voting open]: turnout %.2f%% vs quorum %.0f%%, "+
				"%d entities voted, %d did not",
				id, analysis.Status, analysis.FinalTurnout*100, quorum*100, len(byEntity), len(nonVoterByEntity))
			continue
		}
		log.Printf("Proposal %s (%s): turnout %.2f%% vs quorum %.0f%%",
			id, analysis.Status, analysis.FinalTurnout*100, quorum*100)
	}

	return errors.Join(liveErrors...)
}

func hasTurnoutIndependentVetoPower(entityShare, vetoThreshold float64) bool {
	return vetoThreshold > 0 && vetoThreshold < 1 && entityShare > vetoThreshold
}

// Run starts the polling loop. It runs one collection immediately, then on the given interval.
func (c *Collector) Run(interval time.Duration) {
	if interval <= 0 {
		log.Printf("Collection loop disabled because interval is %v", interval)
		return
	}

	// Run immediately on start
	if err := c.CollectSnapshot(); err != nil {
		log.Printf("Initial collection failed: %v", err)
	}

	for {
		next := nextAlignedRun(time.Now(), interval)
		log.Printf("Next snapshot collection scheduled for %s", next.Format(time.RFC3339))
		timer := time.NewTimer(time.Until(next))
		<-timer.C
		if err := c.CollectSnapshot(); err != nil {
			log.Printf("Collection failed: %v", err)
		}
	}
}

// RunRedelegation waits for the initial snapshot to resolve endpoints, then
// runs independently of the hourly snapshot loop.
func (c *Collector) RunRedelegation() {
	if c.redelegation == nil || c.redelegationInterval <= 0 {
		return
	}
	<-c.redelegationReady

	run := func() {
		if err := c.redelegation.Scan(time.Now()); err != nil {
			log.Printf("Redelegation scan failed: %v", err)
		}
	}
	run()

	ticker := time.NewTicker(c.redelegationInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		if err := c.redelegation.Scan(now); err != nil {
			log.Printf("Redelegation scan failed: %v", err)
		}
	}
}

func nextAlignedRun(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval).Add(interval)
}
