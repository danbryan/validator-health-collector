package collector

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danbryan/validator-health-collector/cosmosaddr"
)

// GovVote is a current validator vote on a live proposal, resolved to an entity.
type GovVote struct {
	ValoperAddress string
	Moniker        string
	Entity         string
	Option         string // YES, NO, ABSTAIN, NO_WITH_VETO, or WEIGHTED
	Options        map[string]float64
	VotingPower    float64
}

// ProposalAnalysis combines an authoritative tally with optional live validator
// attribution. Closed proposals intentionally keep only their final aggregate
// state because the governance module removes their current-vote records.
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
	NonVoters              []GovVote
	Yes, No, Abstain, Veto float64
	IsLive                 bool
	AttributionComplete    bool
}

// accToValoper converts a cosmos1... account address to cosmosvaloper1...
func accToValoper(acc string) (string, error) {
	return cosmosaddr.Reprefix(acc, "cosmosvaloper")
}

func normalizeVoteOption(option string) string {
	switch strings.TrimPrefix(option, "VOTE_OPTION_") {
	case "1", "YES":
		return "YES"
	case "2", "ABSTAIN":
		return "ABSTAIN"
	case "3", "NO":
		return "NO"
	case "4", "NO_WITH_VETO":
		return "NO_WITH_VETO"
	default:
		return "UNKNOWN"
	}
}

// tallyOf reads a proposal's authoritative tally. Open proposals must use the
// live tally endpoint because final_tally_result stays zeroed until voting ends.
func tallyOf(
	prop *Proposal,
	liveTally func(string) (float64, float64, float64, float64, error),
) (yes, no, abstain, veto float64, err error) {
	if prop.Status == statusVotingPeriod {
		if liveTally == nil {
			return 0, 0, 0, 0, fmt.Errorf("proposal %s is live but no tally query is available", prop.ID)
		}
		return liveTally(prop.ID)
	}

	yes, _ = strconv.ParseFloat(prop.FinalTallyResult.YesCount, 64)
	no, _ = strconv.ParseFloat(prop.FinalTallyResult.NoCount, 64)
	abstain, _ = strconv.ParseFloat(prop.FinalTallyResult.AbstainCount, 64)
	veto, _ = strconv.ParseFloat(prop.FinalTallyResult.NoWithVetoCount, 64)
	return yes, no, abstain, veto, nil
}

// analyzeProposal builds the actionable proposal view without transaction
// history. Aggregate state is authoritative for live and closed proposals.
// Per-entity attribution is queried only for live proposals and is optional, so
// an unavailable votes endpoint cannot suppress the tally-based alerts.
func analyzeProposal(
	prop *Proposal,
	validators []Validator,
	entityMap map[string]string,
	bondedTokens float64,
	quorum float64,
	liveTally func(string) (float64, float64, float64, float64, error),
	currentVotes func(string) (map[string]GovVote, error),
) (*ProposalAnalysis, error) {
	votingStart, err := time.Parse(time.RFC3339, prop.VotingStartTime)
	if err != nil {
		return nil, fmt.Errorf("parsing proposal %s voting start: %w", prop.ID, err)
	}
	votingEnd, err := time.Parse(time.RFC3339, prop.VotingEndTime)
	if err != nil {
		return nil, fmt.Errorf("parsing proposal %s voting end: %w", prop.ID, err)
	}

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

	yes, no, abstain, veto, err := tallyOf(prop, liveTally)
	if err != nil {
		return nil, err
	}
	analysis.Yes, analysis.No, analysis.Abstain, analysis.Veto = yes, no, abstain, veto
	if bondedTokens > 0 {
		analysis.FinalTurnout = (yes + no + abstain + veto) / bondedTokens
	}

	if !analysis.IsLive {
		return analysis, nil
	}

	if currentVotes == nil {
		log.Printf("WARN: proposal %s has no current-votes query; entity attribution is unavailable", prop.ID)
		return analysis, nil
	}
	rawVotes, err := currentVotes(prop.ID)
	if err != nil {
		log.Printf("WARN: proposal %s current-vote query failed; aggregate alerts remain valid: %v", prop.ID, err)
		return analysis, nil
	}
	if yes+no+abstain+veto > 0 && len(rawVotes) == 0 {
		log.Printf("WARN: proposal %s has a non-zero tally but returned no current votes; entity attribution is unavailable", prop.ID)
		return analysis, nil
	}
	analysis.AttributionComplete = true

	valoperInfo := make(map[string]Validator, len(validators))
	for _, validator := range validators {
		valoperInfo[validator.OperatorAddress] = validator
	}

	votedValopers := make(map[string]bool)
	for accAddr, vote := range rawVotes {
		valoper, convertErr := accToValoper(accAddr)
		if convertErr != nil {
			continue
		}
		validator, ok := valoperInfo[valoper]
		if !ok {
			// Delegator votes are already included in the authoritative tally but
			// cannot be attributed to an active-set validator entity.
			continue
		}
		power, _ := strconv.ParseFloat(validator.Tokens, 64)
		entityName := entityMap[valoper]
		if entityName == "" {
			entityName = validator.Description.Moniker
		}
		vote.ValoperAddress = valoper
		vote.Moniker = validator.Description.Moniker
		vote.Entity = entityName
		vote.VotingPower = power
		analysis.Voted = append(analysis.Voted, vote)
		votedValopers[valoper] = true
	}

	for _, validator := range validators {
		if votedValopers[validator.OperatorAddress] {
			continue
		}
		power, _ := strconv.ParseFloat(validator.Tokens, 64)
		entityName := entityMap[validator.OperatorAddress]
		if entityName == "" {
			entityName = validator.Description.Moniker
		}
		analysis.NonVoters = append(analysis.NonVoters, GovVote{
			ValoperAddress: validator.OperatorAddress,
			Moniker:        validator.Description.Moniker,
			Entity:         entityName,
			Option:         "DID_NOT_VOTE",
			VotingPower:    power,
		})
	}

	sort.Slice(analysis.Voted, func(i, j int) bool {
		return analysis.Voted[i].VotingPower > analysis.Voted[j].VotingPower
	})
	sort.Slice(analysis.NonVoters, func(i, j int) bool {
		return analysis.NonVoters[i].VotingPower > analysis.NonVoters[j].VotingPower
	})
	return analysis, nil
}
