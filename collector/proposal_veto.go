package collector

import "math"

const (
	vetoCapabilityActual    = "Already voting NoWithVeto"
	vetoCapabilityPotential = "Could veto if changed now"
)

type entityVetoCapability struct {
	Entity string
	Kind   string
	Ratio  float64
}

func voteWeights(vote GovVote) map[string]float64 {
	if len(vote.Options) > 0 {
		return vote.Options
	}
	if vote.Option == "" || vote.Option == "DID_NOT_VOTE" {
		return nil
	}
	return map[string]float64{vote.Option: 1}
}

// analyzeProposalVeto separates authoritative proposal state from attributed
// entity capability. The chain tally is exact. Entity rows are a validator-level
// attribution proxy because delegators can override their validator's vote.
func analyzeProposalVeto(
	analysis *ProposalAnalysis,
	vetoThreshold float64,
) (vetoRatio float64, quorumMet bool, vetoState float64, capabilities []entityVetoCapability) {
	totalVotes := analysis.Yes + analysis.No + analysis.Abstain + analysis.Veto
	if totalVotes > 0 {
		vetoRatio = analysis.Veto / totalVotes
	}

	quorumMet = analysis.FinalTurnout >= analysis.Quorum
	vetoState = -1
	if quorumMet {
		vetoState = 0
		if vetoThreshold > 0 && vetoRatio > vetoThreshold {
			vetoState = 1
		}
	}

	if !quorumMet || !analysis.AttributionComplete || totalVotes <= 0 || vetoThreshold <= 0 {
		return vetoRatio, quorumMet, vetoState, nil
	}

	totalByEntity := make(map[string]float64)
	participatingByEntity := make(map[string]float64)
	actualVetoByEntity := make(map[string]float64)
	for _, vote := range analysis.Voted {
		totalByEntity[vote.Entity] += vote.VotingPower
		participatingByEntity[vote.Entity] += vote.VotingPower
		actualVetoByEntity[vote.Entity] += vote.VotingPower * voteWeights(vote)["NO_WITH_VETO"]
	}
	for _, vote := range analysis.NonVoters {
		totalByEntity[vote.Entity] += vote.VotingPower
	}

	for entity, entityPower := range totalByEntity {
		actualRatio := actualVetoByEntity[entity] / totalVotes
		if actualRatio > vetoThreshold {
			capabilities = append(capabilities, entityVetoCapability{
				Entity: entity,
				Kind:   vetoCapabilityActual,
				Ratio:  actualRatio,
			})
			continue
		}

		if !analysis.IsLive {
			continue
		}

		unvotedPower := math.Max(entityPower-participatingByEntity[entity], 0)
		hypotheticalTurnout := totalVotes + unvotedPower
		if hypotheticalTurnout <= 0 {
			continue
		}
		potentialRatio := entityPower / hypotheticalTurnout
		if potentialRatio > vetoThreshold {
			capabilities = append(capabilities, entityVetoCapability{
				Entity: entity,
				Kind:   vetoCapabilityPotential,
				Ratio:  potentialRatio,
			})
		}
	}

	return vetoRatio, quorumMet, vetoState, capabilities
}
