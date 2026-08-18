package collector

import "testing"

func TestAnalyzeProposalVeto(t *testing.T) {
	t.Parallel()

	analysis := &ProposalAnalysis{
		Yes:                 50,
		No:                  5,
		Abstain:             5,
		Veto:                10,
		FinalTurnout:        0.70,
		Quorum:              0.40,
		IsLive:              true,
		AttributionComplete: true,
		Voted: []GovVote{
			{Entity: "actual", VotingPower: 25, Option: "NO_WITH_VETO"},
			{Entity: "potential", VotingPower: 10, Option: "YES"},
		},
		NonVoters: []GovVote{
			{Entity: "potential", VotingPower: 21, Option: "DID_NOT_VOTE"},
		},
	}

	ratio, quorumMet, state, capabilities := analyzeProposalVeto(analysis, 0.334)
	if ratio != 10.0/70.0 {
		t.Fatalf("veto ratio = %v, want %v", ratio, 10.0/70.0)
	}
	if !quorumMet || state != 0 {
		t.Fatalf("quorumMet = %v, state = %v, want true and 0", quorumMet, state)
	}
	if len(capabilities) != 2 {
		t.Fatalf("capabilities = %#v, want one actual and one potential", capabilities)
	}
	byKind := make(map[string]entityVetoCapability, len(capabilities))
	for _, capability := range capabilities {
		byKind[capability.Kind] = capability
	}
	actual := byKind[vetoCapabilityActual]
	if actual.Entity != "actual" || actual.Ratio != 25.0/70.0 {
		t.Errorf("actual capability = %#v, want actual entity at 25/70", actual)
	}
	// The potential entity already contributes 10 to turnout and would add its
	// remaining 21, so its hypothetical share is 31 / (70 + 21).
	potential := byKind[vetoCapabilityPotential]
	if potential.Entity != "potential" || potential.Ratio != 31.0/91.0 {
		t.Errorf("potential capability = %#v, want potential entity at 31/91", potential)
	}
}

func TestAnalyzeProposalVetoGatesEntityRowsOnQuorum(t *testing.T) {
	t.Parallel()

	analysis := &ProposalAnalysis{
		Yes:                 10,
		Veto:                30,
		FinalTurnout:        0.39,
		Quorum:              0.40,
		IsLive:              true,
		AttributionComplete: true,
		Voted: []GovVote{
			{Entity: "vetoer", VotingPower: 30, Option: "NO_WITH_VETO"},
		},
	}

	ratio, quorumMet, state, capabilities := analyzeProposalVeto(analysis, 0.334)
	if ratio != 0.75 || quorumMet || state != -1 || len(capabilities) != 0 {
		t.Fatalf("got ratio=%v quorum=%v state=%v capabilities=%#v", ratio, quorumMet, state, capabilities)
	}
}

func TestAnalyzeProposalVetoClosedProposalOmitsPotentialRows(t *testing.T) {
	t.Parallel()

	analysis := &ProposalAnalysis{
		Yes:                 40,
		FinalTurnout:        0.40,
		Quorum:              0.40,
		AttributionComplete: true,
		NonVoters: []GovVote{
			{Entity: "late", VotingPower: 30, Option: "DID_NOT_VOTE"},
		},
	}

	_, _, _, capabilities := analyzeProposalVeto(analysis, 0.334)
	if len(capabilities) != 0 {
		t.Fatalf("closed proposal capabilities = %#v, want no counterfactual rows", capabilities)
	}
}

func TestAnalyzeProposalVetoUsesStrictThreshold(t *testing.T) {
	t.Parallel()

	analysis := &ProposalAnalysis{
		Yes:                 666,
		Veto:                334,
		FinalTurnout:        1,
		Quorum:              0.40,
		IsLive:              true,
		AttributionComplete: true,
		Voted: []GovVote{
			{Entity: "at-threshold", VotingPower: 334, Option: "NO_WITH_VETO"},
		},
	}

	ratio, quorumMet, state, capabilities := analyzeProposalVeto(analysis, 0.334)
	if ratio != 0.334 || !quorumMet || state != 0 || len(capabilities) != 0 {
		t.Fatalf("got ratio=%v quorum=%v state=%v capabilities=%#v", ratio, quorumMet, state, capabilities)
	}
}

func TestVoteWeightsPreservesWeightedOptions(t *testing.T) {
	t.Parallel()

	vote := GovVote{Options: map[string]float64{"YES": 0.75, "NO_WITH_VETO": 0.25}}
	weights := voteWeights(vote)
	if weights["YES"] != 0.75 || weights["NO_WITH_VETO"] != 0.25 {
		t.Fatalf("voteWeights() = %#v", weights)
	}
}
