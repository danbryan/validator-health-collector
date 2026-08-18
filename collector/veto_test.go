package collector

import "testing"

func TestHasTurnoutIndependentVetoPower(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		share     float64
		threshold float64
		want      bool
	}{
		{name: "current largest entity is below Hub threshold", share: 0.206, threshold: 0.334, want: false},
		{name: "equality does not satisfy strict SDK comparison", share: 0.334, threshold: 0.334, want: false},
		{name: "share above threshold has turnout-independent veto power", share: 0.334001, threshold: 0.334, want: true},
		{name: "zero threshold is invalid", share: 0.5, threshold: 0, want: false},
		{name: "one threshold is invalid", share: 1, threshold: 1, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := hasTurnoutIndependentVetoPower(test.share, test.threshold); got != test.want {
				t.Fatalf("hasTurnoutIndependentVetoPower(%v, %v) = %v, want %v", test.share, test.threshold, got, test.want)
			}
		})
	}
}
