package config_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	collectorconfig "github.com/danbryan/validator-health-collector/config"
)

func TestLoadUsesDefaultOnlyWithoutPath(t *testing.T) {
	t.Parallel()

	cfg, err := collectorconfig.Load("")
	if err != nil {
		t.Fatalf("Load(\"\") returned error: %v", err)
	}
	if cfg.Redelegation.AlertThresholdPercent != 2 {
		t.Fatalf("default threshold = %v, want 2", cfg.Redelegation.AlertThresholdPercent)
	}

	if _, err := collectorconfig.Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("Load(missing path) succeeded, want an error for an explicitly supplied file")
	}
}

func TestLoadValidatesRedelegationThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		want    float64
		wantErr bool
	}{
		{name: "integer", body: "redelegation:\n  alert_threshold_percent: 2\n", want: 2},
		{name: "decimal", body: "redelegation:\n  alert_threshold_percent: 0.5\n", want: 0.5},
		{name: "maximum", body: "redelegation:\n  alert_threshold_percent: 100\n", want: 100},
		{name: "missing", body: "redelegation: {}\n", wantErr: true},
		{name: "zero", body: "redelegation:\n  alert_threshold_percent: 0\n", wantErr: true},
		{name: "negative", body: "redelegation:\n  alert_threshold_percent: -1\n", wantErr: true},
		{name: "over maximum", body: "redelegation:\n  alert_threshold_percent: 100.1\n", wantErr: true},
		{name: "nan", body: "redelegation:\n  alert_threshold_percent: .nan\n", wantErr: true},
		{name: "malformed", body: "redelegation: [\n", wantErr: true},
		{name: "unknown field", body: "redelegation:\n  alert_threshold_percent: 2\n  typo: true\n", wantErr: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			cfg, err := collectorconfig.Load(path)
			if test.wantErr {
				if err == nil {
					t.Fatalf("Load() succeeded with threshold %v, want error", cfg.Redelegation.AlertThresholdPercent)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
			if math.Abs(cfg.Redelegation.AlertThresholdPercent-test.want) > 1e-12 {
				t.Fatalf("threshold = %v, want %v", cfg.Redelegation.AlertThresholdPercent, test.want)
			}
		})
	}
}
