package config_test

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	collectorconfig "github.com/danbryan/validator-health-collector/config"
	"github.com/danbryan/validator-health-collector/cosmosaddr"
)

const validAccount = "cosmos1q6d3d089hg59x6gcx92uumx70s5y5wadntgvtr"

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func completeConfig(redelegation, warning, critical string, accounts string) string {
	return fmt.Sprintf("redelegation:\n  alert_threshold_percent: %s\nmanaged_delegations:\n  warning_jail_progress_percent: %s\n  critical_jail_progress_percent: %s\n%s", redelegation, warning, critical, accounts)
}

func TestLoadUsesDefaultOnlyWithoutPath(t *testing.T) {
	t.Parallel()

	cfg, err := collectorconfig.Load("")
	if err != nil {
		t.Fatalf("Load(\"\") returned error: %v", err)
	}
	if cfg.Redelegation.AlertThresholdPercent != 2 {
		t.Fatalf("default redelegation threshold = %v, want 2", cfg.Redelegation.AlertThresholdPercent)
	}
	if cfg.ManagedDelegations.WarningJailProgressPercent != 10 || cfg.ManagedDelegations.CriticalJailProgressPercent != 80 {
		t.Fatalf("default managed thresholds = %v/%v, want 10/80", cfg.ManagedDelegations.WarningJailProgressPercent, cfg.ManagedDelegations.CriticalJailProgressPercent)
	}
	if len(cfg.ManagedDelegations.Accounts) != 0 {
		t.Fatalf("default managed accounts = %#v, want none", cfg.ManagedDelegations.Accounts)
	}

	if _, err := collectorconfig.Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("Load(missing path) succeeded, want an error for an explicitly supplied file")
	}
}

func TestLoadKeepsManagedDefaultsForLegacyConfig(t *testing.T) {
	t.Parallel()

	cfg, err := collectorconfig.Load(writeConfig(t, "redelegation:\n  alert_threshold_percent: 2\n"))
	if err != nil {
		t.Fatalf("Load() returned error for a legacy config: %v", err)
	}
	if cfg.ManagedDelegations.WarningJailProgressPercent != 10 || cfg.ManagedDelegations.CriticalJailProgressPercent != 80 {
		t.Fatalf("managed thresholds = %v/%v, want defaults 10/80", cfg.ManagedDelegations.WarningJailProgressPercent, cfg.ManagedDelegations.CriticalJailProgressPercent)
	}
	if len(cfg.ManagedDelegations.Accounts) != 0 {
		t.Fatalf("managed accounts = %#v, want none", cfg.ManagedDelegations.Accounts)
	}
}

func TestLoadValidConfiguration(t *testing.T) {
	t.Parallel()

	second, err := cosmosaddr.Encode("cosmos", []byte("abcdefghijklmnopqrst"))
	if err != nil {
		t.Fatalf("creating second test address: %v", err)
	}
	body := completeConfig("0.5", "12.5", "90", fmt.Sprintf("  accounts:\n    - name: Treasury\n      address: %s\n    - name: Reserve\n      address: %s\n", validAccount, second))
	cfg, err := collectorconfig.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.Redelegation.AlertThresholdPercent != 0.5 || cfg.ManagedDelegations.WarningJailProgressPercent != 12.5 || cfg.ManagedDelegations.CriticalJailProgressPercent != 90 {
		t.Fatalf("loaded thresholds = %#v", cfg)
	}
	if len(cfg.ManagedDelegations.Accounts) != 2 || cfg.ManagedDelegations.Accounts[1].Name != "Reserve" {
		t.Fatalf("loaded accounts = %#v", cfg.ManagedDelegations.Accounts)
	}
}

func TestLoadTrimsAndCanonicalizesManagedAccounts(t *testing.T) {
	t.Parallel()

	body := completeConfig("2", "10", "80", fmt.Sprintf("  accounts:\n    - name: %q\n      address: %q\n", " Treasury ", "  "+strings.ToUpper(validAccount)+"  "))
	cfg, err := collectorconfig.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if len(cfg.ManagedDelegations.Accounts) != 1 {
		t.Fatalf("accounts = %#v, want one", cfg.ManagedDelegations.Accounts)
	}
	account := cfg.ManagedDelegations.Accounts[0]
	if account.Name != "Treasury" || account.Address != validAccount {
		t.Fatalf("normalized account = %#v, want trimmed name and lowercase canonical address", account)
	}
}

func TestLoadValidatesRedelegationThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    float64
		wantErr bool
	}{
		{name: "integer", value: "2", want: 2},
		{name: "decimal", value: "0.5", want: 0.5},
		{name: "maximum", value: "100", want: 100},
		{name: "missing", value: "0", wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "over maximum", value: "100.1", wantErr: true},
		{name: "nan", value: ".nan", wantErr: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := collectorconfig.Load(writeConfig(t, completeConfig(test.value, "10", "80", "  accounts: []\n")))
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

func TestLoadRejectsManagedThresholds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, warning, critical string
	}{
		{name: "missing", warning: "0", critical: "0"},
		{name: "zero warning", warning: "0", critical: "80"},
		{name: "equal", warning: "80", critical: "80"},
		{name: "reversed", warning: "90", critical: "80"},
		{name: "critical 100", warning: "10", critical: "100"},
		{name: "negative", warning: "-1", critical: "80"},
		{name: "nan", warning: ".nan", critical: "80"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := collectorconfig.Load(writeConfig(t, completeConfig("2", test.warning, test.critical, "  accounts: []\n"))); err == nil {
				t.Fatal("Load() succeeded, want managed threshold error")
			}
		})
	}
}

func TestLoadRejectsInvalidManagedAccounts(t *testing.T) {
	t.Parallel()

	valoper, err := cosmosaddr.Reprefix(validAccount, "cosmosvaloper")
	if err != nil {
		t.Fatalf("Reprefix: %v", err)
	}
	for _, test := range []struct {
		name     string
		accounts string
	}{
		{name: "empty name", accounts: fmt.Sprintf("  accounts:\n    - name: \"\"\n      address: %s\n", validAccount)},
		{name: "empty address", accounts: "  accounts:\n    - name: Treasury\n      address: \"\"\n"},
		{name: "invalid bech32", accounts: "  accounts:\n    - name: Treasury\n      address: cosmos1invalid\n"},
		{name: "invalid hrp", accounts: fmt.Sprintf("  accounts:\n    - name: Treasury\n      address: %s\n", valoper)},
		{name: "duplicate name", accounts: fmt.Sprintf("  accounts:\n    - name: Treasury\n      address: %s\n    - name: Treasury\n      address: %s\n", validAccount, "cosmos19rl4cm2hmr8afy4kldpxz3fka4jguq0a2ks6xu")},
		{name: "duplicate address", accounts: fmt.Sprintf("  accounts:\n    - name: Treasury\n      address: %s\n    - name: Reserve\n      address: %s\n", validAccount, validAccount)},
		{name: "mixed-case duplicate canonical address", accounts: fmt.Sprintf("  accounts:\n    - name: Treasury\n      address: %s\n    - name: Reserve\n      address: %s\n", validAccount, strings.ToUpper(validAccount))},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := collectorconfig.Load(writeConfig(t, completeConfig("2", "10", "80", test.accounts))); err == nil {
				t.Fatal("Load() succeeded, want account validation error")
			}
		})
	}
}

func TestLoadRejectsMalformedAndUnknownFields(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		"redelegation: [\n",
		completeConfig("2", "10", "80", "  accounts: []\n  typo: true\n"),
		completeConfig("2", "10", "80", "  accounts:\n    - name: Treasury\n      address: "+validAccount+"\n      typo: true\n"),
		completeConfig("2", "10", "80", "  accounts: []\n") + "unexpected: true\n",
	} {
		if _, err := collectorconfig.Load(writeConfig(t, body)); err == nil {
			t.Fatalf("Load() succeeded for invalid body:\n%s", body)
		}
	}
}
