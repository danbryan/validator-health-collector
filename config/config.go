// Package config loads the collector's Git-backed runtime configuration.
package config

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/danbryan/validator-health-collector/cosmosaddr"
)

const (
	DefaultRedelegationAlertThresholdPercent  = 2.0
	DefaultManagedWarningJailProgressPercent  = 10.0
	DefaultManagedCriticalJailProgressPercent = 80.0
)

// Config is the collector configuration stored in YAML.
type Config struct {
	Redelegation       Redelegation       `yaml:"redelegation"`
	ManagedDelegations ManagedDelegations `yaml:"managed_delegations"`
}

// Redelegation configures rolling redelegation alert metrics.
type Redelegation struct {
	AlertThresholdPercent float64 `yaml:"alert_threshold_percent"`
}

// ManagedDelegations configures risk monitoring for current delegations held by
// known accounts.
type ManagedDelegations struct {
	WarningJailProgressPercent  float64            `yaml:"warning_jail_progress_percent"`
	CriticalJailProgressPercent float64            `yaml:"critical_jail_progress_percent"`
	Accounts                    []ManagedDelegator `yaml:"accounts"`
}

// ManagedDelegator identifies one public Cosmos Hub delegator account.
type ManagedDelegator struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

// Load reads path. An empty path selects the local-development default; a
// supplied path must exist and contain a valid threshold.
func Load(path string) (Config, error) {
	if path == "" {
		return Config{
			Redelegation: Redelegation{
				AlertThresholdPercent: DefaultRedelegationAlertThresholdPercent,
			},
			ManagedDelegations: ManagedDelegations{
				WarningJailProgressPercent:  DefaultManagedWarningJailProgressPercent,
				CriticalJailProgressPercent: DefaultManagedCriticalJailProgressPercent,
			},
		}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config %s: %w", path, err)
	}

	cfg := Config{ManagedDelegations: ManagedDelegations{
		WarningJailProgressPercent:  DefaultManagedWarningJailProgressPercent,
		CriticalJailProgressPercent: DefaultManagedCriticalJailProgressPercent,
	}}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("parsing config %s: %w", path, err)
	}
	threshold := cfg.Redelegation.AlertThresholdPercent
	if math.IsNaN(threshold) || math.IsInf(threshold, 0) || threshold <= 0 || threshold > 100 {
		return Config{}, fmt.Errorf("redelegation.alert_threshold_percent must be greater than 0 and at most 100, got %v", threshold)
	}

	warning := cfg.ManagedDelegations.WarningJailProgressPercent
	critical := cfg.ManagedDelegations.CriticalJailProgressPercent
	if !validPercent(warning) || !validPercent(critical) || warning >= critical {
		return Config{}, fmt.Errorf("managed_delegations jail progress thresholds must satisfy 0 < warning < critical < 100, got warning=%v critical=%v", warning, critical)
	}

	names := make(map[string]bool, len(cfg.ManagedDelegations.Accounts))
	addresses := make(map[string]bool, len(cfg.ManagedDelegations.Accounts))
	for i := range cfg.ManagedDelegations.Accounts {
		account := &cfg.ManagedDelegations.Accounts[i]
		account.Name = strings.TrimSpace(account.Name)
		account.Address = strings.TrimSpace(account.Address)
		if account.Name == "" {
			return Config{}, fmt.Errorf("managed_delegations.accounts[%d].name must be nonempty", i)
		}
		if account.Address == "" {
			return Config{}, fmt.Errorf("managed_delegations.accounts[%d].address must be nonempty", i)
		}
		hrp, data, err := cosmosaddr.Decode(account.Address)
		if err != nil || !strings.EqualFold(hrp, "cosmos") || len(data) != 20 {
			return Config{}, fmt.Errorf("managed_delegations.accounts[%d].address must be a valid cosmos account address", i)
		}
		account.Address, err = cosmosaddr.Encode("cosmos", data)
		if err != nil {
			return Config{}, fmt.Errorf("canonicalizing managed_delegations.accounts[%d].address: %w", i, err)
		}
		if names[account.Name] {
			return Config{}, fmt.Errorf("managed_delegations account name %q is duplicated", account.Name)
		}
		if addresses[account.Address] {
			return Config{}, fmt.Errorf("managed_delegations account address %q is duplicated", account.Address)
		}
		names[account.Name] = true
		addresses[account.Address] = true
	}
	return cfg, nil
}

func validPercent(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0 && value < 100
}
