// Package config loads the collector's Git-backed runtime configuration.
package config

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"

	"gopkg.in/yaml.v3"
)

const DefaultRedelegationAlertThresholdPercent = 2.0

// Config is the collector configuration stored in YAML.
type Config struct {
	Redelegation Redelegation `yaml:"redelegation"`
}

// Redelegation configures rolling redelegation alert metrics.
type Redelegation struct {
	AlertThresholdPercent float64 `yaml:"alert_threshold_percent"`
}

// Load reads path. An empty path selects the local-development default; a
// supplied path must exist and contain a valid threshold.
func Load(path string) (Config, error) {
	if path == "" {
		return Config{Redelegation: Redelegation{
			AlertThresholdPercent: DefaultRedelegationAlertThresholdPercent,
		}}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config %s: %w", path, err)
	}

	var cfg Config
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
	return cfg, nil
}
