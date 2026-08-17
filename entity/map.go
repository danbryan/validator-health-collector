// Package entity groups validators into the economic entities that actually
// control them, so concentration is measured per operator rather than per
// validator address.
package entity

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Map is the on-disk entity grouping file.
type Map struct {
	Entities []Entry `yaml:"entities"`
}

// Entry is one entity and the validator addresses it controls.
type Entry struct {
	Name       string   `yaml:"name"`
	Validators []string `yaml:"validators"`
}

// Load reads the entity map and flattens it into a validator-address to
// entity-name lookup.
//
// Any validator absent from the file becomes its own entity, which the caller
// handles by falling back to the moniker. That default is deliberate: an
// unlisted validator should count as independent rather than silently join a
// group.
func Load(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading entity map: %w", err)
	}

	var cfg Map
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing entity map: %w", err)
	}

	result := make(map[string]string)
	for _, e := range cfg.Entities {
		for _, addr := range e.Validators {
			result[addr] = e.Name
		}
	}
	return result, nil
}
