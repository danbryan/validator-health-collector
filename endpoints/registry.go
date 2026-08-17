// Package endpoints discovers Cosmos chain API endpoints from the chain registry
// and picks healthy ones at request time, so the collector does not depend on a
// hardcoded node.
package endpoints

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultRegistryURL is the chain.json template for the cosmos chain registry.
// %s is replaced with the chain name.
const DefaultRegistryURL = "https://raw.githubusercontent.com/cosmos/chain-registry/master/%s/chain.json"

const (
	registryTimeout = 20 * time.Second

	// maxRegistryBody caps how much of a registry response is read, so a wrong
	// or hostile URL cannot exhaust memory.
	maxRegistryBody = 4 << 20
)

// Endpoint is one API server advertised by the registry.
type Endpoint struct {
	Provider string
	Address  string
}

// chainJSON is the subset of the registry chain.json this package reads.
type chainJSON struct {
	ChainName string          `json:"chain_name"`
	ChainID   string          `json:"chain_id"`
	APIs      registryAPISets `json:"apis"`
}

// registryAPISets groups the endpoint lists a registry entry advertises.
type registryAPISets struct {
	RPC  []registryAPI `json:"rpc"`
	REST []registryAPI `json:"rest"`
}

type registryAPI struct {
	Address  string `json:"address"`
	Provider string `json:"provider"`
}

// ChainAPIs holds the endpoints a chain advertises in the registry.
type ChainAPIs struct {
	REST []Endpoint
	RPC  []Endpoint
}

// FetchChain reads the registry entry for a chain and returns its advertised
// REST and RPC endpoints. registryURL must contain a single %s for the chain name.
func FetchChain(registryURL, chain string) (ChainAPIs, error) {
	url := fmt.Sprintf(registryURL, chain)

	client := &http.Client{Timeout: registryTimeout}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return ChainAPIs{}, fmt.Errorf("building registry request: %w", err)
	}
	// The registry URL is an operator-supplied flag with a pinned default, not
	// attacker-controlled input.
	resp, err := client.Do(req) //nolint:gosec // G704: operator-configured registry URL
	if err != nil {
		return ChainAPIs{}, fmt.Errorf("fetching registry %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return ChainAPIs{}, fmt.Errorf("registry %s returned status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRegistryBody))
	if err != nil {
		return ChainAPIs{}, fmt.Errorf("reading registry response: %w", err)
	}

	var cj chainJSON
	if err := json.Unmarshal(body, &cj); err != nil {
		return ChainAPIs{}, fmt.Errorf("parsing registry chain.json: %w", err)
	}

	return ChainAPIs{
		REST: convert(cj.APIs.REST),
		RPC:  convert(cj.APIs.RPC),
	}, nil
}

func convert(in []registryAPI) []Endpoint {
	out := make([]Endpoint, 0, len(in))
	for _, a := range in {
		addr := strings.TrimSpace(a.Address)
		if addr == "" {
			continue
		}
		// Plain-HTTP endpoints are dropped: the collector runs in-cluster and
		// should not send queries over an unencrypted link.
		if !strings.HasPrefix(addr, "https://") {
			continue
		}
		out = append(out, Endpoint{
			Provider: strings.TrimSpace(a.Provider),
			Address:  strings.TrimRight(addr, "/"),
		})
	}
	return out
}

// FallbackREST and FallbackRPC are used only when the registry itself is
// unreachable, so a GitHub outage does not become a collector outage. They are
// deliberately short and are not a substitute for discovery.
//
// Chosen because each was observed serving Cosmos Hub mainnet on 2026-08-17.
// The RPC list is ordered deepest-history-first, since the governance vote
// backfill needs a node whose transaction index still covers the proposal.
var (
	FallbackREST = []Endpoint{
		{Provider: "Allnodes", Address: "https://cosmos-rest.publicnode.com"},
		{Provider: "Lavender.Five", Address: "https://rest.lavenderfive.com/cosmoshub"},
		{Provider: "kjnodes", Address: "https://cosmoshub.api.kjnodes.com"},
		{Provider: "Polkachu", Address: "https://cosmos-api.polkachu.com"},
	}

	FallbackRPC = []Endpoint{
		{Provider: "Citizen Web3 archive", Address: "https://rpc.cosmoshub-4-archive.citizenweb3.com"},
		{Provider: "Allnodes", Address: "https://cosmos-rpc.publicnode.com"},
		{Provider: "Lavender.Five", Address: "https://rpc.lavenderfive.com/cosmoshub"},
		{Provider: "kjnodes", Address: "https://cosmoshub.rpc.kjnodes.com"},
	}
)
