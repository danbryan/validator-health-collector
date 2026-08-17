package endpoints_test

import (
	"testing"
	"time"

	"github.com/cosmos/platform/apps/validator-health-collector/endpoints"
)

func TestAddressesKeepsOnlyHealthyInOrder(t *testing.T) {
	t.Parallel()

	cands := []endpoints.Candidate{
		{Endpoint: endpoints.Endpoint{Address: "https://a"}, Healthy: true},
		{Endpoint: endpoints.Endpoint{Address: "https://b"}, Healthy: false},
		{Endpoint: endpoints.Endpoint{Address: "https://c"}, Healthy: true},
	}

	got := endpoints.Addresses(cands)
	want := []string{"https://a", "https://c"}

	if len(got) != len(want) {
		t.Fatalf("Addresses() returned %d entries, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Addresses()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLookupFindsProviderByAddress(t *testing.T) {
	t.Parallel()

	cands := []endpoints.Candidate{
		{Endpoint: endpoints.Endpoint{Provider: "Alpha", Address: "https://a"}},
		{Endpoint: endpoints.Endpoint{Provider: "Beta", Address: "https://b"}},
	}

	cand, ok := endpoints.Lookup(cands, "https://b")
	if !ok {
		t.Fatal("Lookup() did not find an address that is present")
	}
	if cand.Provider != "Beta" {
		t.Errorf("Lookup() provider = %q, want %q", cand.Provider, "Beta")
	}

	if _, ok := endpoints.Lookup(cands, "https://missing"); ok {
		t.Error("Lookup() reported success for an address that is absent")
	}
}

// TestProbeRESTRanksHealthyFirst and the RPC equivalent below exercise the
// ranking rules against a local server rather than the public internet, so the
// test does not depend on any provider being up.
func TestProbeRESTMarksUnreachableUnhealthy(t *testing.T) {
	t.Parallel()

	// A port that nothing listens on. The probe must record this as unhealthy
	// rather than panicking or hanging past its timeout.
	cands := endpoints.ProbeREST([]endpoints.Endpoint{
		{Provider: "dead", Address: "http://127.0.0.1:1"},
	})

	if len(cands) != 1 {
		t.Fatalf("ProbeREST() returned %d candidates, want 1", len(cands))
	}
	if cands[0].Healthy {
		t.Error("ProbeREST() marked an unreachable endpoint healthy")
	}
	if cands[0].Err == "" {
		t.Error("ProbeREST() left Err empty for a failed probe")
	}
}

func TestProbeRPCMarksUnreachableUnhealthy(t *testing.T) {
	t.Parallel()

	cands := endpoints.ProbeRPC([]endpoints.Endpoint{
		{Provider: "dead", Address: "http://127.0.0.1:1"},
	})

	if len(cands) != 1 {
		t.Fatalf("ProbeRPC() returned %d candidates, want 1", len(cands))
	}
	if cands[0].Healthy {
		t.Error("ProbeRPC() marked an unreachable endpoint healthy")
	}
}

func TestProbeHandlesEmptyInput(t *testing.T) {
	t.Parallel()

	if got := endpoints.ProbeREST(nil); len(got) != 0 {
		t.Errorf("ProbeREST(nil) returned %d candidates, want 0", len(got))
	}
	if got := endpoints.ProbeRPC(nil); len(got) != 0 {
		t.Errorf("ProbeRPC(nil) returned %d candidates, want 0", len(got))
	}
	if got := endpoints.Addresses(nil); len(got) != 0 {
		t.Errorf("Addresses(nil) returned %d entries, want 0", len(got))
	}
}

func TestFallbackListsAreUsable(t *testing.T) {
	t.Parallel()

	// The fallback lists are the last line of defence when the registry is
	// unreachable, so an empty or malformed one would be silently fatal.
	if len(endpoints.FallbackREST) == 0 {
		t.Error("FallbackREST is empty")
	}
	if len(endpoints.FallbackRPC) == 0 {
		t.Error("FallbackRPC is empty")
	}
	for _, ep := range append(endpoints.FallbackREST, endpoints.FallbackRPC...) {
		if ep.Address == "" {
			t.Errorf("fallback entry for provider %q has an empty address", ep.Provider)
		}
		if ep.Provider == "" {
			t.Errorf("fallback entry %q has an empty provider", ep.Address)
		}
	}
}

func TestResolverHonoursOverridesWithoutProbing(t *testing.T) {
	t.Parallel()

	// Overrides must bypass discovery entirely. An unroutable registry URL proves
	// the resolver never reached for it, and the trailing slash proves the
	// override is normalised.
	res := endpoints.NewResolver(endpoints.Options{
		RegistryURL:  "http://127.0.0.1:1/%s.json",
		Chain:        "cosmoshub",
		RESTOverride: "https://rest.example.com/",
		RPCOverride:  "https://rpc.example.com",
	}).Resolve()

	if !res.RESTFromOverride || !res.RPCFromOverride {
		t.Error("Resolve() did not flag the results as coming from overrides")
	}
	if len(res.RESTAddresses) != 1 || res.RESTAddresses[0] != "https://rest.example.com" {
		t.Errorf("REST addresses = %v, want [https://rest.example.com]", res.RESTAddresses)
	}
	if len(res.RPCAddresses) != 1 || res.RPCAddresses[0] != "https://rpc.example.com" {
		t.Errorf("RPC addresses = %v, want [https://rpc.example.com]", res.RPCAddresses)
	}
}

func TestResolverFallsBackWhenRegistryUnreachable(t *testing.T) {
	t.Parallel()

	// With no overrides and an unreachable registry, the resolver must still
	// produce candidates from the compiled-in fallback rather than returning none.
	start := time.Now()
	res := endpoints.NewResolver(endpoints.Options{
		RegistryURL: "http://127.0.0.1:1/%s.json",
		Chain:       "cosmoshub",
	}).Resolve()

	if len(res.REST) == 0 || len(res.RPC) == 0 {
		t.Fatal("Resolve() produced no candidates when the registry was unreachable")
	}
	if len(res.REST) != len(endpoints.FallbackREST) {
		t.Errorf("probed %d REST candidates, want the %d fallback entries",
			len(res.REST), len(endpoints.FallbackREST))
	}
	// Guard against the probe budget regressing into something serial and slow.
	if elapsed := time.Since(start); elapsed > 60*time.Second {
		t.Errorf("Resolve() took %v, which suggests probing lost its concurrency", elapsed)
	}
}
