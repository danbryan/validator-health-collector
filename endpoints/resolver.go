package endpoints

import (
	"log"
	"sync"
	"time"
)

// registryTTL is how long a fetched registry entry is reused before refetching.
// The endpoint list changes on the order of weeks, so this is deliberately long;
// health is re-probed every cycle regardless.
const registryTTL = 24 * time.Hour

// Resolution is the outcome of one resolve pass.
type Resolution struct {
	REST []Candidate
	RPC  []Candidate

	// RESTAddresses and RPCAddresses are the healthy addresses in ranked order,
	// ready to hand to a rotating client.
	RESTAddresses []string
	RPCAddresses  []string

	// FromOverride reports whether operator-supplied flags replaced discovery.
	RESTFromOverride bool
	RPCFromOverride  bool
}

// Options configures a Resolver.
type Options struct {
	RegistryURL string
	Chain       string

	// RESTOverride and RPCOverride bypass discovery entirely when set. They exist
	// so an operator can pin an endpoint, not as the normal path.
	RESTOverride string
	RPCOverride  string
}

// Resolver discovers and ranks endpoints, caching the registry lookup.
type Resolver struct {
	opts Options

	mu           sync.Mutex
	cachedREST   []Endpoint
	cachedRPC    []Endpoint
	cachedAt     time.Time
	usedFallback bool
}

func NewResolver(opts Options) *Resolver {
	if opts.RegistryURL == "" {
		opts.RegistryURL = DefaultRegistryURL
	}
	if opts.Chain == "" {
		opts.Chain = "cosmoshub"
	}
	return &Resolver{opts: opts}
}

// candidates returns the endpoint lists to probe, refetching the registry when
// the cache has expired. A failed refetch keeps serving the previous list, and
// falls back to the compiled-in list only if nothing has ever been fetched.
func (r *Resolver) candidates() ChainAPIs {
	r.mu.Lock()
	defer r.mu.Unlock()

	cached := ChainAPIs{REST: r.cachedREST, RPC: r.cachedRPC}
	haveCache := len(r.cachedREST) > 0 && len(r.cachedRPC) > 0

	if haveCache && time.Since(r.cachedAt) < registryTTL {
		return cached
	}

	apis, err := FetchChain(r.opts.RegistryURL, r.opts.Chain)
	switch {
	case err != nil:
		log.Printf("WARN: endpoints: registry lookup failed: %v", err)
	case len(apis.REST) == 0 || len(apis.RPC) == 0:
		log.Printf("WARN: endpoints: registry entry for %s advertises no usable endpoints", r.opts.Chain)
	default:
		r.cachedREST, r.cachedRPC = apis.REST, apis.RPC
		r.cachedAt = time.Now()
		r.usedFallback = false
		log.Printf("endpoints: registry %s advertises %d REST and %d RPC endpoints",
			r.opts.Chain, len(apis.REST), len(apis.RPC))
		return apis
	}

	if haveCache {
		log.Print("endpoints: reusing the previously fetched registry list")
		return cached
	}

	if !r.usedFallback {
		log.Print("WARN: endpoints: no registry data, using the compiled-in fallback list")
		r.usedFallback = true
	}
	return ChainAPIs{REST: FallbackREST, RPC: FallbackRPC}
}

// Resolve probes the current candidate lists and returns them ranked.
//
// Overrides short-circuit probing for the protocol they cover: a pinned endpoint
// is used as given, on the assumption that the operator meant it.
func (r *Resolver) Resolve() Resolution {
	var res Resolution

	apis := r.candidates()

	if r.opts.RESTOverride != "" {
		res.RESTFromOverride = true
		res.REST = []Candidate{{
			Endpoint: Endpoint{Provider: "override", Address: trimSlash(r.opts.RESTOverride)},
			Healthy:  true,
		}}
	} else {
		res.REST = ProbeREST(apis.REST)
	}

	if r.opts.RPCOverride != "" {
		res.RPCFromOverride = true
		res.RPC = []Candidate{{
			Endpoint: Endpoint{Provider: "override", Address: trimSlash(r.opts.RPCOverride)},
			Healthy:  true,
		}}
	} else {
		res.RPC = ProbeRPC(apis.RPC)
	}

	res.RESTAddresses = Addresses(res.REST)
	res.RPCAddresses = Addresses(res.RPC)

	log.Printf("endpoints: %d/%d REST healthy, %d/%d RPC healthy",
		len(res.RESTAddresses), len(res.REST), len(res.RPCAddresses), len(res.RPC))

	if len(res.RPCAddresses) > 0 {
		top, _ := Lookup(res.RPC, res.RPCAddresses[0])
		log.Printf("endpoints: preferred RPC %s (%s), retains from height %d",
			top.Address, top.Provider, top.EarliestHeight)
	}

	return res
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
