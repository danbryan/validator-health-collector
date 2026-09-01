package endpoints

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	probeTimeout     = 5 * time.Second
	probeConcurrency = 8

	// maxBlockLag is how far behind the wall clock a node's latest block may be
	// before it is treated as unusable. Cosmos Hub blocks are roughly 6s, so two
	// minutes is generous.
	maxBlockLag = 2 * time.Minute
)

// poolProbeBody is the part of a staking pool response the REST probe reads.
type poolProbeBody struct {
	BondedTokens string `json:"bonded_tokens"`
}

// poolProbeResponse wraps the staking pool probe response.
type poolProbeResponse struct {
	Pool poolProbeBody `json:"pool"`
}

// txSearchProbeResponse distinguishes a working transaction index from an
// ordinary REST endpoint. Non-nil arrays prove that the endpoint understood the
// tx-search response shape even when no matching transaction exists.
type txSearchProbeResponse struct {
	Txs         []json.RawMessage `json:"txs"`
	TxResponses []json.RawMessage `json:"tx_responses"`
}

// statusProbeSyncInfo is the part of a CometBFT status response the RPC probe reads.
type statusProbeSyncInfo struct {
	LatestBlockHeight   string `json:"latest_block_height"`
	LatestBlockTime     string `json:"latest_block_time"`
	EarliestBlockHeight string `json:"earliest_block_height"`
	CatchingUp          bool   `json:"catching_up"`
}

// statusProbeResult is the body of a CometBFT status response.
type statusProbeResult struct {
	SyncInfo statusProbeSyncInfo `json:"sync_info"`
}

// statusProbeResponse wraps a CometBFT status probe response.
type statusProbeResponse struct {
	Result statusProbeResult `json:"result"`
}

// Candidate is an endpoint that has been probed.
type Candidate struct {
	Endpoint

	Healthy bool
	Err     string
	Latency time.Duration

	// RPC only. EarliestHeight is what the node reports as the oldest block it
	// retains. Note that some providers report 0 here while actually pruning, so
	// it is a ranking hint, never a guarantee.
	EarliestHeight int64
	LatestHeight   int64

	// TxSearch is independent of Healthy. An endpoint can serve staking and
	// governance correctly while lacking a transaction index.
	TxSearch    bool
	TxSearchErr string
}

// Addresses returns the addresses of the healthy candidates, in ranked order.
func Addresses(cands []Candidate) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		if c.Healthy {
			out = append(out, c.Address)
		}
	}
	return out
}

// TxSearchAddresses returns only endpoints whose transaction-search index
// answered the dedicated capability probe.
func TxSearchAddresses(cands []Candidate) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		if c.TxSearch {
			out = append(out, c.Address)
		}
	}
	return out
}

// Lookup maps an address back to its provider name, for labelling metrics.
func Lookup(cands []Candidate, address string) (Candidate, bool) {
	for _, c := range cands {
		if c.Address == address {
			return c, true
		}
	}
	return Candidate{}, false
}

// probeAll runs fn against every endpoint with bounded concurrency.
func probeAll(eps []Endpoint, fn func(Endpoint) Candidate) []Candidate {
	out := make([]Candidate, len(eps))
	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup

	for i, ep := range eps {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = fn(ep)
		})
	}
	wg.Wait()
	return out
}

func newProbeClient() *http.Client {
	return NewHTTPClient(probeTimeout)
}

// getJSON issues a GET and decodes the body, capping how much is read so a
// misbehaving endpoint cannot stall the probe.
func getJSON(client *http.Client, url string, dst any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return &statusError{code: resp.StatusCode}
	}
	if dst == nil {
		return nil
	}
	return json.Unmarshal(body, dst)
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "status " + strconv.Itoa(e.code) }

// ProbeREST health checks REST endpoints and returns them ranked fastest first.
//
// The pool query is used rather than only node_info because it exercises the
// staking module the collector actually depends on; an endpoint that answers
// node_info but not staking queries is no use here.
func ProbeREST(eps []Endpoint) []Candidate {
	cands := probeAll(eps, func(ep Endpoint) Candidate {
		c := Candidate{Endpoint: ep}
		client := newProbeClient()
		start := time.Now()

		var pool poolProbeResponse
		if err := getJSON(client, ep.Address+"/cosmos/staking/v1beta1/pool", &pool); err != nil {
			c.Err = err.Error()
			return c
		}
		if pool.Pool.BondedTokens == "" {
			c.Err = "pool response missing bonded_tokens"
			return c
		}

		c.Latency = time.Since(start)
		c.Healthy = true
		probeTxSearch(client, &c)
		return c
	})

	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].Healthy != cands[j].Healthy {
			return cands[i].Healthy
		}
		return cands[i].Latency < cands[j].Latency
	})
	return cands
}

// ProbeTxSearch probes only transaction-search capability. It is used when
// ordinary REST health is already established or trusted separately.
func ProbeTxSearch(eps []Endpoint) []Candidate {
	cands := probeAll(eps, func(ep Endpoint) Candidate {
		c := Candidate{Endpoint: ep}
		start := time.Now()
		probeTxSearch(newProbeClient(), &c)
		c.Latency = time.Since(start)
		return c
	})
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].TxSearch != cands[j].TxSearch {
			return cands[i].TxSearch
		}
		return cands[i].Latency < cands[j].Latency
	})
	return cands
}

func probeTxSearch(client *http.Client, candidate *Candidate) {
	params := url.Values{}
	params.Set("query", "message.action='/cosmos.staking.v1beta1.MsgBeginRedelegate'")
	params.Set("page", "1")
	params.Set("limit", "1")
	params.Set("order_by", "ORDER_BY_DESC")

	var response txSearchProbeResponse
	path := "/cosmos/tx/v1beta1/txs?" + params.Encode()
	if err := getJSON(client, candidate.Address+path, &response); err != nil {
		candidate.TxSearchErr = err.Error()
		return
	}
	if response.Txs == nil || response.TxResponses == nil {
		candidate.TxSearchErr = "tx search response missing txs or tx_responses"
		return
	}
	candidate.TxSearch = true
}

// ProbeRPC health checks CometBFT RPC endpoints and returns them ranked fastest
// first. The collector reads only current state and a recent block-time sample,
// so archive depth is not a dependency.
func ProbeRPC(eps []Endpoint) []Candidate {
	cands := probeAll(eps, func(ep Endpoint) Candidate {
		c := Candidate{Endpoint: ep}
		client := newProbeClient()
		start := time.Now()

		var status statusProbeResponse
		if err := getJSON(client, ep.Address+"/status", &status); err != nil {
			c.Err = err.Error()
			return c
		}

		si := status.Result.SyncInfo
		if si.CatchingUp {
			c.Err = "node is catching up"
			return c
		}

		blockTime, err := time.Parse(time.RFC3339Nano, si.LatestBlockTime)
		if err != nil {
			c.Err = "unparseable latest_block_time"
			return c
		}
		if lag := time.Since(blockTime); lag > maxBlockLag {
			c.Err = "latest block is " + lag.Truncate(time.Second).String() + " behind"
			return c
		}

		c.LatestHeight, _ = strconv.ParseInt(si.LatestBlockHeight, 10, 64)
		c.EarliestHeight, _ = strconv.ParseInt(si.EarliestBlockHeight, 10, 64)
		if c.LatestHeight == 0 {
			c.Err = "no latest_block_height"
			return c
		}

		c.Latency = time.Since(start)
		c.Healthy = true
		return c
	})

	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.Healthy != b.Healthy {
			return a.Healthy
		}
		return a.Latency < b.Latency
	})
	return cands
}
