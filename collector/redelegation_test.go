package collector //nolint:testpackage // Tests exercise rolling-window state and parser boundaries.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func redelegationMessage(source, destination, denom, amount string) txSearchMessage {
	return txSearchMessage{
		TypeURL:         redelegationTypeURL,
		SourceValidator: source,
		DestValidator:   destination,
		Amount:          txSearchCoin{Denom: denom, Amount: amount},
	}
}

func TestQueryRedelegationsParsesPaginationMessagesAndFilters(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	pageOneTxs := make([]txSearchTx, redelegationPageLimit)
	pageOneResults := make([]txSearchResult, redelegationPageLimit)
	for i := range redelegationPageLimit {
		pageOneTxs[i] = txSearchTx{Body: txSearchBody{Messages: []txSearchMessage{{TypeURL: "/cosmos.bank.v1beta1.MsgSend"}}}}
		pageOneResults[i] = txSearchResult{
			TxHash:    fmt.Sprintf("other-%03d", i),
			Timestamp: now.Add(-time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
		}
	}
	pageOneTxs[0].Body.Messages = []txSearchMessage{
		redelegationMessage("source-a", "dest-a", "uatom", "1500000"),
		redelegationMessage("source-a", "dest-b", "uosmo", "9000000"),
		redelegationMessage("source-a", "dest-b", "uatom", "2500000"),
	}
	pageOneResults[0].TxHash = "abc"
	pageOneTxs[1].Body.Messages = []txSearchMessage{redelegationMessage("failed", "dest", "uatom", "7000000")}
	pageOneResults[1].Code = 5
	pageOneTxs[2].Body.Messages = []txSearchMessage{redelegationMessage("source-a", "dest", "uatom", "not-a-number")}
	pageOneResults[2].Code = 7 // Failed transactions are ignored before amount parsing.

	var requestedPages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cosmos/tx/v1beta1/txs" {
			http.NotFound(w, r)
			return
		}
		requestedPages = append(requestedPages, r.URL.Query().Get("page"))
		if got := r.URL.Query().Get("query"); got != "message.action='"+redelegationTypeURL+"'" {
			t.Errorf("query = %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != strconv.Itoa(redelegationPageLimit) {
			t.Errorf("limit = %q", got)
		}
		if got := r.URL.Query().Get("order_by"); got != "ORDER_BY_DESC" {
			t.Errorf("order_by = %q", got)
		}

		var response txSearchResponse
		switch r.URL.Query().Get("page") {
		case "1":
			response = txSearchResponse{
				Txs: pageOneTxs, TxResponses: pageOneResults,
				Pagination: Pagination{Total: "101"},
			}
		case "2":
			response = txSearchResponse{
				Txs: []txSearchTx{{Body: txSearchBody{Messages: []txSearchMessage{
					redelegationMessage("source-b", "dest-c", "uatom", "3000000"),
				}}}},
				TxResponses: []txSearchResult{{
					TxHash: "def", Timestamp: now.Add(-100 * time.Minute).Format(time.RFC3339Nano),
				}},
				Pagination: Pagination{Total: "101"},
			}
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	events, err := NewRESTClient(server.URL).QueryRedelegations(now.Add(-3 * time.Hour))
	if err != nil {
		t.Fatalf("QueryRedelegations() error: %v", err)
	}
	if len(requestedPages) != 2 {
		t.Fatalf("requested pages = %v, want [1 2]", requestedPages)
	}
	if len(events) != 3 {
		t.Fatalf("events = %#v, want 3 successful uatom messages", events)
	}

	got := make(map[string]float64)
	for _, event := range events {
		got[fmt.Sprintf("%s:%d", event.TxHash, event.MessageIndex)] = event.AmountUAtom
	}
	if got["ABC:0"] != 1_500_000 || got["ABC:2"] != 2_500_000 || got["DEF:0"] != 3_000_000 {
		t.Fatalf("parsed events = %v", got)
	}
}

func TestQueryRedelegationsEnforcesPaginationBound(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	txs := make([]txSearchTx, redelegationPageLimit)
	results := make([]txSearchResult, redelegationPageLimit)
	for i := range redelegationPageLimit {
		txs[i] = txSearchTx{Body: txSearchBody{Messages: []txSearchMessage{{TypeURL: "/cosmos.bank.v1beta1.MsgSend"}}}}
		results[i] = txSearchResult{TxHash: fmt.Sprintf("%d", i), Timestamp: now.Format(time.RFC3339Nano)}
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(txSearchResponse{
			Txs: txs, TxResponses: results,
			Pagination: Pagination{Total: strconv.Itoa(redelegationMaxPages*redelegationPageLimit + 1)},
		})
	}))
	defer server.Close()

	events, err := NewRESTClient(server.URL).QueryRedelegations(now.Add(-time.Hour))
	if err == nil {
		t.Fatalf("QueryRedelegations() returned %d events, want page-bound error", len(events))
	}
	if events != nil {
		t.Fatalf("events = %#v, want nil when page bound is exceeded", events)
	}
	if calls != redelegationMaxPages {
		t.Fatalf("requests = %d, want bound of %d", calls, redelegationMaxPages)
	}
}

func TestQueryRedelegationsFailsOverBetweenTxSearchEndpoints(t *testing.T) {
	t.Parallel()

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "tx index unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := txSearchResponse{
			Txs: []txSearchTx{{Body: txSearchBody{Messages: []txSearchMessage{
				redelegationMessage("source", "destination", "uatom", "1000000"),
			}}}},
			TxResponses: []txSearchResult{{TxHash: "ok", Timestamp: now.Format(time.RFC3339Nano)}},
			Pagination:  Pagination{Total: "1"},
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer working.Close()

	client := NewRESTClient(failed.URL, working.URL)
	events, err := client.QueryRedelegations(now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("QueryRedelegations() failed instead of rotating: %v", err)
	}
	if len(events) != 1 || client.BaseURL() != working.URL {
		t.Fatalf("events = %#v, selected endpoint = %s", events, client.BaseURL())
	}
}

func TestQueryRedelegationsFailsInsteadOfReturningPartialPage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"txs":[{"body":{"messages":[]}}],"tx_responses":[],"pagination":{"total":"1"}}`))
	}))
	defer server.Close()

	events, err := NewRESTClient(server.URL).QueryRedelegations(time.Time{})
	if err == nil {
		t.Fatalf("QueryRedelegations() returned %#v, want mismatched response error", events)
	}
	if events != nil {
		t.Fatalf("events = %#v, want nil on partial response", events)
	}
}

type redelegationTestIndex struct {
	mu      sync.Mutex
	events  []indexedRedelegation
	failTx  bool
	txCalls int
}

type indexedRedelegation struct {
	hash        string
	timestamp   time.Time
	source      string
	destination string
	amount      string
}

func (index *redelegationTestIndex) handler(w http.ResponseWriter, r *http.Request) {
	index.mu.Lock()
	defer index.mu.Unlock()

	switch r.URL.Path {
	case "/cosmos/tx/v1beta1/txs":
		index.txCalls++
		if index.failTx {
			http.Error(w, "index unavailable", http.StatusServiceUnavailable)
			return
		}
		response := txSearchResponse{Pagination: Pagination{Total: strconv.Itoa(len(index.events))}}
		for _, event := range index.events {
			response.Txs = append(response.Txs, txSearchTx{Body: txSearchBody{Messages: []txSearchMessage{
				redelegationMessage(event.source, event.destination, "uatom", event.amount),
			}}})
			response.TxResponses = append(response.TxResponses, txSearchResult{
				TxHash: event.hash, Timestamp: event.timestamp.Format(time.RFC3339Nano),
			})
		}
		_ = json.NewEncoder(w).Encode(response)
	case "/cosmos/staking/v1beta1/pool":
		_, _ = w.Write([]byte(`{"pool":{"bonded_tokens":"100000000"}}`))
	case "/cosmos/staking/v1beta1/validators":
		_, _ = w.Write([]byte(`{"validators":[{"operator_address":"source-a","description":{"moniker":"Alpha"}},{"operator_address":"source-b","description":{"moniker":"Beta"}},{"operator_address":"dest-a","description":{"moniker":"Destination A"}},{"operator_address":"dest-b","description":{"moniker":"Destination B"}}],"pagination":{"next_key":null}}`))
	default:
		http.NotFound(w, r)
	}
}

func TestRedelegationScannerOverlapExpiryAggregationAndRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	index := &redelegationTestIndex{events: []indexedRedelegation{
		{hash: "b1", timestamp: now.Add(-20 * time.Minute), source: "source-b", destination: "dest-a", amount: "4000000"},
		{hash: "a2", timestamp: now.Add(-30 * time.Minute), source: "source-a", destination: "dest-b", amount: "2000000"},
		{hash: "a1", timestamp: now.Add(-time.Hour), source: "source-a", destination: "dest-a", amount: "1000000"},
	}}
	server := httptest.NewServer(http.HandlerFunc(index.handler))
	defer server.Close()

	metrics := NewMetrics()
	scanner := NewRedelegationScanner(metrics, map[string]string{"source-a": "Verified Alpha"}, 2*time.Hour, 20*time.Minute, 0.02)
	scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("initial Scan() error: %v", err)
	}
	if scanner.eventCount() != 3 {
		t.Fatalf("initial event count = %d, want 3", scanner.eventCount())
	}
	if got := testutil.ToFloat64(metrics.RedelegationOutflowATOM.WithLabelValues("source-a", "Alpha", "Verified Alpha")); got != 3 {
		t.Fatalf("source-a outflow ATOM = %v, want 3", got)
	}
	if got := testutil.ToFloat64(metrics.RedelegationOutflowRatio.WithLabelValues("source-b", "Beta", "unmapped")); got != 0.04 {
		t.Fatalf("source-b outflow ratio = %v, want 0.04", got)
	}
	pairLabels := []string{"source-a", "Alpha", "Verified Alpha", "dest-a", "Destination A", "unmapped"}
	if got := testutil.ToFloat64(metrics.RedelegationEventCount.WithLabelValues(pairLabels...)); got != 1 {
		t.Fatalf("source-a/dest-a event count = %v, want 1", got)
	}

	// The next scan overlaps the previous range. Existing rows are returned by
	// the index again, while the scanner deduplicates by hash and message index.
	index.mu.Lock()
	index.events = append([]indexedRedelegation{{
		hash: "new", timestamp: now.Add(5 * time.Minute), source: "source-a", destination: "dest-a", amount: "500000",
	}}, index.events...)
	index.mu.Unlock()
	if err := scanner.Scan(now.Add(10 * time.Minute)); err != nil {
		t.Fatalf("incremental Scan() error: %v", err)
	}
	if scanner.eventCount() != 4 {
		t.Fatalf("overlap event count = %d, want 4 without duplicates", scanner.eventCount())
	}
	if got := testutil.ToFloat64(metrics.RedelegationOutflowATOM.WithLabelValues("source-a", "Alpha", "Verified Alpha")); got != 3.5 {
		t.Fatalf("source-a outflow after overlap = %v ATOM, want 3.5", got)
	}

	// A fresh scanner has no local checkpoint and rebuilds the full window from
	// the REST index.
	restartedMetrics := NewMetrics()
	restarted := NewRedelegationScanner(restartedMetrics, nil, 2*time.Hour, 20*time.Minute, 0.02)
	restarted.SetEndpoints([]string{server.URL}, []string{server.URL})
	if err := restarted.Scan(now.Add(10 * time.Minute)); err != nil {
		t.Fatalf("restart backfill Scan() error: %v", err)
	}
	if restarted.eventCount() != 4 {
		t.Fatalf("restart event count = %d, want full backfill of 4", restarted.eventCount())
	}

	if err := scanner.Scan(now.Add(3 * time.Hour)); err != nil {
		t.Fatalf("expiry Scan() error: %v", err)
	}
	if scanner.eventCount() != 0 {
		t.Fatalf("expired event count = %d, want 0", scanner.eventCount())
	}
}

func TestRedelegationThresholdCrossingDoesNotRefreshWhileAbove(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	firstCrossing := now.Add(-5 * time.Minute)
	index := &redelegationTestIndex{events: []indexedRedelegation{
		{hash: "cross", timestamp: firstCrossing, source: "source-a", destination: "dest-a", amount: "1000000"},
		{hash: "start", timestamp: now.Add(-10 * time.Minute), source: "source-a", destination: "dest-a", amount: "1000000"},
	}}
	server := httptest.NewServer(http.HandlerFunc(index.handler))
	defer server.Close()

	metrics := NewMetrics()
	scanner := NewRedelegationScanner(metrics, nil, 2*time.Hour, 20*time.Minute, 0.02)
	scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("initial Scan() error: %v", err)
	}
	labels := []string{"source-a", "Alpha", "unmapped"}
	if got := testutil.ToFloat64(metrics.RedelegationThresholdCrossed.WithLabelValues(labels...)); got != float64(firstCrossing.Unix()) {
		t.Fatalf("initial crossing = %v, want %v", got, firstCrossing.Unix())
	}

	latest := now.Add(5 * time.Minute)
	index.mu.Lock()
	index.events = append([]indexedRedelegation{{
		hash: "later", timestamp: latest, source: "source-a", destination: "dest-a", amount: "500000",
	}}, index.events...)
	index.mu.Unlock()
	if err := scanner.Scan(now.Add(10 * time.Minute)); err != nil {
		t.Fatalf("second Scan() error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.RedelegationThresholdCrossed.WithLabelValues(labels...)); got != float64(firstCrossing.Unix()) {
		t.Fatalf("crossing refreshed to %v while continuously above, want %v", got, firstCrossing.Unix())
	}
	if got := testutil.ToFloat64(metrics.RedelegationLatestEvent.WithLabelValues(labels...)); got != float64(latest.Unix()) {
		t.Fatalf("latest event = %v, want %v", got, latest.Unix())
	}
}

func TestRedelegationThresholdCrossingUpdatesAfterTrueRecross(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	index := &redelegationTestIndex{events: []indexedRedelegation{
		{hash: "old-cross", timestamp: now.Add(-5 * time.Minute), source: "source-a", destination: "dest-a", amount: "1000000"},
		{hash: "old-start", timestamp: now.Add(-10 * time.Minute), source: "source-a", destination: "dest-a", amount: "1000000"},
	}}
	server := httptest.NewServer(http.HandlerFunc(index.handler))
	defer server.Close()

	metrics := NewMetrics()
	scanner := NewRedelegationScanner(metrics, nil, 2*time.Hour, 20*time.Minute, 0.02)
	scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("initial Scan() error: %v", err)
	}

	// Both old events expire, taking the rolling outflow below threshold.
	belowTime := now.Add(2 * time.Hour)
	if err := scanner.Scan(belowTime); err != nil {
		t.Fatalf("below-threshold Scan() error: %v", err)
	}

	newCrossing := belowTime.Add(5 * time.Minute)
	index.mu.Lock()
	index.events = append([]indexedRedelegation{
		{hash: "new-cross", timestamp: newCrossing, source: "source-a", destination: "dest-a", amount: "1000000"},
		{hash: "new-start", timestamp: belowTime.Add(time.Minute), source: "source-a", destination: "dest-a", amount: "1000000"},
	}, index.events...)
	index.mu.Unlock()
	if err := scanner.Scan(belowTime.Add(10 * time.Minute)); err != nil {
		t.Fatalf("recross Scan() error: %v", err)
	}
	labels := []string{"source-a", "Alpha", "unmapped"}
	if got := testutil.ToFloat64(metrics.RedelegationThresholdCrossed.WithLabelValues(labels...)); got != float64(newCrossing.Unix()) {
		t.Fatalf("recross timestamp = %v, want %v", got, newCrossing.Unix())
	}
}

func TestRedelegationThresholdCrossingReconstructsOnRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	crossing := now.Add(-30 * time.Minute)
	latest := now.Add(-5 * time.Minute)
	index := &redelegationTestIndex{events: []indexedRedelegation{
		{hash: "latest", timestamp: latest, source: "source-a", destination: "dest-a", amount: "500000"},
		{hash: "cross", timestamp: crossing, source: "source-a", destination: "dest-a", amount: "1000000"},
		{hash: "start", timestamp: now.Add(-time.Hour), source: "source-a", destination: "dest-a", amount: "1000000"},
	}}
	server := httptest.NewServer(http.HandlerFunc(index.handler))
	defer server.Close()

	labels := []string{"source-a", "Alpha", "unmapped"}
	for run := 1; run <= 2; run++ {
		metrics := NewMetrics()
		scanner := NewRedelegationScanner(metrics, nil, 2*time.Hour, 20*time.Minute, 0.02)
		scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
		scanTime := now.Add(time.Duration(run-1) * time.Minute)
		if err := scanner.Scan(scanTime); err != nil {
			t.Fatalf("run %d Scan() error: %v", run, err)
		}
		if got := testutil.ToFloat64(metrics.RedelegationThresholdCrossed.WithLabelValues(labels...)); got != float64(crossing.Unix()) {
			t.Fatalf("run %d reconstructed crossing = %v, want historical event %v", run, got, crossing.Unix())
		}
		if got := testutil.ToFloat64(metrics.RedelegationLatestEvent.WithLabelValues(labels...)); got != float64(latest.Unix()) {
			t.Fatalf("run %d latest event = %v, want %v", run, got, latest.Unix())
		}
	}
}

func TestRedelegationMonikerFallsBackToValidatorAddress(t *testing.T) {
	t.Parallel()

	scanner := NewRedelegationScanner(NewMetrics(), nil, time.Hour, time.Minute, 0.02)
	if got := scanner.monikerName("cosmosvaloper1unknown"); got != "cosmosvaloper1unknown" {
		t.Fatalf("moniker fallback = %q, want validator address", got)
	}
	scanner.monikers["cosmosvaloper1known"] = "Known"
	if got := scanner.monikerName("cosmosvaloper1known"); got != "Known" {
		t.Fatalf("known moniker = %q, want Known", got)
	}
}

func TestRedelegationScanFailureMetricsRetainLastCompleteData(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	index := &redelegationTestIndex{events: []indexedRedelegation{{
		hash: "a1", timestamp: now, source: "source-a", destination: "dest-a", amount: "1000000",
	}}}
	server := httptest.NewServer(http.HandlerFunc(index.handler))
	defer server.Close()

	metrics := NewMetrics()
	scanner := NewRedelegationScanner(metrics, nil, time.Hour, 10*time.Minute, 0.005)
	scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("initial Scan() error: %v", err)
	}
	if got := testutil.ToFloat64(metrics.RedelegationScanSuccess); got != 1 {
		t.Fatalf("scan_success = %v, want 1", got)
	}
	lastSuccess := testutil.ToFloat64(metrics.RedelegationScanLastSuccess)
	outflow := testutil.ToFloat64(metrics.RedelegationOutflowATOM.WithLabelValues("source-a", "Alpha", "unmapped"))

	index.mu.Lock()
	index.failTx = true
	index.mu.Unlock()
	if err := scanner.Scan(now.Add(5 * time.Minute)); err == nil {
		t.Fatal("failed tx index produced a successful scan")
	}
	if got := testutil.ToFloat64(metrics.RedelegationScanSuccess); got != 0 {
		t.Fatalf("scan_success = %v, want 0 after failure", got)
	}
	if got := testutil.ToFloat64(metrics.RedelegationScanLastSuccess); got != lastSuccess {
		t.Fatalf("last_success changed from %v to %v on failure", lastSuccess, got)
	}
	if got := testutil.ToFloat64(metrics.RedelegationOutflowATOM.WithLabelValues("source-a", "Alpha", "unmapped")); got != outflow {
		t.Fatalf("outflow changed from %v to %v after partial scan failure", outflow, got)
	}
	if got := testutil.ToFloat64(metrics.RedelegationAlertThreshold); got != 0.005 {
		t.Fatalf("alert threshold ratio = %v, want 0.005", got)
	}
}
