package collector //nolint:testpackage // Scanner tests exercise publication and state transitions.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	collectorconfig "github.com/danbryan/validator-health-collector/config"
)

type managedTestLock struct {
	destination string
	amount      string
	completion  time.Time
}

type managedTestChain struct {
	mu sync.Mutex

	delegations      map[string]map[string]string
	locks            map[string][]managedTestLock
	validators       []Validator
	signing          map[string]SigningInfo
	activeHex        map[string]bool
	consensusSets    map[int64]map[string]bool
	commits          map[int64][]CometCommitSignature
	commitHeights    []int64
	validatorHeights []int64
	height           int64
	blockTime        time.Time
	failAccount      string
	failCommitHeight int64
	failRewards      bool
}

func newManagedTestChain(t *testing.T) (*managedTestChain, Validator, Validator, string, string) {
	t.Helper()
	validatorA, consensusA, hexA := managedTestValidator(t, "cosmosvaloper1alpha", "Alpha", 1, false, "0.10")
	validatorB, consensusB, _ := managedTestValidator(t, "cosmosvaloper1beta", "Beta", 2, true, "0.20")
	chain := &managedTestChain{
		delegations: make(map[string]map[string]string),
		locks:       make(map[string][]managedTestLock),
		validators:  []Validator{validatorA, validatorB},
		signing: map[string]SigningInfo{
			consensusA: {Address: consensusA, MissedBlocksCounter: "45"},
			consensusB: {Address: consensusB, MissedBlocksCounter: "20", Tombstoned: true},
		},
		activeHex:     map[string]bool{hexA: true},
		consensusSets: make(map[int64]map[string]bool),
		commits:       make(map[int64][]CometCommitSignature),
		height:        10_000,
		blockTime:     time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	}
	for height := chain.height; height > chain.height-managedRecentCommitCount; height-- {
		chain.commits[height] = []CometCommitSignature{{BlockIDFlag: 2, ValidatorAddress: hexA}}
	}
	return chain, validatorA, validatorB, consensusA, consensusB
}

func managedTestValidator(t *testing.T, operator, moniker string, fill byte, jailed bool, commission string) (Validator, string, string) {
	t.Helper()
	key := bytes.Repeat([]byte{fill}, 32)
	encoded := base64.StdEncoding.EncodeToString(key)
	consensus, err := consensusAddressFromPubKey(encoded)
	if err != nil {
		t.Fatalf("consensusAddressFromPubKey: %v", err)
	}
	hexAddress, err := consensusHexFromPubKey(encoded)
	if err != nil {
		t.Fatalf("consensusHexFromPubKey: %v", err)
	}
	return Validator{
		OperatorAddress: operator,
		ConsensusPubKey: map[string]any{
			"@type": "/cosmos.crypto.ed25519.PubKey",
			"key":   encoded,
		},
		Jailed:      jailed,
		Status:      "BOND_STATUS_UNBONDED",
		Description: ValidatorDescription{Moniker: moniker},
		Commission:  Commission{CommissionRates: CommissionRates{Rate: commission}},
	}, consensus, hexAddress
}

func (chain *managedTestChain) handler(w http.ResponseWriter, r *http.Request) {
	chain.mu.Lock()
	defer chain.mu.Unlock()

	switch {
	case strings.HasPrefix(r.URL.Path, "/cosmos/staking/v1beta1/delegations/"):
		account, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/cosmos/staking/v1beta1/delegations/"))
		if account == chain.failAccount {
			http.Error(w, "account unavailable", http.StatusServiceUnavailable)
			return
		}
		response := DelegationsResponse{}
		operators := make([]string, 0, len(chain.delegations[account]))
		for operator := range chain.delegations[account] {
			operators = append(operators, operator)
		}
		sort.Strings(operators)
		for _, operator := range operators {
			response.DelegationResponses = append(response.DelegationResponses, DelegationResponse{
				Delegation: Delegation{DelegatorAddress: account, ValidatorAddress: operator},
				Balance:    Coin{Denom: "uatom", Amount: chain.delegations[account][operator]},
			})
		}
		_ = json.NewEncoder(w).Encode(response)
	case strings.HasPrefix(r.URL.Path, "/cosmos/staking/v1beta1/delegators/"):
		trimmed := strings.TrimPrefix(r.URL.Path, "/cosmos/staking/v1beta1/delegators/")
		account := strings.TrimSuffix(trimmed, "/redelegations")
		response := RedelegationsResponse{}
		for _, lock := range chain.locks[account] {
			response.RedelegationResponses = append(response.RedelegationResponses, RedelegationResponse{
				Redelegation: Redelegation{DelegatorAddress: account, DestValidator: lock.destination},
				Entries: []RedelegationEntryResponse{{
					Entry:   RedelegationEntry{CompletionTime: lock.completion.Format(time.RFC3339Nano)},
					Balance: lock.amount,
				}},
			})
		}
		_ = json.NewEncoder(w).Encode(response)
	case r.URL.Path == "/cosmos/staking/v1beta1/validators":
		_ = json.NewEncoder(w).Encode(ValidatorsResponse{Validators: chain.validators})
	case r.URL.Path == "/cosmos/slashing/v1beta1/signing_infos":
		response := SigningInfosResponse{}
		for _, info := range chain.signing {
			response.Info = append(response.Info, info)
		}
		_ = json.NewEncoder(w).Encode(response)
	case r.URL.Path == "/cosmos/slashing/v1beta1/params":
		_ = json.NewEncoder(w).Encode(SlashingParamsResponse{Params: SlashingParamsRaw{
			SignedBlocksWindow:      "100",
			MinSignedPerWindow:      "0.10",
			SlashFractionDowntime:   "0.01",
			SlashFractionDoubleSign: "0.05",
		}})
	case r.URL.Path == "/validators":
		set := chain.activeHex
		blockHeight := ""
		if rawHeight := r.URL.Query().Get("height"); rawHeight != "" {
			height, _ := strconv.ParseInt(rawHeight, 10, 64)
			blockHeight = strconv.FormatInt(height, 10)
			chain.validatorHeights = append(chain.validatorHeights, height)
			if historical, ok := chain.consensusSets[height]; ok {
				set = historical
			}
		}
		response := CometValidatorsResponse{Result: CometValidatorsResult{BlockHeight: blockHeight, Total: strconv.Itoa(len(set))}}
		for address := range set {
			response.Result.Validators = append(response.Result.Validators, CometValidator{Address: address})
		}
		_ = json.NewEncoder(w).Encode(response)
	case r.URL.Path == "/status":
		_ = json.NewEncoder(w).Encode(CometStatusResponse{Result: CometStatusResult{
			NodeInfo: CometNodeInfo{Network: "cosmoshub-4"},
			SyncInfo: CometSyncInfo{LatestBlockHeight: strconv.FormatInt(chain.height, 10), LatestBlockTime: chain.blockTime.Format(time.RFC3339Nano)},
		}})
	case r.URL.Path == "/commit":
		height, _ := strconv.ParseInt(r.URL.Query().Get("height"), 10, 64)
		chain.commitHeights = append(chain.commitHeights, height)
		if height == chain.failCommitHeight {
			http.Error(w, "commit unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(CometCommitResponse{Result: CometCommitResult{SignedHeader: CometSignedHeader{Commit: CometCommit{
			Height: strconv.FormatInt(height, 10), Signatures: chain.commits[height],
		}}}})
	case r.URL.Path == "/block":
		height := chain.height
		if raw := r.URL.Query().Get("height"); raw != "" {
			height, _ = strconv.ParseInt(raw, 10, 64)
		}
		blockTime := chain.blockTime.Add(-time.Duration(chain.height-height) * 5 * time.Second)
		_ = json.NewEncoder(w).Encode(BlockResponse{Result: BlockResult{Block: Block{Header: BlockHeader{
			Height: strconv.FormatInt(height, 10), Time: blockTime.Format(time.RFC3339Nano),
		}}}})
	case r.URL.Path == "/cosmos/mint/v1beta1/annual_provisions":
		if chain.failRewards {
			http.Error(w, "mint unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"annual_provisions":"876000000"}`))
	case r.URL.Path == "/cosmos/distribution/v1beta1/params":
		_, _ = w.Write([]byte(`{"params":{"community_tax":"0.20"}}`))
	case r.URL.Path == "/cosmos/staking/v1beta1/pool":
		_, _ = w.Write([]byte(`{"pool":{"bonded_tokens":"1000000000","not_bonded_tokens":"0"}}`))
	default:
		http.NotFound(w, r)
	}
}

func managedTestScanner(t *testing.T, chain *managedTestChain, accounts []collectorconfig.ManagedDelegator) (*ManagedDelegationScanner, *Metrics, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(chain.handler))
	metrics := NewMetrics()
	scanner := NewManagedDelegationScanner(metrics, map[string]string{"cosmosvaloper1alpha": "Entity Alpha"}, collectorconfig.ManagedDelegations{
		WarningJailProgressPercent:  10,
		CriticalJailProgressPercent: 80,
		Accounts:                    accounts,
	})
	scanner.SetEndpoints([]string{server.URL}, []string{server.URL})
	return scanner, metrics, server
}

func managedLabels(operator, moniker, entity, consensus, accounts string) []string {
	return []string{operator, moniker, entity, consensus, accounts}
}

func TestManagedDelegationConfigurationMetricsExistBeforeFirstScan(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	NewManagedDelegationScanner(metrics, nil, collectorconfig.ManagedDelegations{
		WarningJailProgressPercent:  10,
		CriticalJailProgressPercent: 80,
	})
	if got := testutil.ToFloat64(metrics.ManagedDelegationConfiguredAccounts); got != 0 {
		t.Errorf("configured accounts = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationWarningJailProgress); got != 0.1 {
		t.Errorf("warning threshold = %v, want 0.1", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationCriticalJailProgress); got != 0.8 {
		t.Errorf("critical threshold = %v, want 0.8", got)
	}
}

func TestMetricsCollectorIncludesManagedAccountCountAndActive(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.ManagedDelegationConfiguredAccounts.Set(1)
	metrics.ManagedValidatorActive.WithLabelValues("operator", "moniker", "entity", "consensus", "account").Set(1)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(metrics)
	if got, err := testutil.GatherAndCount(registry,
		"validator_health_managed_delegation_configured_accounts",
		"validator_health_managed_validator_active",
	); err != nil || got != 2 {
		t.Fatalf("GatherAndCount() = %d, %v, want both managed metrics through Metrics Collect", got, err)
	}
}

func TestManagedDelegationScannerAggregatesOverlapAndCalculatesRisk(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	now := chain.blockTime
	chain.delegations["account-z"] = map[string]string{validatorA.OperatorAddress: "40000000"}
	chain.delegations["account-a"] = map[string]string{validatorA.OperatorAddress: "60000000"}
	finalUnlock := now.Add(2 * time.Hour)
	chain.locks["account-z"] = []managedTestLock{{destination: validatorA.OperatorAddress, amount: "1000000", completion: finalUnlock}}

	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{
		{Name: "Zulu", Address: "account-z"},
		{Name: "Alpha", Address: "account-a"},
	})
	defer server.Close()
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("Scan() error: %v", err)
	}
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Alpha,Zulu")

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{name: "configured accounts", got: testutil.ToFloat64(metrics.ManagedDelegationConfiguredAccounts), want: 2},
		{name: "scan success", got: testutil.ToFloat64(metrics.ManagedDelegationScanSuccess), want: 1},
		{name: "aggregate delegation", got: testutil.ToFloat64(metrics.ManagedValidatorDelegationATOM.WithLabelValues(labels...)), want: 100},
		{name: "portfolio share", got: testutil.ToFloat64(metrics.ManagedValidatorPortfolioShare.WithLabelValues(labels...)), want: 1},
		{name: "account alpha", got: testutil.ToFloat64(metrics.ManagedDelegationAccountATOM.WithLabelValues("Alpha", "account-a", validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA)), want: 60},
		{name: "account zulu", got: testutil.ToFloat64(metrics.ManagedDelegationAccountATOM.WithLabelValues("Zulu", "account-z", validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA)), want: 40},
		{name: "missed blocks", got: testutil.ToFloat64(metrics.ManagedValidatorMissedBlocks.WithLabelValues(labels...)), want: 45},
		{name: "missed ratio", got: testutil.ToFloat64(metrics.ManagedValidatorMissedRatio.WithLabelValues(labels...)), want: 0.45},
		{name: "jail progress", got: testutil.ToFloat64(metrics.ManagedValidatorJailProgress.WithLabelValues(labels...)), want: 0.5},
		{name: "blocks to jail", got: testutil.ToFloat64(metrics.ManagedValidatorBlocksToJail.WithLabelValues(labels...)), want: 46},
		{name: "seconds to jail", got: testutil.ToFloat64(metrics.ManagedValidatorSecondsToJail.WithLabelValues(labels...)), want: 230},
		{name: "recent miss rate", got: testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labels...)), want: 0},
		{name: "active", got: testutil.ToFloat64(metrics.ManagedValidatorActive.WithLabelValues(labels...)), want: 1},
		{name: "downtime exposure", got: testutil.ToFloat64(metrics.ManagedValidatorDowntimeSlashExposure.WithLabelValues(labels...)), want: 1},
		{name: "double sign exposure", got: testutil.ToFloat64(metrics.ManagedValidatorDoubleSignSlashExposure.WithLabelValues(labels...)), want: 5},
		{name: "reward loss", got: testutil.ToFloat64(metrics.ManagedValidatorEstimatedRewardsLost.WithLabelValues(labels...)), want: 0.0072},
		{name: "locked", got: testutil.ToFloat64(metrics.ManagedValidatorRedelegationLocked.WithLabelValues(labels...)), want: 40},
		{name: "redelegatable", got: testutil.ToFloat64(metrics.ManagedValidatorEstimatedRedelegatable.WithLabelValues(labels...)), want: 60},
		{name: "next unlock", got: testutil.ToFloat64(metrics.ManagedValidatorNextRedelegationUnlock.WithLabelValues(labels...)), want: float64(finalUnlock.Unix())},
		{name: "final unlock", got: testutil.ToFloat64(metrics.ManagedValidatorFinalRedelegationUnlock.WithLabelValues(labels...)), want: float64(finalUnlock.Unix())},
	}
	for _, check := range checks {
		if diff := check.got - check.want; diff < -1e-12 || diff > 1e-12 {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
	if got := testutil.CollectAndCount(metrics.ManagedValidatorDelegationATOM, "validator_health_managed_validator_delegation_atom"); got != 1 {
		t.Fatalf("aggregate delegation series = %d, want one for overlapping accounts", got)
	}
	if got := testutil.CollectAndCount(metrics.ManagedDelegationAccountATOM, "validator_health_managed_delegation_account_atom"); got != 2 {
		t.Fatalf("account delegation series = %d, want two", got)
	}
}

func TestManagedDelegationScannerUsesLatestPairUnlockAndLocksFullPosition(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	now := chain.blockTime
	earlier := now.Add(time.Hour)
	latest := now.Add(3 * time.Hour)
	chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "75000000"}
	chain.locks["account"] = []managedTestLock{
		{destination: validatorA.OperatorAddress, amount: "1000000", completion: latest},
		{destination: validatorA.OperatorAddress, amount: "2000000", completion: earlier},
	}

	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("Scan() error: %v", err)
	}
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")
	if got := testutil.ToFloat64(metrics.ManagedValidatorRedelegationLocked.WithLabelValues(labels...)); got != 75 {
		t.Errorf("locked = %v, want full 75 ATOM position", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorEstimatedRedelegatable.WithLabelValues(labels...)); got != 0 {
		t.Errorf("redelegatable = %v, want 0", got)
	}
	for name, got := range map[string]float64{
		"next":  testutil.ToFloat64(metrics.ManagedValidatorNextRedelegationUnlock.WithLabelValues(labels...)),
		"final": testutil.ToFloat64(metrics.ManagedValidatorFinalRedelegationUnlock.WithLabelValues(labels...)),
	} {
		if got != float64(latest.Unix()) {
			t.Errorf("%s unlock = %v, want latest pair completion %v", name, got, latest.Unix())
		}
	}
}

func TestManagedDelegationScannerLocksEveryChainReturnedReceivingEntry(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		amount     string
		completion time.Duration
	}{
		{name: "expired but returned", amount: "1000000", completion: -time.Hour},
		{name: "zero balance", amount: "0", completion: time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
			chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "12000000"}
			completion := chain.blockTime.Add(test.completion)
			chain.locks["account"] = []managedTestLock{{
				destination: validatorA.OperatorAddress,
				amount:      test.amount,
				completion:  completion,
			}}

			scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
			defer server.Close()
			if err := scanner.Scan(chain.blockTime); err != nil {
				t.Fatalf("Scan() error: %v", err)
			}
			labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")
			if got := testutil.ToFloat64(metrics.ManagedValidatorRedelegationLocked.WithLabelValues(labels...)); got != 12 {
				t.Errorf("locked = %v, want full 12 ATOM position", got)
			}
			if got := testutil.ToFloat64(metrics.ManagedValidatorFinalRedelegationUnlock.WithLabelValues(labels...)); got != float64(completion.Unix()) {
				t.Errorf("completion timestamp = %v, want %v", got, completion.Unix())
			}
		})
	}
}

func TestManagedDelegationScannerAggregatesPairFinalUnlocks(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	now := chain.blockTime
	next := now.Add(2 * time.Hour)
	final := now.Add(3 * time.Hour)
	chain.delegations["account-a"] = map[string]string{validatorA.OperatorAddress: "10000000"}
	chain.delegations["account-b"] = map[string]string{validatorA.OperatorAddress: "20000000"}
	chain.delegations["account-c"] = map[string]string{validatorA.OperatorAddress: "30000000"}
	chain.locks["account-a"] = []managedTestLock{{destination: validatorA.OperatorAddress, amount: "1", completion: final}}
	chain.locks["account-b"] = []managedTestLock{{destination: validatorA.OperatorAddress, amount: "1", completion: next}}

	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{
		{Name: "A", Address: "account-a"},
		{Name: "B", Address: "account-b"},
		{Name: "C", Address: "account-c"},
	})
	defer server.Close()
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("Scan() error: %v", err)
	}
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "A,B,C")
	if got := testutil.ToFloat64(metrics.ManagedValidatorRedelegationLocked.WithLabelValues(labels...)); got != 30 {
		t.Errorf("locked = %v, want 30 ATOM across locked account positions", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorEstimatedRedelegatable.WithLabelValues(labels...)); got != 30 {
		t.Errorf("redelegatable = %v, want 30 ATOM from unlocked account position", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorNextRedelegationUnlock.WithLabelValues(labels...)); got != float64(next.Unix()) {
		t.Errorf("next unlock = %v, want earliest pair-final %v", got, next.Unix())
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorFinalRedelegationUnlock.WithLabelValues(labels...)); got != float64(final.Unix()) {
		t.Errorf("final unlock = %v, want latest pair-final %v", got, final.Unix())
	}
}

func TestManagedDelegationScannerPublishesJailedTombstonedUnbondedExposure(t *testing.T) {
	t.Parallel()

	chain, _, validatorB, _, consensusB := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{validatorB.OperatorAddress: "20000000"}
	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	if err := scanner.Scan(chain.blockTime); err != nil {
		t.Fatalf("Scan() error: %v", err)
	}
	labels := managedLabels(validatorB.OperatorAddress, "Beta", "Beta", consensusB, "Treasury")
	if got := testutil.ToFloat64(metrics.ManagedValidatorActive.WithLabelValues(labels...)); got != 0 {
		t.Errorf("active = %v, want 0 for unbonded validator", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorJailed.WithLabelValues(labels...)); got != 1 {
		t.Errorf("jailed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorTombstoned.WithLabelValues(labels...)); got != 1 {
		t.Errorf("tombstoned = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labels...)); got != 0 {
		t.Errorf("recent miss rate = %v, want 0 for inactive validator", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorDoubleSignSlashExposure.WithLabelValues(labels...)); got != 1 {
		t.Errorf("double-sign exposure = %v ATOM, want 1", got)
	}
}

func TestManagedDelegationScannerRetiresStaleOnlyAfterSuccess(t *testing.T) {
	t.Parallel()

	chain, validatorA, validatorB, consensusA, consensusB := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "10000000"}
	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	now := chain.blockTime
	if err := scanner.Scan(now); err != nil {
		t.Fatalf("first Scan() error: %v", err)
	}
	lastSuccess := testutil.ToFloat64(metrics.ManagedDelegationScanLastSuccess)

	chain.mu.Lock()
	chain.delegations["account"] = map[string]string{validatorB.OperatorAddress: "20000000"}
	chain.failAccount = "account"
	chain.mu.Unlock()
	if err := scanner.Scan(now.Add(5 * time.Minute)); err == nil {
		t.Fatal("failed account query produced a successful scan")
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationScanSuccess); got != 0 {
		t.Fatalf("scan success = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationScanLastSuccess); got != lastSuccess {
		t.Fatalf("last success changed from %v to %v", lastSuccess, got)
	}
	oldLabels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")
	if got := testutil.ToFloat64(metrics.ManagedValidatorDelegationATOM.WithLabelValues(oldLabels...)); got != 10 {
		t.Fatalf("old delegation changed on failure to %v, want 10", got)
	}

	chain.mu.Lock()
	chain.failAccount = ""
	chain.mu.Unlock()
	if err := scanner.Scan(now.Add(10 * time.Minute)); err != nil {
		t.Fatalf("successful changed Scan() error: %v", err)
	}
	if got := testutil.CollectAndCount(metrics.ManagedValidatorDelegationATOM, "validator_health_managed_validator_delegation_atom"); got != 1 {
		t.Fatalf("aggregate series after roster change = %d, want 1", got)
	}
	newLabels := managedLabels(validatorB.OperatorAddress, "Beta", "Beta", consensusB, "Treasury")
	if got := testutil.ToFloat64(metrics.ManagedValidatorDelegationATOM.WithLabelValues(newLabels...)); got != 20 {
		t.Fatalf("new delegation = %v, want 20", got)
	}
}

func TestManagedDelegationRecentMissRateSamplesExactCommits(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "1000000"}
	consensusHex, err := consensusHexFromPubKey(pubKeyOf(validatorA))
	if err != nil {
		t.Fatalf("consensusHexFromPubKey() error: %v", err)
	}
	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")

	for _, test := range []struct {
		name   string
		misses int64
		want   float64
	}{
		{name: "zero of ten", misses: 0, want: 0},
		{name: "five of ten", misses: 5, want: 0.5},
		{name: "ten of ten", misses: 10, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			chain.mu.Lock()
			chain.commitHeights = nil
			chain.validatorHeights = nil
			for offset := int64(0); offset < managedRecentCommitCount; offset++ {
				height := chain.height - offset
				chain.commits[height] = nil
				if offset >= test.misses {
					chain.commits[height] = []CometCommitSignature{{BlockIDFlag: 2, ValidatorAddress: consensusHex}}
				}
			}
			chain.mu.Unlock()

			if err := scanner.Scan(chain.blockTime); err != nil {
				t.Fatalf("Scan() error: %v", err)
			}
			if got := testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labels...)); got != test.want {
				t.Fatalf("recent miss rate = %v, want %v", got, test.want)
			}
			chain.mu.Lock()
			queriedCommits := append([]int64(nil), chain.commitHeights...)
			queriedSets := append([]int64(nil), chain.validatorHeights...)
			chain.mu.Unlock()
			if len(queriedCommits) != int(managedRecentCommitCount) || len(queriedSets) != int(managedRecentCommitCount) {
				t.Fatalf("queried commit heights = %v, validator-set heights = %v, want 10 each", queriedCommits, queriedSets)
			}
			for offset, height := range queriedCommits {
				want := chain.height - int64(offset)
				if height != want || queriedSets[offset] != want {
					t.Fatalf("sample[%d] commit/set heights = %d/%d, want exact height %d", offset, height, queriedSets[offset], want)
				}
			}
		})
	}
}

func TestManagedDelegationRecentMissRateUsesEligibilityAtEachHeight(t *testing.T) {
	t.Parallel()

	chain, validatorA, validatorB, consensusA, consensusB := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{
		validatorA.OperatorAddress: "1000000",
		validatorB.OperatorAddress: "1000000",
	}
	hexA, err := consensusHexFromPubKey(pubKeyOf(validatorA))
	if err != nil {
		t.Fatalf("consensusHexFromPubKey(A): %v", err)
	}
	hexB, err := consensusHexFromPubKey(pubKeyOf(validatorB))
	if err != nil {
		t.Fatalf("consensusHexFromPubKey(B): %v", err)
	}
	for offset := int64(0); offset < managedRecentCommitCount; offset++ {
		height := chain.height - offset
		chain.commits[height] = nil
		if offset < 5 {
			chain.consensusSets[height] = map[string]bool{hexA: true}
			if offset != 0 {
				chain.commits[height] = []CometCommitSignature{{BlockIDFlag: 2, ValidatorAddress: hexA}}
			}
			continue
		}
		chain.consensusSets[height] = map[string]bool{hexB: true}
		if offset >= 7 {
			chain.commits[height] = []CometCommitSignature{{BlockIDFlag: 2, ValidatorAddress: hexB}}
		} else {
			chain.commits[height] = nil
		}
	}

	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	if err := scanner.Scan(chain.blockTime); err != nil {
		t.Fatalf("Scan() error: %v", err)
	}
	labelsA := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")
	labelsB := managedLabels(validatorB.OperatorAddress, "Beta", "Beta", consensusB, "Treasury")
	if got := testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labelsA...)); got != 0.2 {
		t.Errorf("entering validator miss rate = %v, want 1/5", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labelsB...)); got != 0.4 {
		t.Errorf("exiting validator miss rate = %v, want 2/5", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorActive.WithLabelValues(labelsB...)); got != 0 {
		t.Errorf("exited validator current active state = %v, want 0", got)
	}
}

func TestManagedDelegationCommitFailureFailsCoreScan(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "1000000"}
	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	if err := scanner.Scan(chain.blockTime); err != nil {
		t.Fatalf("first Scan() error: %v", err)
	}
	lastSuccess := testutil.ToFloat64(metrics.ManagedDelegationScanLastSuccess)
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")

	chain.mu.Lock()
	chain.failCommitHeight = chain.height - 4
	chain.commits[chain.height] = nil
	chain.mu.Unlock()
	if err := scanner.Scan(chain.blockTime.Add(5 * time.Minute)); err == nil {
		t.Fatal("Scan() succeeded despite a recent commit query failure")
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationScanSuccess); got != 0 {
		t.Errorf("scan success = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationScanLastSuccess); got != lastSuccess {
		t.Errorf("last success = %v, want preserved %v", got, lastSuccess)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorRecentMissRate.WithLabelValues(labels...)); got != 0 {
		t.Errorf("published recent miss rate = %v, want prior complete value 0", got)
	}
}

func TestManagedDelegationOptionalRewardFailureDoesNotFailCoreScan(t *testing.T) {
	t.Parallel()

	chain, validatorA, _, consensusA, _ := newManagedTestChain(t)
	chain.delegations["account"] = map[string]string{validatorA.OperatorAddress: "1000000"}
	scanner, metrics, server := managedTestScanner(t, chain, []collectorconfig.ManagedDelegator{{Name: "Treasury", Address: "account"}})
	defer server.Close()
	if err := scanner.Scan(chain.blockTime); err != nil {
		t.Fatalf("first Scan() error: %v", err)
	}
	labels := managedLabels(validatorA.OperatorAddress, "Alpha", "Entity Alpha", consensusA, "Treasury")
	if got := testutil.CollectAndCount(metrics.ManagedValidatorEstimatedRewardsLost, "validator_health_managed_validator_estimated_rewards_lost_per_hour_atom"); got != 1 {
		t.Fatalf("reward series after successful inputs = %d, want 1", got)
	}

	chain.mu.Lock()
	chain.failRewards = true
	chain.mu.Unlock()
	if err := scanner.Scan(chain.blockTime.Add(5 * time.Minute)); err != nil {
		t.Fatalf("Scan() failed on optional reward query: %v", err)
	}
	if got := testutil.ToFloat64(metrics.ManagedDelegationScanSuccess); got != 1 {
		t.Fatalf("scan success = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.ManagedValidatorEstimatedRewardsLost, "validator_health_managed_validator_estimated_rewards_lost_per_hour_atom"); got != 0 {
		t.Fatalf("reward series after optional failure = %d, want reset", got)
	}
	if got := testutil.ToFloat64(metrics.ManagedValidatorJailProgress.WithLabelValues(labels...)); got != 0.5 {
		t.Fatalf("core jail progress = %v, want 0.5", got)
	}
}

func TestManagedDelegationRPCFailover(t *testing.T) {
	t.Parallel()

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"result":{"total":"1","validators":[{"address":"ABC"}]}}`)
	}))
	defer working.Close()
	client := NewRPCClient(failed.URL, working.URL)
	set, err := client.QueryConsensusSet()
	if err != nil {
		t.Fatalf("QueryConsensusSet() failed instead of rotating: %v", err)
	}
	if !set["ABC"] || client.BaseURL() != working.URL {
		t.Fatalf("set = %#v, selected endpoint = %q", set, client.BaseURL())
	}
}

func TestManagedMetricsPublicationIsAtomicForConcurrentScrapes(t *testing.T) {
	const positions = 25

	result := func(prefix string) managedScanResult {
		snapshot := managedScanResult{
			totalUAtom:    positions * uatomPerATOM,
			slashing:      &SlashingParams{SignedBlocksWindow: 100, MaxMissedBlocks: 90},
			blockInterval: 5,
		}
		for i := 0; i < positions; i++ {
			account := fmt.Sprintf("%s-account-%02d", prefix, i)
			operator := fmt.Sprintf("%s-operator-%02d", prefix, i)
			snapshot.accounts = append(snapshot.accounts, managedAccountPosition{
				accountName:      account,
				delegatorAddress: account,
				operatorAddress:  operator,
				amountUAtom:      uatomPerATOM,
			})
			snapshot.aggregates = append(snapshot.aggregates, managedAggregate{
				operatorAddress:  operator,
				moniker:          operator,
				entity:           operator,
				consensusAddress: prefix + "-consensus",
				accounts:         []string{account},
				amountUAtom:      uatomPerATOM,
				active:           true,
			})
		}
		return snapshot
	}

	metrics := NewMetrics()
	scanner := &ManagedDelegationScanner{metrics: metrics}
	oldSnapshot := result("old")
	newSnapshot := result("new")
	scanner.publish(oldSnapshot, time.Unix(1, 0))

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		defer close(done)
		for i := 0; i < 200; i++ {
			if i%2 == 0 {
				scanner.publish(newSnapshot, time.Unix(2, 0))
			} else {
				scanner.publish(oldSnapshot, time.Unix(1, 0))
			}
		}
	}()
	<-started

	scrapes := 0
	for {
		if got := testutil.CollectAndCount(metrics, "validator_health_managed_validator_delegation_atom"); got != positions {
			t.Fatalf("managed aggregate count during publication = %d, want complete snapshot of %d", got, positions)
		}
		scrapes++
		select {
		case <-done:
			if scrapes == 0 {
				t.Fatal("concurrent publication completed without a scrape")
			}
			return
		default:
		}
	}
}

func TestBlocksUntilJailUsesStrictGreaterThanBoundary(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		missed float64
		want   float64
	}{
		{name: "below maximum", missed: 89, want: 2},
		{name: "at maximum", missed: 90, want: 1},
		{name: "above maximum", missed: 91, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := blocksUntilJail(90, test.missed); got != test.want {
				t.Fatalf("blocksUntilJail(90, %v) = %v, want %v", test.missed, got, test.want)
			}
		})
	}
}

func TestSigningRiskMetricsExposeStableValidatorIdentityLabels(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	labels := []string{"cosmosvaloper1a", "Alpha", "Entity Alpha", "cosmosvalcons1a"}
	metrics.ValMissedBlocksInfo.WithLabelValues(labels...).Set(10)
	metrics.ValMissedRatio.WithLabelValues(labels...).Set(0.1)
	metrics.ValBlocksToJail.WithLabelValues(labels...).Set(90)
	metrics.ValSecondsToJail.WithLabelValues(labels...).Set(450)
	for _, metric := range []struct {
		collector prometheus.Collector
		name      string
	}{
		{collector: metrics.ValMissedBlocksInfo, name: "validator_health_validator_missed_blocks_info"},
		{collector: metrics.ValMissedRatio, name: "validator_health_validator_missed_ratio"},
		{collector: metrics.ValBlocksToJail, name: "validator_health_validator_blocks_to_jail"},
		{collector: metrics.ValSecondsToJail, name: "validator_health_validator_seconds_to_jail"},
	} {
		if got := testutil.CollectAndCount(metric.collector, metric.name); got != 1 {
			t.Errorf("%s series = %d, want 1", metric.name, got)
		}
	}
}
