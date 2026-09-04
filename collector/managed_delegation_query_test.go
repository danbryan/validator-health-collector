package collector //nolint:testpackage // Query tests exercise internal response contracts.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueryDelegationsPaginatesAndFiltersZeroBalances(t *testing.T) {
	t.Parallel()

	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cosmos/staking/v1beta1/delegations/cosmos1account" {
			http.NotFound(w, r)
			return
		}
		cursors = append(cursors, r.URL.Query().Get("pagination.key"))
		switch r.URL.Query().Get("pagination.key") {
		case "":
			_, _ = w.Write([]byte(`{"delegation_responses":[{"delegation":{"validator_address":"cosmosvaloper1a"},"balance":{"denom":"uatom","amount":"1500000"}},{"delegation":{"validator_address":"cosmosvaloper1zero"},"balance":{"denom":"uatom","amount":"0"}}],"pagination":{"next_key":"next"}}`))
		case "next":
			_, _ = w.Write([]byte(`{"delegation_responses":[{"delegation":{"validator_address":"cosmosvaloper1b"},"balance":{"denom":"uatom","amount":"2500000"}}],"pagination":{"next_key":null}}`))
		default:
			t.Fatalf("unexpected cursor %q", r.URL.Query().Get("pagination.key"))
		}
	}))
	defer server.Close()

	got, err := NewRESTClient(server.URL).QueryDelegations("cosmos1account")
	if err != nil {
		t.Fatalf("QueryDelegations() error: %v", err)
	}
	if len(cursors) != 2 || cursors[0] != "" || cursors[1] != "next" {
		t.Fatalf("pagination cursors = %#v, want [\"\" \"next\"]", cursors)
	}
	if len(got) != 2 || got[0].OperatorAddress != "cosmosvaloper1a" || got[0].AmountUAtom != 1_500_000 || got[1].AmountUAtom != 2_500_000 {
		t.Fatalf("delegations = %#v", got)
	}
}

func TestQueryDelegationsRejectsDenomAndMalformedAmount(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, denom, amount string
	}{
		{name: "denom", denom: "uosmo", amount: "1"},
		{name: "malformed", denom: "uatom", amount: "1.5"},
		{name: "negative", denom: "uatom", amount: "-1"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(DelegationsResponse{DelegationResponses: []DelegationResponse{{
					Delegation: Delegation{ValidatorAddress: "cosmosvaloper1a"},
					Balance:    Coin{Denom: test.denom, Amount: test.amount},
				}}})
			}))
			defer server.Close()
			if got, err := NewRESTClient(server.URL).QueryDelegations("cosmos1account"); err == nil {
				t.Fatalf("QueryDelegations() = %#v, want validation error", got)
			}
		})
	}
}

func TestQueryReceivingRedelegationsReturnsEveryChainEntry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	next := now.Add(time.Hour)
	expired := now.Add(-time.Second)
	final := now.Add(2 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var response RedelegationsResponse
		switch r.URL.Query().Get("pagination.key") {
		case "":
			response = RedelegationsResponse{
				RedelegationResponses: []RedelegationResponse{{
					Redelegation: Redelegation{DestValidator: "cosmosvaloper1a"},
					Entries: []RedelegationEntryResponse{
						{Entry: RedelegationEntry{CompletionTime: next.Format(time.RFC3339Nano)}, Balance: "1000000"},
						{Entry: RedelegationEntry{CompletionTime: expired.Format(time.RFC3339Nano)}, Balance: "9000000"},
						{Entry: RedelegationEntry{CompletionTime: final.Format(time.RFC3339Nano)}, Balance: "0"},
					},
				}},
				Pagination: Pagination{NextKey: "next"},
			}
		case "next":
			response = RedelegationsResponse{RedelegationResponses: []RedelegationResponse{{
				Redelegation: Redelegation{DestValidator: "cosmosvaloper1b"},
				Entries: []RedelegationEntryResponse{{
					Entry: RedelegationEntry{CompletionTime: final.Format(time.RFC3339Nano)}, Balance: "2500000",
				}},
			}}}
		default:
			t.Fatalf("unexpected cursor")
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	got, err := NewRESTClient(server.URL).QueryReceivingRedelegations("cosmos1account")
	if err != nil {
		t.Fatalf("QueryReceivingRedelegations() error: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("receiving redelegations count = %d, want every 4 chain-returned entries: %#v", len(got), got)
	}
	if got[0].DelegatorAddress != "cosmos1account" || got[0].BalanceUAtom != 1_000_000 || !got[0].CompletionTime.Equal(next) {
		t.Fatalf("first receiving redelegation = %#v", got[0])
	}
	if got[1].BalanceUAtom != 9_000_000 || !got[1].CompletionTime.Equal(expired) {
		t.Fatalf("expired-but-returned redelegation = %#v", got[1])
	}
	if got[2].BalanceUAtom != 0 || !got[2].CompletionTime.Equal(final) {
		t.Fatalf("zero-balance redelegation = %#v", got[2])
	}
	if got[3].DestinationOperator != "cosmosvaloper1b" {
		t.Fatalf("paginated receiving redelegation = %#v", got[3])
	}
}

func TestQueryReceivingRedelegationsRejectsMalformedBalanceAndTime(t *testing.T) {
	t.Parallel()

	for _, entry := range []RedelegationEntryResponse{
		{Entry: RedelegationEntry{CompletionTime: "2026-09-02T00:00:00Z"}, Balance: "bad"},
		{Entry: RedelegationEntry{CompletionTime: "bad"}, Balance: "1"},
	} {
		entry := entry
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(RedelegationsResponse{RedelegationResponses: []RedelegationResponse{{
				Redelegation: Redelegation{DestValidator: "cosmosvaloper1a"}, Entries: []RedelegationEntryResponse{entry},
			}}})
		}))
		if got, err := NewRESTClient(server.URL).QueryReceivingRedelegations("cosmos1account"); err == nil {
			server.Close()
			t.Fatalf("QueryReceivingRedelegations() = %#v, want validation error", got)
		}
		server.Close()
	}
}

func TestQueryAllValidatorsPaginatesWithoutStatusFilter(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if status := r.URL.Query().Get("status"); status != "" {
			t.Errorf("status filter = %q, want none", status)
		}
		response := ValidatorsResponse{}
		if r.URL.Query().Get("pagination.key") == "" {
			response.Validators = []Validator{{OperatorAddress: "a"}}
			response.Pagination.NextKey = "next"
		} else {
			response.Validators = []Validator{{OperatorAddress: "b"}}
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	got, err := NewRESTClient(server.URL).QueryAllValidators()
	if err != nil {
		t.Fatalf("QueryAllValidators() error: %v", err)
	}
	if requests != 2 || len(got) != 2 || got[0].OperatorAddress != "a" || got[1].OperatorAddress != "b" {
		t.Fatalf("requests = %d, validators = %#v", requests, got)
	}
}

func TestQueryCommitSignersIncludesOnlyCommitSignaturesAndUppercasesAddresses(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/commit" || r.URL.Query().Get("height") != "123" {
			t.Fatalf("request = %s?%s, want /commit?height=123", r.URL.Path, r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(CometCommitResponse{Result: CometCommitResult{SignedHeader: CometSignedHeader{Commit: CometCommit{
			Height: "123",
			Signatures: []CometCommitSignature{
				{BlockIDFlag: 2, ValidatorAddress: "abc123"},
				{BlockIDFlag: 2, ValidatorAddress: "DEF456"},
				{BlockIDFlag: 1, ValidatorAddress: "absent"},
				{BlockIDFlag: 3, ValidatorAddress: "nil"},
				{BlockIDFlag: 2},
			},
		}}}})
	}))
	defer server.Close()

	signers, err := NewRPCClient(server.URL).QueryCommitSigners(123)
	if err != nil {
		t.Fatalf("QueryCommitSigners() error: %v", err)
	}
	if len(signers) != 2 || !signers["ABC123"] || !signers["DEF456"] {
		t.Fatalf("signers = %#v, want uppercase commit signers only", signers)
	}
}

func TestQueryCommitSignersFailsOver(t *testing.T) {
	t.Parallel()

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":{"signed_header":{"commit":{"height":"456","signatures":[{"block_id_flag":2,"validator_address":"abc"}]}}}}`))
	}))
	defer working.Close()

	client := NewRPCClient(failed.URL, working.URL)
	signers, err := client.QueryCommitSigners(456)
	if err != nil {
		t.Fatalf("QueryCommitSigners() failed instead of rotating: %v", err)
	}
	if !signers["ABC"] || client.BaseURL() != working.URL {
		t.Fatalf("signers = %#v, selected endpoint = %q", signers, client.BaseURL())
	}
}

func TestQueryConsensusSetAtHeightUsesExactHeightAndFailsOver(t *testing.T) {
	t.Parallel()

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("height"); got != "789" {
			t.Errorf("validator-set height = %q, want 789", got)
		}
		_, _ = w.Write([]byte(`{"result":{"block_height":"789","total":"1","validators":[{"address":"abc"}]}}`))
	}))
	defer working.Close()

	client := NewRPCClient(failed.URL, working.URL)
	set, err := client.QueryConsensusSetAtHeight(789)
	if err != nil {
		t.Fatalf("QueryConsensusSetAtHeight() failed instead of rotating: %v", err)
	}
	if !set["ABC"] || client.BaseURL() != working.URL {
		t.Fatalf("set = %#v, selected endpoint = %q", set, client.BaseURL())
	}
}

func TestQuerySlashingParamsUsesExactSDKRounding(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		window    string
		ratio     string
		wantMin   int64
		wantMax   int64
		wantError bool
	}{
		{name: "fractional half rounds up", window: "3", ratio: "0.5", wantMin: 2, wantMax: 1},
		{name: "below half rounds down", window: "3", ratio: "0.499999999999999999", wantMin: 1, wantMax: 2},
		{name: "zero ratio", window: "3", ratio: "0", wantMin: 0, wantMax: 3},
		{name: "one ratio leaves no jail budget", window: "3", ratio: "1", wantError: true},
		{name: "above one rejected exactly", window: "3", ratio: "1.0000000000000000001", wantError: true},
		{name: "negative rejected", window: "3", ratio: "-0.1", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(SlashingParamsResponse{Params: SlashingParamsRaw{
					SignedBlocksWindow:      test.window,
					MinSignedPerWindow:      test.ratio,
					SlashFractionDoubleSign: "0.05",
					SlashFractionDowntime:   "0.01",
				}})
			}))
			defer server.Close()

			got, err := NewRESTClient(server.URL).QuerySlashingParams()
			if test.wantError {
				if err == nil {
					t.Fatalf("QuerySlashingParams() = %#v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("QuerySlashingParams() error: %v", err)
			}
			if got.MinSignedBlocks != test.wantMin || got.MaxMissedBlocks != test.wantMax {
				t.Fatalf("rounded minimum/maximum = %d/%d, want %d/%d", got.MinSignedBlocks, got.MaxMissedBlocks, test.wantMin, test.wantMax)
			}
		})
	}
}

func TestManagedRESTQueriesFailOver(t *testing.T) {
	t.Parallel()

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"delegation_responses":[{"delegation":{"validator_address":"cosmosvaloper1a"},"balance":{"denom":"uatom","amount":"1"}}],"pagination":{"next_key":null}}`))
	}))
	defer working.Close()

	client := NewRESTClient(failed.URL, working.URL)
	got, err := client.QueryDelegations("cosmos1account")
	if err != nil {
		t.Fatalf("QueryDelegations() failed instead of rotating: %v", err)
	}
	if len(got) != 1 || client.BaseURL() != working.URL {
		t.Fatalf("delegations = %#v, selected endpoint = %q", got, client.BaseURL())
	}
}
