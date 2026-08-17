package cosmosaddr_test

import (
	"testing"

	"github.com/danbryan/validator-health-collector/cosmosaddr"
)

// The expected values below were produced by the cosmos-sdk implementations this
// package replaces, ed25519.PubKey.Address() and types/bech32, and were compared
// against all 200 bonded Cosmos Hub validators with zero mismatches before
// cosmos-sdk was dropped. They are real mainnet validator keys.
var cases = []struct {
	name        string
	pubKeyB64   string
	wantHex     string
	wantValcons string
}{
	{
		name:        "validator-1",
		pubKeyB64:   "uEUR1gpesU4bnSWL2TOXOf3org2mCYhQHMYkiCJyMD4=",
		wantHex:     "0ABBA36C54DD0CA6A790AEF96A01D4392E36345F",
		wantValcons: "cosmosvalcons1p2a6xmz5m5x2dfus4muk5qw58yhrvdzltw4q4u",
	},
	{
		name:        "validator-2",
		pubKeyB64:   "QGVtDX/SksMy9iAShygkGVs3yoCHzItXX3qtyrSUIng=",
		wantHex:     "EC07626908C1070B8115C76713D2D1AA76FAF965",
		wantValcons: "cosmosvalcons1asrky6ggcyrshqg4can385k34fm047t9hprqtf",
	},
	{
		name:        "validator-3",
		pubKeyB64:   "Qajjf1kiAJ0M1UcH1TSUYLP13kgE128Av1XmGQO711c=",
		wantHex:     "F59734A896A7689436BC3422244FD862AE189C5C",
		wantValcons: "cosmosvalcons17ktnf2yk5a5fgd4uxs3zgn7cv2hp38zuw8j35e",
	},
}

func TestFromEd25519PubKeyMatchesCosmosSDK(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			addr, err := cosmosaddr.FromEd25519PubKey(tc.pubKeyB64)
			if err != nil {
				t.Fatalf("FromEd25519PubKey() error: %v", err)
			}
			if len(addr) != 20 {
				t.Errorf("address length = %d, want 20", len(addr))
			}
			if got := cosmosaddr.HexAddress(addr); got != tc.wantHex {
				t.Errorf("HexAddress() = %q, want %q", got, tc.wantHex)
			}
			got, err := cosmosaddr.Encode("cosmosvalcons", addr)
			if err != nil {
				t.Fatalf("Encode() error: %v", err)
			}
			if got != tc.wantValcons {
				t.Errorf("Encode() = %q, want %q", got, tc.wantValcons)
			}
		})
	}
}

func TestFromEd25519PubKeyRejectsBadBase64(t *testing.T) {
	t.Parallel()

	if _, err := cosmosaddr.FromEd25519PubKey("not base64!!"); err == nil {
		t.Error("FromEd25519PubKey() accepted invalid base64")
	}
}

func TestReprefixMapsAccountToValidatorOperator(t *testing.T) {
	t.Parallel()

	// Same underlying 20 bytes, different human-readable prefix. This is how a
	// governance voter's account address is matched to a validator.
	const acc = "cosmos1q6d3d089hg59x6gcx92uumx70s5y5wadntgvtr"
	const wantValoper = "cosmosvaloper1q6d3d089hg59x6gcx92uumx70s5y5wadklue8s"

	got, err := cosmosaddr.Reprefix(acc, "cosmosvaloper")
	if err != nil {
		t.Fatalf("Reprefix() error: %v", err)
	}
	if got != wantValoper {
		t.Errorf("Reprefix() = %q, want %q", got, wantValoper)
	}
}

func TestReprefixRejectsInvalidAddress(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "cosmos1invalid", "notbech32"} {
		if _, err := cosmosaddr.Reprefix(bad, "cosmosvaloper"); err == nil {
			t.Errorf("Reprefix(%q) accepted an invalid address", bad)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	raw := []byte{
		0x0A, 0xBB, 0xA3, 0x6C, 0x54, 0xDD, 0x0C, 0xA6, 0xA7, 0x90,
		0xAE, 0xF9, 0x6A, 0x01, 0xD4, 0x39, 0x2E, 0x36, 0x34, 0x5F,
	}

	encoded, err := cosmosaddr.Encode("cosmosvaloper", raw)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}

	hrp, decoded, err := cosmosaddr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if hrp != "cosmosvaloper" {
		t.Errorf("prefix = %q, want cosmosvaloper", hrp)
	}
	if len(decoded) != len(raw) {
		t.Fatalf("decoded length = %d, want %d", len(decoded), len(raw))
	}
	for i := range raw {
		if decoded[i] != raw[i] {
			t.Fatalf("round trip changed byte %d: got %#x, want %#x", i, decoded[i], raw[i])
		}
	}
}
