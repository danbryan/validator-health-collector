// Package cosmosaddr derives and re-encodes Cosmos address forms.
//
// This exists so the collector does not depend on cosmos-sdk. The three functions
// below were the entire surface it used, and pulling in cosmos-sdk v0.55.0 for
// them cost roughly seventy indirect dependencies and forced the module to require
// Go 1.26.5, which the monorepo's pinned toolchain cannot build.
//
// Every function here is verified against the cosmos-sdk implementations it
// replaces in cosmosaddr_test.go, using real Cosmos Hub validator keys.
package cosmosaddr

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cosmos/btcutil/bech32"
)

// addressLen is the 20-byte address length CometBFT truncates a key hash to.
const addressLen = 20

// FromEd25519PubKey derives a CometBFT consensus address from a base64-encoded
// ed25519 consensus public key.
//
// CometBFT defines an ed25519 address as the first 20 bytes of the SHA-256 hash of
// the public key, which is what cosmos-sdk's ed25519.PubKey.Address() returns.
func FromEd25519PubKey(pubKeyBase64 string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(pubKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("decoding consensus pubkey: %w", err)
	}
	sum := sha256.Sum256(raw)
	addr := make([]byte, addressLen)
	copy(addr, sum[:addressLen])
	return addr, nil
}

// HexAddress renders an address as the uppercase hex string CometBFT uses to
// identify a validator in the consensus set.
func HexAddress(addr []byte) string {
	return strings.ToUpper(hex.EncodeToString(addr))
}

// Encode bech32-encodes raw address bytes under the given human-readable prefix,
// converting from 8-bit to 5-bit groups first as Cosmos addresses require.
func Encode(hrp string, data []byte) (string, error) {
	converted, err := bech32.ConvertBits(data, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("converting address bits: %w", err)
	}
	encoded, err := bech32.Encode(hrp, converted)
	if err != nil {
		return "", fmt.Errorf("encoding bech32 with prefix %q: %w", hrp, err)
	}
	return encoded, nil
}

// Decode reverses Encode, returning the prefix and the raw address bytes.
func Decode(addr string) (string, []byte, error) {
	hrp, converted, err := bech32.Decode(addr, len(addr))
	if err != nil {
		return "", nil, fmt.Errorf("decoding bech32 %q: %w", addr, err)
	}
	data, err := bech32.ConvertBits(converted, 5, 8, false)
	if err != nil {
		return "", nil, fmt.Errorf("converting address bits back: %w", err)
	}
	return hrp, data, nil
}

// Reprefix re-encodes an address under a different prefix, which is how an
// account address is mapped to its validator operator address.
func Reprefix(addr, newHRP string) (string, error) {
	_, data, err := Decode(addr)
	if err != nil {
		return "", err
	}
	return Encode(newHRP, data)
}
