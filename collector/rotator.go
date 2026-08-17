package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
)

// maxAttempts caps how many endpoints one logical request will try before giving
// up. The candidate list can be 30 long, and walking all of it would let a single
// query outlive the collection cycle.
const maxAttempts = 3

// maxBodyBytes caps how much of a response is read, so one misbehaving endpoint
// cannot exhaust memory.
const maxBodyBytes = 32 << 20

// rotator holds a ranked list of interchangeable endpoints and tracks which one
// is currently selected.
//
// Selection is sticky rather than round-robin: a working endpoint stays selected
// across calls so one collection cycle reads a consistent view of the chain, and
// only an actual failure advances to the next candidate.
type rotator struct {
	mu      sync.Mutex
	bases   []string
	current int
}

// set replaces the candidate list, preserving the current selection when it is
// still present so a healthy endpoint is not dropped mid-cycle.
func (r *rotator) set(baseURLs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cleaned := make([]string, 0, len(baseURLs))
	for _, u := range baseURLs {
		if trimmed := strings.TrimRight(strings.TrimSpace(u), "/"); trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}

	previous := r.currentBaseLocked()
	r.bases = cleaned
	r.current = 0
	for i, u := range cleaned {
		if u == previous {
			r.current = i
			break
		}
	}
}

// base reports the endpoint currently selected, or "" when none is available.
func (r *rotator) base() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentBaseLocked()
}

func (r *rotator) currentBaseLocked() string {
	if r.current < len(r.bases) {
		return r.bases[r.current]
	}
	return ""
}

// advance moves to the next candidate after a failure.
func (r *rotator) advance() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.bases) > 0 {
		r.current = (r.current + 1) % len(r.bases)
	}
}

// size reports how many candidates are available.
func (r *rotator) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bases)
}

// budget is how many attempts to make: capped by maxAttempts and by how many
// candidates actually exist.
func (r *rotator) budget() int {
	if n := r.size(); n < maxAttempts {
		return n
	}
	return maxAttempts
}

// fetchJSON issues a GET against the rotator's current endpoint and decodes the
// body, rotating to the next candidate on any failure.
//
// A transport error, a non-200, and an undecodable body are treated alike: the
// endpoint did not answer usefully, so try the next one. Public Cosmos endpoints
// return HTML error pages and 429s often enough that the undecodable case must
// rotate rather than fail, which is how a transient proxy error was previously
// able to corrupt a whole snapshot.
func fetchJSON(rot *rotator, client *http.Client, kind, path string, dst any) error {
	attempts := rot.budget()
	if attempts == 0 {
		return fmt.Errorf("no %s endpoint available", kind)
	}

	var lastErr error
	for range attempts {
		base := rot.base()
		lastErr = getJSONOnce(client, base+path, dst)
		if lastErr == nil {
			return nil
		}
		log.Printf("WARN: %s %s%s failed, rotating: %v", kind, base, path, lastErr)
		rot.advance()
	}
	return fmt.Errorf("all %d %s attempts failed for %s: %w", attempts, kind, path, lastErr)
}

// getJSONOnce performs a single request with no rotation.
func getJSONOnce(client *http.Client, url string, dst any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}

	// The URL is variable by design: endpoints are discovered from the Cosmos chain
	// registry and rotated at runtime, which is the whole point of this package. The
	// registry is a trusted upstream, non-https entries are dropped when the list is
	// built, and nothing user-supplied reaches this call.
	resp, err := client.Do(req) //nolint:gosec // G704: registry-sourced endpoint, see above
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, snippet(body))
	}

	if dst == nil {
		return nil
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("parsing response (body began %q): %w", snippet(body), err)
	}
	return nil
}

// snippet trims a response body down to something loggable.
func snippet(body []byte) string {
	const limit = 120
	s := strings.TrimSpace(string(body))
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return strings.ReplaceAll(s, "\n", " ")
}
