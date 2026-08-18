package collector

import (
	"testing"
	"time"
)

func TestNextAlignedRun(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 18, 13, 57, 42, 0, time.FixedZone("EDT", -4*60*60))
	want := time.Date(2026, time.August, 18, 14, 0, 0, 0, now.Location())
	if got := nextAlignedRun(now, time.Hour); !got.Equal(want) {
		t.Fatalf("nextAlignedRun() = %s, want %s", got, want)
	}

	onBoundary := time.Date(2026, time.August, 18, 14, 0, 0, 0, now.Location())
	wantNext := time.Date(2026, time.August, 18, 15, 0, 0, 0, now.Location())
	if got := nextAlignedRun(onBoundary, time.Hour); !got.Equal(wantNext) {
		t.Fatalf("nextAlignedRun() on a boundary = %s, want %s", got, wantNext)
	}
}
