package logic

import (
	"testing"
	"time"
)

// TestTrackPresenceInitializesMap covers the 2026-11-17 production panic:
// c.watches was never created, so the first 1106 subscribe panicked with
// "assignment to entry in nil map" and online pushes (1903) stayed dead.
func TestTrackPresenceInitializesMap(t *testing.T) {
	c := &core{}
	expires := time.Now().Add(time.Minute)
	last := map[int64]bool{7: true, 8: false}

	c.trackPresence(42, []int64{7, 8}, last, expires)

	if c.watches == nil {
		t.Fatal("watches map was not initialized")
	}
	watch := c.watches[42]
	if watch == nil {
		t.Fatal("watch for device key 42 missing")
	}
	if len(watch.targets) != 2 || watch.targets[0] != 7 || watch.targets[1] != 8 {
		t.Fatalf("targets = %v", watch.targets)
	}
	if !watch.last[7] || watch.last[8] {
		t.Fatalf("last snapshot = %v", watch.last)
	}
	if !watch.expires.Equal(expires) {
		t.Fatalf("expires = %v, want %v", watch.expires, expires)
	}

	// A second subscription for the same device replaces the entry.
	c.trackPresence(42, []int64{9}, map[int64]bool{9: true}, expires.Add(time.Minute))
	if len(c.watches) != 1 {
		t.Fatalf("watches has %d entries, want 1", len(c.watches))
	}
	if got := c.watches[42]; len(got.targets) != 1 || got.targets[0] != 9 {
		t.Fatalf("replaced watch = %+v", got)
	}
}
