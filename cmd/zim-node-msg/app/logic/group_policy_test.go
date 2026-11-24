package logic

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGroupPolicyCheckSpamTiers(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	clock := time.Unix(1_700_000_000, 0)
	server.SetTime(clock)
	policy := &groupPolicy{client: client, now: func() time.Time { return clock }} // proxy nil: AutoMute no-ops
	advance := func(d time.Duration) {
		clock = clock.Add(d)
		// FastForward both advances miniredis's clock and processes TTLs.
		server.FastForward(d)
	}

	// First 5 sends inside the 5s window are allowed.
	for i := 0; i < 5; i++ {
		tier, err := policy.CheckSpam(context.Background(), 5001, 1001, 5, 5)
		if err != nil {
			t.Fatalf("check spam: err = %v", err)
		}
		if tier != 0 {
			t.Fatalf("send %d: tier = %d, want 0", i+1, tier)
		}
	}
	// 6th send over budget: first offense tier.
	tier, err := policy.CheckSpam(context.Background(), 5001, 1001, 5, 5)
	if err != nil {
		t.Fatalf("check spam: err = %v", err)
	}
	if tier != 1 {
		t.Fatalf("first offense tier = %d, want 1", tier)
	}
	// Two more offenses escalate to tier 2 then 3.
	advance(6 * time.Second) // advance past the 5s window
	for i := 0; i < 5; i++ {
		if _, err := policy.CheckSpam(context.Background(), 5001, 1001, 5, 5); err != nil {
			t.Fatal(err)
		}
	}
	tier, _ = policy.CheckSpam(context.Background(), 5001, 1001, 5, 5)
	if tier != 2 {
		t.Fatalf("second offense tier = %d, want 2", tier)
	}
	advance(6 * time.Second)
	for i := 0; i < 5; i++ {
		if _, err := policy.CheckSpam(context.Background(), 5001, 1001, 5, 5); err != nil {
			t.Fatal(err)
		}
	}
	tier, _ = policy.CheckSpam(context.Background(), 5001, 1001, 5, 5)
	if tier != 3 {
		t.Fatalf("third offense tier = %d, want 3", tier)
	}
	// A different user has its own window.
	tier, err = policy.CheckSpam(context.Background(), 5001, 1002, 5, 5)
	if err != nil || tier != 0 {
		t.Fatalf("other user tier = %d, err = %v; want 0", tier, err)
	}
	// Offense counter expires after 24h.
	advance(25 * time.Hour)
	if value, err := client.Exists(context.Background(), offenseKey(5001, 1001)).Result(); err != nil || value != 0 {
		t.Fatalf("offense key must expire: exists = %d, err = %v", value, err)
	}
}
