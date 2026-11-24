package message

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

type fakePolicy struct {
	tier        int
	muted       []int64
	muteSeconds int64
}

func (f *fakePolicy) CheckSpam(context.Context, int64, int64, int32, int32) (int, error) {
	return f.tier, nil
}

func (f *fakePolicy) AutoMute(_ context.Context, _ int64, uid int64, seconds int64) error {
	f.muted = append(f.muted, uid)
	f.muteSeconds = seconds
	return nil
}

func TestCheckGroupSpam(t *testing.T) {
	// Disabled: no policy.
	service := &Service{}
	if err := service.checkGroupSpam(context.Background(), 1, 5001, &Membership{AntiSpamEnable: true}); err != nil {
		t.Fatalf("nil policy must pass: %v", err)
	}
	// Anti-spam off: no check.
	policy := &fakePolicy{tier: 1}
	service.policy = policy
	if err := service.checkGroupSpam(context.Background(), 1, 5001, &Membership{AntiSpamEnable: false}); err != nil {
		t.Fatalf("disabled anti-spam must pass: %v", err)
	}
	// Tier 1 → 5min mute + 130104.
	if err := service.checkGroupSpam(context.Background(), 1, 5001, &Membership{AntiSpamEnable: true, AntiSpamWindowSec: 5, AntiSpamMaxMsgs: 5}); !errs.IsCode(err, 130104) {
		t.Fatalf("want 130104, got %v", err)
	}
	if len(policy.muted) != 1 || policy.muted[0] != 1 || policy.muteSeconds != 300 {
		t.Fatalf("tier1 mute: muted = %v, seconds = %d", policy.muted, policy.muteSeconds)
	}
	// Tier 2 → 1h; tier 3+ → 24h.
	policy.tier = 2
	if err := service.checkGroupSpam(context.Background(), 1, 5001, &Membership{AntiSpamEnable: true}); !errs.IsCode(err, 130104) {
		t.Fatalf("tier2 want 130104, got %v", err)
	}
	if policy.muteSeconds != 3600 {
		t.Fatalf("tier2 seconds = %d, want 3600", policy.muteSeconds)
	}
	policy.tier = 5
	if err := service.checkGroupSpam(context.Background(), 1, 5001, &Membership{AntiSpamEnable: true}); !errs.IsCode(err, 130104) {
		t.Fatalf("tier5 want 130104, got %v", err)
	}
	if policy.muteSeconds != 86400 {
		t.Fatalf("tier5 seconds = %d, want 86400", policy.muteSeconds)
	}
}
