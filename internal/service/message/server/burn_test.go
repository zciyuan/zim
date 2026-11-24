package message

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/idem"
)

// burnTestService returns a service wired with an in-memory repo and a fixed
// clock, without the directory (not needed for burn use cases).
func burnTestService() (*Service, *fakeRepo, *time.Time) {
	repo := &fakeRepo{mu: sync.Mutex{}}
	now := time.Unix(1_700_000_000, 0)
	service := &Service{
		repo: repo, seq: &fakeSeq{},
		idem: &fakeIdem{results: make(map[string]*idem.Result)},
		conv: &fakeConv{}, dir: &fakeDir{friend: true},
		ids: snowflake.NewWithClock(1, func() time.Time { return now }), now: func() time.Time { return now },
	}
	return service, repo, &now
}

func TestValidateBurn(t *testing.T) {
	valid := &SendRequest{ClientMsgID: "c1", MsgType: contract.MsgTypeText, Content: "hi", BurnTTL: 10}
	if err := validateBurn(valid); err != nil {
		t.Fatalf("text burn rejected: %v", err)
	}
	for _, msgType := range []int32{contract.MsgTypeImage, contract.MsgTypeVoice,
		contract.MsgTypeVideo, contract.MsgTypeFile, contract.MsgTypeLocation} {
		req := &SendRequest{ClientMsgID: "c1", MsgType: msgType, Content: "x", BurnTTL: 30}
		if err := validateBurn(req); err != nil {
			t.Fatalf("msg_type %d burn rejected: %v", msgType, err)
		}
	}
	if err := validateBurn(&SendRequest{MsgType: contract.MsgTypeText, Content: "x", BurnTTL: 13}); err == nil {
		t.Fatalf("non-tier ttl accepted")
	}
	if err := validateBurn(&SendRequest{MsgType: contract.MsgTypeCustom, Content: "x", BurnTTL: 10}); err == nil {
		t.Fatalf("custom type burn accepted")
	}
	if err := validateBurn(&SendRequest{MsgType: contract.MsgTypeText, Content: "x", BurnTTL: 10, ExtType: model.ExtTypeRedPacket}); err == nil {
		t.Fatalf("card burn accepted")
	}
	if err := validateBurn(&SendRequest{MsgType: contract.MsgTypeText, Content: "x"}); err != nil {
		t.Fatalf("non-burn message rejected: %v", err)
	}
}

func TestApplyBurn(t *testing.T) {
	now := int64(1_000_000)
	// Group: countdown starts at creation.
	ttl, expireAt, _, _ := applyBurn(&SendRequest{BurnTTL: 60}, now, true)
	if ttl != 60 || expireAt != now+60_000 {
		t.Fatalf("group burn: ttl=%d expireAt=%d", ttl, expireAt)
	}
	// Single: no countdown yet, but the 7-day backstop exists.
	ttl, expireAt, retentionAt, flags := applyBurn(&SendRequest{BurnTTL: 60}, now, false)
	if ttl != 60 || expireAt != 0 {
		t.Fatalf("single burn: ttl=%d expireAt=%d", ttl, expireAt)
	}
	if retentionAt != now+7*86400*1000 {
		t.Fatalf("single burn retentionAt = %d", retentionAt)
	}
	if flags != model.BurnAfterReadingFlags {
		t.Fatalf("security flags = %d, want %d", flags, model.BurnAfterReadingFlags)
	}
	// Non-burn: all zeros.
	ttl, expireAt, retentionAt, flags = applyBurn(&SendRequest{}, now, false)
	if ttl != 0 || expireAt != 0 || retentionAt != 0 || flags != 0 {
		t.Fatalf("non-burn metadata must be zero")
	}
}

func TestCheckGroupMuteMatrix(t *testing.T) {
	now := time.Now().UnixMilli()
	// Legacy window mute.
	if err := checkGroupMute(&Membership{GroupMute: now + 1000}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("legacy group mute not enforced")
	}
	// Mute-all: member rejected, whitelist exempt, admin exempt.
	if err := checkGroupMute(&Membership{MuteAll: true}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("mute-all not enforced")
	}
	if err := checkGroupMute(&Membership{MuteAll: true, IsWhitelist: true}, contract.MsgTypeText, now); err != nil {
		t.Fatalf("whitelist must be exempt: %v", err)
	}
	if err := checkGroupMute(&Membership{MuteAll: true, Role: 1}, contract.MsgTypeText, now); err != nil {
		t.Fatalf("admin must be exempt: %v", err)
	}
	// Scheduled mute active.
	if err := checkGroupMute(&Membership{ScheduledMuteActive: true}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("scheduled mute not enforced")
	}
	// New member protection.
	if err := checkGroupMute(&Membership{NewMemberUntil: now + 1000}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("new member protection not enforced")
	}
	// Category mute: default flags block images, allow text.
	if err := checkGroupMute(&Membership{DefaultMuteFlags: model.MuteFlagImageVideo}, contract.MsgTypeImage, now); err == nil {
		t.Fatalf("category mute not enforced for image")
	}
	if err := checkGroupMute(&Membership{DefaultMuteFlags: model.MuteFlagImageVideo}, contract.MsgTypeText, now); err != nil {
		t.Fatalf("text must pass image-only mute: %v", err)
	}
	// Full mute flag blocks everything.
	if err := checkGroupMute(&Membership{MemberMuteFlags: model.MuteFlagAll}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("mute flag all not enforced")
	}
	// Permanent restriction.
	if err := checkGroupMute(&Membership{MemberMuteUntil: -1}, contract.MsgTypeText, now); err == nil {
		t.Fatalf("permanent restriction not enforced")
	}
	// Clean member passes.
	if err := checkGroupMute(&Membership{}, contract.MsgTypeText, now); err != nil {
		t.Fatalf("clean member rejected: %v", err)
	}
}

func TestBurnReadAck(t *testing.T) {
	service, repo, _ := burnTestService()
	msg := &model.Message{
		ServerMsgID: "m1", ClientMsgID: "c1", ConversationID: "s_1_2", Seq: 1,
		SendID: 1, RecvID: 2, MsgType: contract.MsgTypeText, Content: "secret",
		BurnTTL: 10, MaxRetentionAt: time.Now().UnixMilli() + 86400_000,
	}
	repo.messages = append(repo.messages, msg)

	// Sender may not ack.
	if _, err := service.BurnReadAck(context.Background(), 1, "m1"); err == nil {
		t.Fatalf("sender ack accepted")
	}
	// Receiver acks: countdown = now + 10s.
	start, err := service.BurnReadAck(context.Background(), 2, "m1")
	if err != nil {
		t.Fatalf("receiver ack failed: %v", err)
	}
	if start.BurnExpireAt <= start.ServerNowMS || start.BurnExpireAt-start.ServerNowMS != 10_000 {
		t.Fatalf("countdown wrong: expire=%d now=%d", start.BurnExpireAt, start.ServerNowMS)
	}
	// Idempotent re-ack returns the same deadline.
	again, err := service.BurnReadAck(context.Background(), 2, "m1")
	if err != nil {
		t.Fatalf("re-ack failed: %v", err)
	}
	if again.BurnExpireAt != start.BurnExpireAt {
		t.Fatalf("re-ack changed deadline: %d != %d", again.BurnExpireAt, start.BurnExpireAt)
	}
	// Burned message rejects the ack.
	repo.messages[0].IsBurned = true
	if _, err := service.BurnReadAck(context.Background(), 2, "m1"); err == nil {
		t.Fatalf("burned message ack accepted")
	}
}

func TestExpireCandidates(t *testing.T) {
	service, repo, _ := burnTestService()
	now := time.Now()
	repo.messages = append(repo.messages,
		&model.Message{ServerMsgID: "expired", BurnTTL: 60, BurnExpireAt: now.UnixMilli() - 1},
		&model.Message{ServerMsgID: "retained", BurnTTL: 60, MaxRetentionAt: now.UnixMilli() - 1},
		&model.Message{ServerMsgID: "alive", BurnTTL: 60, BurnExpireAt: now.UnixMilli() + 60_000, MaxRetentionAt: now.UnixMilli() + 86400_000},
		&model.Message{ServerMsgID: "plain", BurnTTL: 0},
	)
	candidates, err := service.ExpireCandidates(context.Background(), now, 10)
	if err != nil {
		t.Fatalf("expire candidates failed: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want 2", len(candidates))
	}
}
