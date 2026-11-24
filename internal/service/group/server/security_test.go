package server

import (
	"context"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
)

func TestScheduledMuteActive(t *testing.T) {
	location := time.FixedZone("CST", 8*3600)
	settings := &model.GroupSecuritySettings{
		ScheduledMuteEnable: true, ScheduledMuteStart: "23:00",
		ScheduledMuteEnd: "07:00", ScheduledTimezone: "Asia/Shanghai",
	}
	// 23:00 local -> inside the overnight window.
	at := time.Date(2026, 11, 24, 23, 30, 0, 0, location)
	if !scheduledMuteActive(settings, at.UnixMilli()) {
		t.Fatalf("23:30 must be muted")
	}
	// 03:00 local (next day) -> inside.
	at = time.Date(2026, 11, 25, 3, 0, 0, 0, location)
	if !scheduledMuteActive(settings, at.UnixMilli()) {
		t.Fatalf("03:00 must be muted")
	}
	// 12:00 local -> outside.
	at = time.Date(2026, 11, 25, 12, 0, 0, 0, location)
	if scheduledMuteActive(settings, at.UnixMilli()) {
		t.Fatalf("12:00 must not be muted")
	}
	// Disabled -> never active.
	disabled := &model.GroupSecuritySettings{}
	if scheduledMuteActive(disabled, at.UnixMilli()) {
		t.Fatalf("disabled schedule must not be active")
	}
	// Same start/end -> never active.
	same := &model.GroupSecuritySettings{ScheduledMuteEnable: true, ScheduledMuteStart: "10:00", ScheduledMuteEnd: "10:00", ScheduledTimezone: "Asia/Shanghai"}
	if scheduledMuteActive(same, at.UnixMilli()) {
		t.Fatalf("empty window must not be active")
	}
}

func TestScheduledMuteActiveDayWindow(t *testing.T) {
	location := time.FixedZone("CST", 8*3600)
	settings := &model.GroupSecuritySettings{
		ScheduledMuteEnable: true, ScheduledMuteStart: "09:00",
		ScheduledMuteEnd: "17:00", ScheduledTimezone: "Asia/Shanghai",
	}
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, location)
	if !scheduledMuteActive(settings, at.UnixMilli()) {
		t.Fatalf("10:00 must be muted in day window")
	}
	at = time.Date(2026, 11, 24, 18, 0, 0, 0, location)
	if scheduledMuteActive(settings, at.UnixMilli()) {
		t.Fatalf("18:00 must not be muted")
	}
}

func TestValidateSchedule(t *testing.T) {
	if err := validateSchedule("23:00", "07:00", "Asia/Shanghai"); err != nil {
		t.Fatalf("valid schedule rejected: %v", err)
	}
	if err := validateSchedule("", "", ""); err != nil {
		t.Fatalf("empty schedule must pass: %v", err)
	}
	if err := validateSchedule("25:00", "07:00", "Asia/Shanghai"); err == nil {
		t.Fatalf("invalid hour accepted")
	}
	if err := validateSchedule("23:00", "07:00", "Not/AZone"); err == nil {
		t.Fatalf("invalid timezone accepted")
	}
}

func TestSetGroupSecurityRequiresAdmin(t *testing.T) {
	repo := newFakeRepo()
	repo.groups[5001] = &model.Group{ID: 5001, OwnerID: 10001}
	repo.members[5001] = map[int64]*model.GroupMember{
		10001: {GroupID: 5001, UserID: 10001, Role: model.GroupRoleOwner},
		10002: {GroupID: 5001, UserID: 10002, Role: model.GroupRoleMember},
	}
	server := NewServer(nil, repo)
	_, err := server.doSetGroupSecurity(context.Background(), setGroupSecurityArgs{
		GroupID: 5001, OperatorID: 10002, MuteAll: true,
	})
	if err == nil {
		t.Fatalf("member must not set group security")
	}
	reply, err := server.doSetGroupSecurity(context.Background(), setGroupSecurityArgs{
		GroupID: 5001, OperatorID: 10001, MuteAll: true,
		ScheduledMuteEnable: true, ScheduledMuteStart: "23:00", ScheduledMuteEnd: "07:00",
		ScheduledTimezone: "Asia/Shanghai", ScreenshotPenalty: 2,
	})
	if err != nil {
		t.Fatalf("owner set security failed: %v", err)
	}
	if !reply.GetSecurity().GetMuteAll() || reply.GetSecurity().GetScreenshotPenalty() != 2 {
		t.Fatalf("security not persisted: %+v", reply.GetSecurity())
	}
}

func TestSetMemberRestrictionRoleGuard(t *testing.T) {
	repo := newFakeRepo()
	repo.groups[5001] = &model.Group{ID: 5001, OwnerID: 10001}
	repo.members[5001] = map[int64]*model.GroupMember{
		10001: {GroupID: 5001, UserID: 10001, Role: model.GroupRoleOwner},
		10002: {GroupID: 5001, UserID: 10002, Role: model.GroupRoleAdmin},
		10003: {GroupID: 5001, UserID: 10003, Role: model.GroupRoleMember},
	}
	server := NewServer(nil, repo)
	// Admin cannot restrict the owner.
	if _, err := server.doSetMemberRestriction(context.Background(), setMemberRestrictionArgs{
		GroupID: 5001, OperatorID: 10002, UserID: 10001, MuteFlags: model.MuteFlagAll,
	}); err == nil {
		t.Fatalf("admin must not restrict owner")
	}
	// Admin cannot restrict another admin.
	if _, err := server.doSetMemberRestriction(context.Background(), setMemberRestrictionArgs{
		GroupID: 5001, OperatorID: 10002, UserID: 10002, MuteFlags: model.MuteFlagAll,
	}); err == nil {
		t.Fatalf("admin must not restrict admin")
	}
	// Owner can restrict an admin.
	if _, err := server.doSetMemberRestriction(context.Background(), setMemberRestrictionArgs{
		GroupID: 5001, OperatorID: 10001, UserID: 10002, MuteFlags: model.MuteFlagImageVideo,
	}); err != nil {
		t.Fatalf("owner restrict admin failed: %v", err)
	}
	restriction, err := repo.GetRestriction(context.Background(), 5001, 10002)
	if err != nil || restriction == nil || restriction.MuteFlags != model.MuteFlagImageVideo {
		t.Fatalf("restriction not persisted: %+v, err = %v", restriction, err)
	}
}

func TestRecordDLPEvent(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	if err := server.doRecordDLPEvent(context.Background(), dlpEventArgs{
		ConvID: "g_5001", ConvType: model.ConversationGroup, UID: 10002,
		EventType: 1, DeviceInfo: "Pixel 8", ActionTaken: model.ScreenshotPenaltyMute24H,
	}); err != nil {
		t.Fatalf("record dlp event failed: %v", err)
	}
	if len(repo.dlpEvents) != 1 {
		t.Fatalf("dlp events = %d, want 1", len(repo.dlpEvents))
	}
	event := repo.dlpEvents[0]
	if event.ID <= 0 || event.ActionTaken != model.ScreenshotPenaltyMute24H {
		t.Fatalf("dlp event invalid: %+v", event)
	}
}

func TestSecurityStateWhitelistAndProtection(t *testing.T) {
	now := time.Now().UnixMilli()
	settings := &model.GroupSecuritySettings{NewMemberMuteSec: 600, DefaultMuteFlags: model.MuteFlagImageVideo}
	member := &model.GroupMember{JoinedAt: now - 1000}
	restriction := &model.GroupMemberRestriction{IsWhitelist: true, MuteFlags: model.MuteFlagAll, MuteUntil: -1}
	state := securityState(settings, restriction, member, now)
	if !state.GetIsWhitelist() {
		t.Fatalf("whitelist must propagate")
	}
	if state.GetMemberMuteFlags() != model.MuteFlagAll || state.GetMemberMuteUntil() != -1 {
		t.Fatalf("restriction must propagate: %+v", state)
	}
	if state.GetNewMemberUntil() <= 0 {
		t.Fatalf("new member protection must be active")
	}
}
