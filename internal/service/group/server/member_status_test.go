package server

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeStatusPub struct {
	mutes []memberStatusEvent
	kicks []memberStatusEvent
}

func (f *fakeStatusPub) PublishMute(_ context.Context, gid, uid int64, flags int32, until int64, operatorUID int64) error {
	f.mutes = append(f.mutes, memberStatusEvent{GroupID: gid, UID: uid, MuteFlags: flags, MuteUntil: until, OperatorUID: operatorUID})
	return nil
}

func (f *fakeStatusPub) PublishKick(_ context.Context, gid, uid int64, operatorUID int64) error {
	f.kicks = append(f.kicks, memberStatusEvent{GroupID: gid, UID: uid, OperatorUID: operatorUID})
	return nil
}

// TestMuteAndKickPublishMemberStatus 禁言/全员禁言/踢人必须发
// zim.pipe.groupMemberStatus 事件（ADR-0043 G-10）。
func TestMuteAndKickPublishMemberStatus(t *testing.T) {
	repo := newFakeRepo()
	repo.groups[5001] = &model.Group{ID: 5001, OwnerID: 10001}
	repo.members[5001] = map[int64]*model.GroupMember{
		10001: {GroupID: 5001, UserID: 10001, Role: model.GroupRoleOwner},
		10002: {GroupID: 5001, UserID: 10002, Role: model.GroupRoleMember},
	}
	pub := &fakeStatusPub{}
	server := NewServer(nil, repo)
	server.SetMemberStatusPublisher(pub)

	if _, err := server.doMuteMember(context.Background(), muteMemberArgs{
		GroupID: 5001, OperatorID: 10001, UserID: 10002, Until: 1_800_000_000_000,
	}); err != nil {
		t.Fatalf("mute member: err = %v", err)
	}
	if len(pub.mutes) != 1 || pub.mutes[0].UID != 10002 {
		t.Fatalf("mute event missing: %+v", pub.mutes)
	}

	if _, err := server.doMuteAll(context.Background(), muteAllArgs{
		GroupID: 5001, OperatorID: 10001, Until: 1_800_000_000_000,
	}); err != nil {
		t.Fatalf("mute all: err = %v", err)
	}
	if len(pub.mutes) != 2 || pub.mutes[1].MuteFlags != model.MuteFlagAll {
		t.Fatalf("mute-all event wrong: %+v", pub.mutes[1])
	}

	if _, err := server.doRemoveMember(context.Background(), removeMemberArgs{
		GroupID: 5001, OperatorID: 10001, UserID: 10002,
	}); err != nil {
		t.Fatalf("remove member: err = %v", err)
	}
	if len(pub.kicks) != 1 || pub.kicks[0].UID != 10002 {
		t.Fatalf("kick event missing: %+v", pub.kicks)
	}
}
