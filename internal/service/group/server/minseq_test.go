package server

import (
	"context"
	"testing"

	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

type fakeLeaver struct {
	raised []int64
}

func (f *fakeLeaver) LastSeq(context.Context, int64, string) int64 { return 7 }

func (f *fakeLeaver) RaiseMin(_ context.Context, uid int64, _ string, seq int64) error {
	if seq != 7 {
		panic("unexpected seq")
	}
	f.raised = append(f.raised, uid)
	return nil
}

func TestQuitRaisesMinSeq(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2},
	})
	leaver := &fakeLeaver{}
	server.SetConversation(leaver)
	if _, err := server.QuitGroup(context.Background(), &pb.QuitGroupArgs{
		GroupId: group.GetGroupId(), UserId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if len(leaver.raised) != 1 || leaver.raised[0] != 2 {
		t.Fatalf("raised = %v", leaver.raised)
	}
	if _, err := repo.Member(context.Background(), group.GetGroupId(), 2); err == nil {
		t.Fatal("member must be removed")
	}
}

func TestDismissRaisesAllMembers(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2, 3},
	})
	leaver := &fakeLeaver{}
	server.SetConversation(leaver)
	if _, err := server.DismissGroup(context.Background(), &pb.DismissGroupArgs{
		GroupId: group.GetGroupId(), OperatorId: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(leaver.raised) != 3 {
		t.Fatalf("raised = %v", leaver.raised)
	}
}
