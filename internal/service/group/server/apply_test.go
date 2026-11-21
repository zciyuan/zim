package server

import (
	"context"
	"testing"

	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

func TestApplyAndAnswerJoin(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{OwnerId: 1, Name: "g", NeedVerification: 1})
	if _, err := server.ApplyJoin(context.Background(), &pb.ApplyJoinArgs{
		GroupId: group.GetGroupId(), UserId: 2, Why: "hi",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AnswerJoin(context.Background(), &pb.AnswerJoinArgs{
		GroupId: group.GetGroupId(), OperatorId: 1, UserId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Member(context.Background(), group.GetGroupId(), 2); err != nil {
		t.Fatal("accepted member missing")
	}
}

func TestTransferGroup(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2},
	})
	if _, err := server.TransferGroup(context.Background(), &pb.TransferGroupArgs{
		GroupId: group.GetGroupId(), OperatorId: 1, NewOwnerId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	updated, _ := repo.Get(context.Background(), group.GetGroupId())
	if updated.OwnerID != 2 {
		t.Fatalf("owner = %d", updated.OwnerID)
	}
}

func TestApplyAutoJoinWithoutVerification(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{OwnerId: 1, Name: "open"})
	if _, err := server.ApplyJoin(context.Background(), &pb.ApplyJoinArgs{GroupId: group.GetGroupId(), UserId: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Member(context.Background(), group.GetGroupId(), 2); err != nil {
		t.Fatal("auto-joined member missing")
	}
}

func TestDuplicateApplyIsRejected(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", NeedVerification: 1,
	})
	for i := 0; i < 2; i++ {
		_, err := server.ApplyJoin(context.Background(), &pb.ApplyJoinArgs{
			GroupId: group.GetGroupId(), UserId: 2,
		})
		if i == 0 && err != nil {
			t.Fatal(err)
		}
		if i == 1 && err == nil {
			t.Fatal("duplicate apply must be rejected")
		}
	}
}
