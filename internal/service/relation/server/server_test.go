package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
)

func TestFriendRequestFlow(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2, Why: "hi",
	}); err != nil {
		t.Fatal(err)
	}
	requests, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 2, Inbound: true,
	})
	if err != nil || len(requests.GetRequests()) != 1 {
		t.Fatalf("requests = %+v, err = %v", requests, err)
	}
	if _, err = server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}
	check, err := server.IsFriend(context.Background(), &pb.IsFriendArgs{Uid: 1, TargetId: 2})
	if err != nil || !check.GetFriend() {
		t.Fatalf("check = %+v, err = %v", check, err)
	}
	// The handled request must disappear from the recipient's inbox.
	inbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 2, Inbound: true,
	})
	if err != nil || len(inbox.GetRequests()) != 0 {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	// The sender keeps the outbound record and its final status.
	outbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 1, Inbound: false,
	})
	if err != nil || len(outbox.GetRequests()) != 1 || outbox.GetRequests()[0].GetStatus() != int32(model.FriendRequestAccepted) {
		t.Fatalf("outbox = %+v, err = %v", outbox, err)
	}
	// Retrying accept after a lost response is idempotent.
	if _, err = server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatalf("idempotent accept = %v", err)
	}
	if _, err = server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate err = %v", err)
	}
}

func TestFriendRequestReciprocalAccept(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	for _, pair := range [][2]int64{{1, 2}, {2, 1}} {
		if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
			FromId: pair[0], ToId: pair[1],
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []int64{1, 2} {
		inbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
			Uid: uid, Inbound: true,
		})
		if err != nil || len(inbox.GetRequests()) != 0 {
			t.Fatalf("uid %d inbox = %+v, err = %v", uid, inbox, err)
		}
		outbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
			Uid: uid, Inbound: false,
		})
		if err != nil || len(outbox.GetRequests()) != 1 || outbox.GetRequests()[0].GetStatus() != int32(model.FriendRequestAccepted) {
			t.Fatalf("uid %d outbox = %+v, err = %v", uid, outbox, err)
		}
	}
}

func TestFriendRequestNotifications(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	notifier := &recordingNotifier{}
	server.SetWebhook(notifier)
	if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}

	events := notifier.list()
	accepted := 0
	for _, event := range events {
		if event.Type == "friend.rejected" {
			t.Fatalf("accept emitted a rejection event: %+v", events)
		}
		if event.Type == "friend.accepted" {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted events = %d, all = %+v", accepted, events)
	}
}

func TestFriendRequestReject(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: false,
	}); err != nil {
		t.Fatal(err)
	}
	inbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 2, Inbound: true,
	})
	if err != nil || len(inbox.GetRequests()) != 0 {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	outbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 1, Inbound: false,
	})
	if err != nil || len(outbox.GetRequests()) != 1 || outbox.GetRequests()[0].GetStatus() != int32(model.FriendRequestRejected) {
		t.Fatalf("outbox = %+v, err = %v", outbox, err)
	}
	check, err := server.IsFriend(context.Background(), &pb.IsFriendArgs{Uid: 1, TargetId: 2})
	if err != nil || check.GetFriend() {
		t.Fatalf("check = %+v, err = %v", check, err)
	}
}

func TestBlacklistAndRemark(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	_ = repo.AddFriends(context.Background(), 1, 2, 0)
	if _, err := server.UpdateRemark(context.Background(), &pb.UpdateRemarkArgs{
		Uid: 1, FriendId: 2, Remark: "buddy",
	}); err != nil {
		t.Fatal(err)
	}
	friends, err := server.ListFriends(context.Background(), &pb.ListFriendsArgs{Uid: 1})
	if err != nil || len(friends.GetFriends()) != 1 || friends.GetFriends()[0].GetRemark() != "buddy" {
		t.Fatalf("friends = %+v, err = %v", friends, err)
	}
	if _, err = server.AddBlack(context.Background(), &pb.AddBlackArgs{Uid: 2, BlackId: 1}); err != nil {
		t.Fatal(err)
	}
	blocked, err := server.IsFriend(context.Background(), &pb.IsFriendArgs{Uid: 1, TargetId: 2})
	if err != nil || !blocked.GetBlack() {
		t.Fatalf("blocked = %+v, err = %v", blocked, err)
	}
	if _, err = server.DeleteFriend(context.Background(), &pb.DeleteFriendArgs{Uid: 1, FriendId: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("blocked send err = %v", err)
	}
}

func TestAnswerMissingRequest(t *testing.T) {
	server := NewServer(nil, newFakeRepo())
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v", err)
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: false,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("reject err = %v", err)
	}
}

// TestAnswerRejectHandledRequest pins the contract for a stale inbox row: a
// request that is no longer pending must return 120002 (NotFound), not a 500.
func TestAnswerRejectHandledRequest(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: false,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("reject after accept err = %v", err)
	}
}

// TestAnswerOutboundRequestRejected verifies the direction check: answering a
// request the caller sent (from_id = caller) must not consume/accept it, and
// must not create a friendship without the recipient's consent.
func TestAnswerOutboundRequestRejected(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	if _, err := server.SendFriendRequest(context.Background(), &pb.SendFriendRequestArgs{
		FromId: 1, ToId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	// Caller 1 pretends to answer with friend_id = 2, i.e. from=2 to=1.
	for _, accept := range []bool{true, false} {
		if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
			FromId: 2, ToId: 1, Accept: accept,
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("accept=%v err = %v", accept, err)
		}
	}
	check, err := server.IsFriend(context.Background(), &pb.IsFriendArgs{Uid: 1, TargetId: 2})
	if err != nil || check.GetFriend() {
		t.Fatalf("friendship was created by the sender: %+v, err = %v", check, err)
	}
	inbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 2, Inbound: true,
	})
	if err != nil || len(inbox.GetRequests()) != 1 {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	// The real recipient can still answer it.
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatal(err)
	}
}

// TestAnswerStalePendingWithFriendship reproduces old data: the pair is already
// friends but a pending request row is still stored. Accepting must close the
// row instead of failing, and the inbox must be empty afterwards.
func TestAnswerStalePendingWithFriendship(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	_ = repo.AddFriends(context.Background(), 1, 2, 0)
	repo.requests[42] = &model.FriendRequest{
		ID: 42, FromID: 1, ToID: 2, Status: model.FriendRequestPending,
	}
	if _, err := server.AnswerFriend(context.Background(), &pb.AnswerFriendArgs{
		FromId: 1, ToId: 2, Accept: true,
	}); err != nil {
		t.Fatalf("accept stale request = %v", err)
	}
	inbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 2, Inbound: true,
	})
	if err != nil || len(inbox.GetRequests()) != 0 {
		t.Fatalf("inbox = %+v, err = %v", inbox, err)
	}
	outbox, err := server.ListFriendRequests(context.Background(), &pb.ListFriendRequestsArgs{
		Uid: 1, Inbound: false,
	})
	if err != nil || len(outbox.GetRequests()) != 1 ||
		outbox.GetRequests()[0].GetStatus() != int32(model.FriendRequestAccepted) {
		t.Fatalf("outbox = %+v, err = %v", outbox, err)
	}
}
