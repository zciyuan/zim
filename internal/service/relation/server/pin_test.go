package server

import (
	"context"
	"testing"

	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
)

func TestPinAndBlacklistList(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	_ = repo.AddFriends(context.Background(), 1, 2, 0)
	if _, err := server.PinFriend(context.Background(), &pb.PinFriendArgs{
		Uid: 1, FriendId: 2, Pinned: true,
	}); err != nil {
		t.Fatal(err)
	}
	friends, _ := server.ListFriends(context.Background(), &pb.ListFriendsArgs{Uid: 1})
	if friends.GetFriends()[0].GetPinned() != 1 {
		t.Fatalf("pinned = %+v", friends.GetFriends()[0])
	}
	if _, err := server.AddBlack(context.Background(), &pb.AddBlackArgs{Uid: 1, BlackId: 2}); err != nil {
		t.Fatal(err)
	}
	blacklist, err := server.ListBlacklist(context.Background(), &pb.ListBlacklistArgs{Uid: 1})
	if err != nil || len(blacklist.GetUids()) != 1 {
		t.Fatalf("blacklist = %+v, err = %v", blacklist, err)
	}
	if _, err = server.AddBlack(context.Background(), &pb.AddBlackArgs{
		Uid: 1, BlackId: 2, Remove: true,
	}); err != nil {
		t.Fatal(err)
	}
	blacklist, _ = server.ListBlacklist(context.Background(), &pb.ListBlacklistArgs{Uid: 1})
	if len(blacklist.GetUids()) != 0 {
		t.Fatalf("blacklist after remove = %+v", blacklist)
	}
}
