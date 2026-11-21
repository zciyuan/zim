package server

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

func TestGetProfileAndNotFound(t *testing.T) {
	server := NewServer(nil, newFakeRepo())
	profile, err := server.GetProfile(context.Background(), &pb.GetProfileArgs{Uid: 1})
	if err != nil {
		t.Fatal(err)
	}
	if profile.GetUsername() != "alice" || profile.GetNickname() != "Alice" {
		t.Fatalf("profile = %+v", profile)
	}
	_, err = server.GetProfile(context.Background(), &pb.GetProfileArgs{Uid: 99})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestBatchSearchAndUpdate(t *testing.T) {
	server := NewServer(nil, newFakeRepo())
	reply, err := server.BatchGetUsers(context.Background(), &pb.BatchGetUsersArgs{Uids: []int64{1, 2, 3}})
	if err != nil || len(reply.GetUsers()) != 2 {
		t.Fatalf("reply = %+v, err = %v", reply, err)
	}
	search, err := server.SearchUsers(context.Background(), &pb.SearchUsersArgs{Keyword: "bob"})
	if err != nil || len(search.GetUsers()) != 1 {
		t.Fatalf("search = %+v, err = %v", search, err)
	}
	updated, err := server.UpdateProfile(context.Background(), &pb.UpdateProfileArgs{
		Uid: 1, Nickname: "New", Avatar: "b", Gender: 2,
	})
	if err != nil || updated.GetNickname() != "New" {
		t.Fatalf("updated = %+v, err = %v", updated, err)
	}
}

func TestInvalidArguments(t *testing.T) {
	server := NewServer(nil, newFakeRepo())
	if _, err := server.GetProfile(context.Background(), &pb.GetProfileArgs{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v", err)
	}
	if _, err := server.UpdateProfile(context.Background(), &pb.UpdateProfileArgs{
		Uid: 1, Nickname: strings.Repeat("x", 40),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v", err)
	}
}
