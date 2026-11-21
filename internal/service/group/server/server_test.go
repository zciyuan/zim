package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

func TestCreateAndMembers(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, err := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "family", GroupType: 3, MemberIds: []int64{2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if group.GetMemberCount() != 3 || group.GetMaxMember() != 20 {
		t.Fatalf("group = %+v", group)
	}
	members, err := server.Members(context.Background(), &pb.MembersArgs{GroupId: group.GetGroupId()})
	if err != nil || len(members.GetMembers()) != 3 {
		t.Fatalf("members = %+v, err = %v", members, err)
	}
	myGroups, err := server.MyGroups(context.Background(), &pb.MyGroupsArgs{UserId: 2})
	if err != nil || len(myGroups.GetGroups()) != 1 {
		t.Fatalf("groups = %+v, err = %v", myGroups, err)
	}
}

func TestPermissionsAndMute(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2},
	})
	gid := group.GetGroupId()
	if _, err := server.SetAdmin(context.Background(), &pb.SetAdminArgs{
		GroupId: gid, OperatorId: 2, UserId: 2, Admin: true,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-owner admin err = %v", err)
	}
	if _, err := server.SetAdmin(context.Background(), &pb.SetAdminArgs{
		GroupId: gid, OperatorId: 1, UserId: 2, Admin: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.MuteMember(context.Background(), &pb.MuteMemberArgs{
		GroupId: gid, OperatorId: 2, UserId: 1, Until: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.QuitGroup(context.Background(), &pb.QuitGroupArgs{
		GroupId: gid, UserId: 1,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("owner quit err = %v", err)
	}
	if _, err := server.RemoveMember(context.Background(), &pb.RemoveMemberArgs{
		GroupId: gid, OperatorId: 1, UserId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	ids, err := server.MemberIDs(context.Background(), &pb.MemberIDsArgs{GroupId: gid})
	if err != nil || len(ids.GetUserIds()) != 1 {
		t.Fatalf("ids = %+v, err = %v", ids, err)
	}
}

func TestUpdateInviteMuteAll(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{OwnerId: 1, Name: "g"})
	gid := group.GetGroupId()
	updated, err := server.UpdateGroup(context.Background(), &pb.UpdateGroupArgs{
		GroupId: gid, OperatorId: 1, Name: "new", Notification: "hello",
	})
	if err != nil || updated.GetName() != "new" {
		t.Fatalf("updated = %+v, err = %v", updated, err)
	}
	if _, err = server.InviteMembers(context.Background(), &pb.InviteMembersArgs{
		GroupId: gid, OperatorId: 1, UserIds: []int64{2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	membership, err := server.GetMembership(context.Background(), &pb.GetMembershipArgs{
		GroupId: gid, UserId: 3,
	})
	if err != nil || membership.GetMember().GetRole() != 0 {
		t.Fatalf("membership = %+v, err = %v", membership, err)
	}
	if _, err = server.MuteAll(context.Background(), &pb.MuteAllArgs{
		GroupId: gid, OperatorId: 2, Until: 500,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("mute all err = %v", err)
	}
	if _, err = server.MuteAll(context.Background(), &pb.MuteAllArgs{
		GroupId: gid, OperatorId: 1, Until: 500,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = server.RemoveMember(context.Background(), &pb.RemoveMemberArgs{
		GroupId: gid, OperatorId: 3, UserId: 2,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("remove err = %v", err)
	}
}

func TestFamilyLimit(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	memberIDs := make([]int64, 0, 25)
	for i := int64(2); i <= 26; i++ {
		memberIDs = append(memberIDs, i)
	}
	if _, err := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "big family", GroupType: 3, MemberIds: memberIDs,
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v", err)
	}
}

func TestLeaveAndDismiss(t *testing.T) {
	repo := newFakeRepo()
	server := NewServer(nil, repo)
	group, _ := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2},
	})
	gid := group.GetGroupId()
	if _, err := server.QuitGroup(context.Background(), &pb.QuitGroupArgs{
		GroupId: gid, UserId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.DismissGroup(context.Background(), &pb.DismissGroupArgs{
		GroupId: gid, OperatorId: 2,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("dismiss err = %v", err)
	}
	if _, err := server.DismissGroup(context.Background(), &pb.DismissGroupArgs{
		GroupId: gid, OperatorId: 1,
	}); err != nil {
		t.Fatal(err)
	}
}
