package server

import (
	"context"

	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

// CreateGroup handles the RPC.
func (s *Server) CreateGroup(ctx context.Context, args *pb.CreateGroupArgs) (*pb.GroupInfo, error) {
	return s.doCreateGroup(ctx, createGroupArgs{OwnerID: args.GetOwnerId(), Name: args.GetName(), FaceURL: args.GetFaceUrl(), GroupType: args.GetGroupType(), MemberIDs: args.GetMemberIds(), NeedVerification: args.GetNeedVerification()})
}

// GetGroup handles the RPC.
func (s *Server) GetGroup(ctx context.Context, args *pb.GetGroupArgs) (*pb.GroupInfo, error) {
	return s.doGetGroup(ctx, getGroupArgs{GroupID: args.GetGroupId()})
}

// UpdateGroup handles the RPC.
func (s *Server) UpdateGroup(ctx context.Context, args *pb.UpdateGroupArgs) (*pb.GroupInfo, error) {
	return s.doUpdateGroup(ctx, updateGroupArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), Name: args.GetName(), FaceURL: args.GetFaceUrl(), Notification: args.GetNotification(), NeedVerification: args.GetNeedVerification(), LookMemberInfo: args.GetLookMemberInfo(), ApplyMemberFriend: args.GetApplyMemberFriend()})
}

// DismissGroup handles the RPC.
func (s *Server) DismissGroup(ctx context.Context, args *pb.DismissGroupArgs) (*pb.OperationReply, error) {
	return s.doDismissGroup(ctx, dismissGroupArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId()})
}

// QuitGroup handles the RPC.
func (s *Server) QuitGroup(ctx context.Context, args *pb.QuitGroupArgs) (*pb.OperationReply, error) {
	return s.doQuitGroup(ctx, quitGroupArgs{GroupID: args.GetGroupId(), UserID: args.GetUserId()})
}

// InviteMembers handles the RPC.
func (s *Server) InviteMembers(ctx context.Context, args *pb.InviteMembersArgs) (*pb.OperationReply, error) {
	return s.doInviteMembers(ctx, inviteMembersArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserIDs: args.GetUserIds()})
}

// RemoveMember handles the RPC.
func (s *Server) RemoveMember(ctx context.Context, args *pb.RemoveMemberArgs) (*pb.OperationReply, error) {
	return s.doRemoveMember(ctx, removeMemberArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserID: args.GetUserId()})
}

// Members handles the RPC.
func (s *Server) Members(ctx context.Context, args *pb.MembersArgs) (*pb.MembersReply, error) {
	return s.doMembers(ctx, membersArgs{GroupID: args.GetGroupId(), Offset: args.GetOffset(), Limit: args.GetLimit()})
}

// MyGroups handles the RPC.
func (s *Server) MyGroups(ctx context.Context, args *pb.MyGroupsArgs) (*pb.MyGroupsReply, error) {
	return s.doMyGroups(ctx, myGroupsArgs{UserID: args.GetUserId()})
}

// SetAdmin handles the RPC.
func (s *Server) SetAdmin(ctx context.Context, args *pb.SetAdminArgs) (*pb.OperationReply, error) {
	return s.doSetAdmin(ctx, setAdminArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserID: args.GetUserId(), Admin: args.GetAdmin()})
}

// MuteMember handles the RPC.
func (s *Server) MuteMember(ctx context.Context, args *pb.MuteMemberArgs) (*pb.OperationReply, error) {
	return s.doMuteMember(ctx, muteMemberArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserID: args.GetUserId(), Until: args.GetUntil()})
}

// MuteAll handles the RPC.
func (s *Server) MuteAll(ctx context.Context, args *pb.MuteAllArgs) (*pb.OperationReply, error) {
	return s.doMuteAll(ctx, muteAllArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), Until: args.GetUntil()})
}

// GetMembership handles the RPC.
func (s *Server) GetMembership(ctx context.Context, args *pb.GetMembershipArgs) (*pb.GetMembershipReply, error) {
	return s.doGetMembership(ctx, getMembershipArgs{GroupID: args.GetGroupId(), UserID: args.GetUserId()})
}

// MemberIDs handles the RPC.
func (s *Server) MemberIDs(ctx context.Context, args *pb.MemberIDsArgs) (*pb.MemberIDsReply, error) {
	return s.doMemberIDs(ctx, memberIDsArgs{GroupID: args.GetGroupId()})
}

// TransferGroup handles the RPC.
func (s *Server) TransferGroup(ctx context.Context, args *pb.TransferGroupArgs) (*pb.OperationReply, error) {
	return s.doTransferGroup(ctx, transferGroupArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), NewOwnerID: args.GetNewOwnerId()})
}

// ApplyJoin handles the RPC.
func (s *Server) ApplyJoin(ctx context.Context, args *pb.ApplyJoinArgs) (*pb.OperationReply, error) {
	return s.doApplyJoin(ctx, applyJoinArgs{GroupID: args.GetGroupId(), UserID: args.GetUserId(), Why: args.GetWhy()})
}

// AnswerJoin handles the RPC.
func (s *Server) AnswerJoin(ctx context.Context, args *pb.AnswerJoinArgs) (*pb.OperationReply, error) {
	return s.doAnswerJoin(ctx, answerJoinArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserID: args.GetUserId(), Accept: args.GetAccept()})
}

// ListApplies handles the RPC.
func (s *Server) ListApplies(ctx context.Context, args *pb.ListAppliesArgs) (*pb.ListAppliesReply, error) {
	return s.doListApplies(ctx, listAppliesArgs{GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), Status: args.GetStatus(), Offset: args.GetOffset(), Limit: args.GetLimit()})
}

// SearchGroups handles the RPC.
func (s *Server) SearchGroups(ctx context.Context, args *pb.SearchGroupsArgs) (*pb.SearchGroupsReply, error) {
	return s.doSearchGroups(ctx, searchGroupsArgs{Keyword: args.GetKeyword(), Limit: args.GetLimit()})
}
