package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// SetGroupSecurity handles op:1318 (ADR-0043).
func (s *Server) SetGroupSecurity(ctx context.Context, args *pb.SetGroupSecurityArgs) (*pb.SecuritySettingsReply, error) {
	return s.doSetGroupSecurity(ctx, setGroupSecurityArgs{
		GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), MuteAll: args.GetMuteAll(),
		ScheduledMuteEnable: args.GetScheduledMuteEnable(), ScheduledMuteStart: args.GetScheduledMuteStart(),
		ScheduledMuteEnd: args.GetScheduledMuteEnd(), ScheduledTimezone: args.GetScheduledTimezone(),
		DefaultMuteFlags: args.GetDefaultMuteFlags(), NewMemberMuteSec: args.GetNewMemberMuteSec(),
		AntiSpamEnable: args.GetAntiSpamEnable(), AntiSpamWindowSec: args.GetAntiSpamWindowSec(),
		AntiSpamMaxMsgs: args.GetAntiSpamMaxMsgs(), SensitiveAutoMuteS: args.GetSensitiveAutoMuteS(),
		AntiScreenshot: args.GetAntiScreenshot(), WatermarkEnable: args.GetWatermarkEnable(),
		ScreenshotPenalty: args.GetScreenshotPenalty(),
	})
}

// SetMemberRestriction handles op:1319 (ADR-0043).
func (s *Server) SetMemberRestriction(ctx context.Context, args *pb.SetMemberRestrictionArgs) (*pb.OperationReply, error) {
	return s.doSetMemberRestriction(ctx, setMemberRestrictionArgs{
		GroupID: args.GetGroupId(), OperatorID: args.GetOperatorId(), UserID: args.GetUserId(),
		MuteFlags: args.GetMuteFlags(), IsWhitelist: args.GetIsWhitelist(), MuteUntil: args.GetMuteUntil(),
	})
}

// RecordDLPEvent appends one screenshot/record audit row (ADR-0043, op:1519).
func (s *Server) RecordDLPEvent(ctx context.Context, args *pb.RecordDLPEventArgs) (*pb.OperationReply, error) {
	err := s.doRecordDLPEvent(ctx, dlpEventArgs{
		ConvID: args.GetConvId(), ConvType: int16(args.GetConvType()), UID: args.GetUid(),
		EventType: int16(args.GetEventType()), DeviceInfo: args.GetDeviceInfo(),
		ActionTaken: int16(args.GetActionTaken()),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "dlp event failed")
	}
	return &pb.OperationReply{Ok: true}, nil
}

// GetSecurity returns the full security settings of a group (ADR-0043).
func (s *Server) GetSecurity(ctx context.Context, args *pb.GetSecurityArgs) (*pb.GetSecurityReply, error) {
	settings, err := s.repo.GetSecurity(ctx, args.GetGroupId())
	if err != nil {
		return nil, status.Error(codes.Internal, "security lookup failed")
	}
	return &pb.GetSecurityReply{Security: securityState(settings, nil, nil, time.Now().UnixMilli())}, nil
}
