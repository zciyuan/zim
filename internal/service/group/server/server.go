package server

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/conv"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// Repository is the group data required by the service.
type Repository interface {
	Create(ctx context.Context, group *model.Group) error
	Get(ctx context.Context, gid int64) (*model.Group, error)
	Update(ctx context.Context, group *model.Group) error
	Dismiss(ctx context.Context, gid int64) error
	TransferOwner(ctx context.Context, gid, oldOwner, newOwner int64) error
	CreateApply(ctx context.Context, apply *model.GroupApply) error
	PendingApply(ctx context.Context, gid, uid int64) (*model.GroupApply, error)
	ListApplies(ctx context.Context, gid int64, status int16, offset, limit int) ([]*model.GroupApply, error)
	Search(ctx context.Context, keyword string, limit int) ([]*model.Group, error)
	AnswerApply(ctx context.Context, id int64, status int16, handledAt int64) error
	AddMembers(ctx context.Context, members []*model.GroupMember) error
	RemoveMember(ctx context.Context, gid, uid int64) error
	Member(ctx context.Context, gid, uid int64) (*model.GroupMember, error)
	Members(ctx context.Context, gid int64, offset, limit int) ([]*model.GroupMember, error)
	MyGroups(ctx context.Context, uid int64) ([]*model.Group, error)
	SetRole(ctx context.Context, gid, uid int64, role int16) error
	SetMemberMute(ctx context.Context, gid, uid int64, until int64) error
	SetMutedUntil(ctx context.Context, gid int64, until int64) error
	CountMembers(ctx context.Context, gid int64) (int, error)
	MemberIDs(ctx context.Context, gid int64) ([]int64, error)
	// ADR-0043: security settings and member restrictions.
	GetSecurity(ctx context.Context, gid int64) (*model.GroupSecuritySettings, error)
	UpsertSecurity(ctx context.Context, settings *model.GroupSecuritySettings) error
	GetRestriction(ctx context.Context, gid, uid int64) (*model.GroupMemberRestriction, error)
	UpsertRestriction(ctx context.Context, restriction *model.GroupMemberRestriction) error
	InsertDLPEvent(ctx context.Context, event *model.DLPSecurityEvent) error
}

var _ pb.GroupServer = (*Server)(nil)

// NewServer returns the group service.
func NewServer(proxy *mesh.Proxy, repo Repository) *Server {
	return &Server{proxy: proxy, repo: repo, ids: snowflake.New()}
}

// Init registers the service provider under the discovery name group.
func (s *Server) Init() {
	s.proxy.AddServiceProvider(define.ServiceNameGroup, &pb.Group_ServiceDesc, s)
}

// SetChangeLog enables member version-log writes.
func (s *Server) SetChangeLog(changes *changelog.Service) {
	s.changes = changes
}

// SetConversation enables min_seq raising on leave/kick/dismiss.
func (s *Server) SetConversation(conv ConversationLeaver) {
	s.conv = conv
}

// SetGroupVersion enables member/join version counters.
func (s *Server) SetGroupVersion(versions GroupVersion) {
	s.versions = versions
}

// SetMemberCache enables versioned group member snapshots.
func (s *Server) SetMemberCache(cache *MemberCache) {
	s.members = cache
}

// SetWebhook enables group event callbacks.
func (s *Server) SetWebhook(notifier webhook.Notifier) {
	s.webhook = notifier
}

func (s *Server) notify(eventType string, gid, uid int64, status string) {
	if s.webhook == nil {
		return
	}
	s.webhook.Notify(webhook.Event{Type: eventType, GroupID: gid, SendID: uid, Status: status})
}

func (s *Server) leave(ctx context.Context, gid, uid int64) error {
	if s.conv == nil {
		return nil
	}
	convID := conv.Group(gid)
	return s.conv.RaiseMin(ctx, uid, convID, s.conv.LastSeq(ctx, uid, convID))
}

func (s *Server) record(ctx context.Context, gid, uid int64, state string) {
	if s.versions != nil {
		if _, err := s.versions.IncrMember(ctx, gid); err != nil {
			log.Warnf("incr group member version failed: gid = %d, err = %v", gid, err)
		}
		if state == model.ChangeInsert {
			if _, err := s.versions.IncrJoin(ctx, gid); err != nil {
				log.Warnf("incr group join version failed: gid = %d, err = %v", gid, err)
			}
		}
	}
	eventType := "group.member.updated"
	switch state {
	case model.ChangeInsert:
		eventType = "group.member.joined"
	case model.ChangeDelete:
		eventType = "group.member.left"
	}
	s.notify(eventType, gid, uid, state)
	if s.changes == nil {
		return
	}
	objID := strconv.FormatInt(gid, 10) + ":" + strconv.FormatInt(uid, 10)
	if err := s.changes.Append(ctx, uid, model.EntityGroupMember, objID, state); err != nil {
		log.Warnf("append group member change log failed: gid = %d, uid = %d, state = %s, err = %v",
			gid, uid, state, err)
	}
}

// CreateGroup creates a group and its initial memberships.
func (s *Server) doCreateGroup(ctx context.Context, args createGroupArgs) (*pb.GroupInfo, error) {
	name := strings.TrimSpace(args.Name)
	if name == "" || len([]rune(name)) > 64 || args.OwnerID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid group")
	}
	groupType := int16(args.GroupType)
	if groupType == 0 {
		groupType = model.GroupTypeNormal
	}
	if groupType != model.GroupTypeNormal && groupType != model.GroupTypeAuthorized && groupType != model.GroupTypeFamily {
		return nil, status.Error(codes.InvalidArgument, "invalid group type")
	}
	maxMember := model.MaxGroupMembers
	if groupType == model.GroupTypeFamily {
		maxMember = model.MaxFamilyMembers
	}
	members := make([]*model.GroupMember, 0, len(args.MemberIDs)+1)
	seen := map[int64]struct{}{args.OwnerID: {}}
	now := time.Now().UnixMilli()
	for _, uid := range args.MemberIDs {
		if uid <= 0 || uid == args.OwnerID {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		if len(members)+1 >= int(maxMember) {
			return nil, status.Error(codes.ResourceExhausted, "group member limit reached")
		}
		seen[uid] = struct{}{}
		members = append(members, &model.GroupMember{UserID: uid, Role: model.GroupRoleMember, JoinedAt: now})
	}
	group := &model.Group{
		ID: s.ids.Next(), Name: name, FaceURL: args.FaceURL, OwnerID: args.OwnerID,
		GroupType: groupType, MaxMember: maxMember, NeedVerification: int16(args.NeedVerification),
		LookMemberInfo: 1, ApplyMemberFriend: 1, CreatedAt: now,
	}
	if err := s.repo.Create(ctx, group); err != nil {
		return nil, status.Error(codes.Internal, "create group failed")
	}
	for _, member := range members {
		member.GroupID = group.ID
	}
	if len(members) > 0 {
		if err := s.repo.AddMembers(ctx, members); err != nil {
			return nil, status.Error(codes.Internal, "add group members failed")
		}
	}
	s.record(ctx, group.ID, group.OwnerID, model.ChangeInsert)
	for _, member := range members {
		s.record(ctx, group.ID, member.UserID, model.ChangeInsert)
	}
	s.notify("group.created", group.ID, group.OwnerID, "created")
	return s.groupInfo(ctx, group)
}

// GetGroup returns group info.
func (s *Server) doGetGroup(ctx context.Context, args getGroupArgs) (*pb.GroupInfo, error) {
	group, err := s.repo.Get(ctx, args.GroupID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "group not found")
		}
		return nil, status.Error(codes.Internal, "group lookup failed")
	}
	return s.groupInfo(ctx, group)
}

// UpdateGroup updates editable fields.
func (s *Server) doUpdateGroup(ctx context.Context, args updateGroupArgs) (*pb.GroupInfo, error) {
	group, member, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if member.Role != model.GroupRoleOwner && member.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if name := strings.TrimSpace(args.Name); name != "" {
		group.Name = name
	}
	if args.FaceURL != "" {
		group.FaceURL = args.FaceURL
	}
	if args.Notification != "" {
		group.Notification = args.Notification
	}
	group.NeedVerification = int16(args.NeedVerification)
	group.LookMemberInfo = int16(args.LookMemberInfo)
	group.ApplyMemberFriend = int16(args.ApplyMemberFriend)
	if err = s.repo.Update(ctx, group); err != nil {
		return nil, status.Error(codes.Internal, "update group failed")
	}
	return s.groupInfo(ctx, group)
}

// DismissGroup deletes a group; only the owner may do so.
func (s *Server) doDismissGroup(ctx context.Context, args dismissGroupArgs) (*pb.OperationReply, error) {
	group, err := s.repo.Get(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if group.OwnerID != args.OperatorID {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	memberIDs, err := s.repo.MemberIDs(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.Internal, "member lookup failed")
	}
	for _, uid := range memberIDs {
		if err = s.leave(ctx, args.GroupID, uid); err != nil {
			return nil, status.Error(codes.Internal, "raise min_seq failed")
		}
	}
	if err = s.repo.Dismiss(ctx, args.GroupID); err != nil {
		return nil, status.Error(codes.Internal, "dismiss group failed")
	}
	for _, uid := range memberIDs {
		s.record(ctx, args.GroupID, uid, model.ChangeDelete)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// QuitGroup removes a non-owner member.
func (s *Server) doQuitGroup(ctx context.Context, args quitGroupArgs) (*pb.OperationReply, error) {
	group, err := s.repo.Get(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if group.OwnerID == args.UserID {
		return nil, status.Error(codes.FailedPrecondition, "owner cannot quit group")
	}
	if err = s.leave(ctx, args.GroupID, args.UserID); err != nil {
		return nil, status.Error(codes.Internal, "raise min_seq failed")
	}
	if err = s.repo.RemoveMember(ctx, args.GroupID, args.UserID); err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	s.record(ctx, args.GroupID, args.UserID, model.ChangeDelete)
	return &pb.OperationReply{Ok: true}, nil
}

// InviteMembers adds members up to the group limit.
func (s *Server) doInviteMembers(ctx context.Context, args inviteMembersArgs) (*pb.OperationReply, error) {
	group, _, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	count, err := s.repo.CountMembers(ctx, group.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, "member count failed")
	}
	if count+len(args.UserIDs) > int(group.MaxMember) {
		return nil, status.Error(codes.ResourceExhausted, "group member limit reached")
	}
	now := time.Now().UnixMilli()
	members := make([]*model.GroupMember, 0, len(args.UserIDs))
	for _, uid := range args.UserIDs {
		if uid > 0 && uid != group.OwnerID {
			members = append(members, &model.GroupMember{
				GroupID: group.ID, UserID: uid, Role: model.GroupRoleMember, JoinedAt: now,
			})
		}
	}
	if err = s.repo.AddMembers(ctx, members); err != nil {
		return nil, status.Error(codes.Internal, "invite members failed")
	}
	for _, member := range members {
		s.record(ctx, group.ID, member.UserID, model.ChangeInsert)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// RemoveMember kicks a member; admins cannot remove peers or the owner.
func (s *Server) doRemoveMember(ctx context.Context, args removeMemberArgs) (*pb.OperationReply, error) {
	group, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if args.UserID == group.OwnerID {
		return nil, status.Error(codes.PermissionDenied, "cannot remove owner")
	}
	if err = s.leave(ctx, args.GroupID, args.UserID); err != nil {
		return nil, status.Error(codes.Internal, "raise min_seq failed")
	}
	if err = s.repo.RemoveMember(ctx, args.GroupID, args.UserID); err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	s.record(ctx, args.GroupID, args.UserID, model.ChangeDelete)
	// ADR-0043 G-10: a kicked member must be evicted from the voice room.
	if s.statusPub != nil {
		_ = s.statusPub.PublishKick(ctx, args.GroupID, args.UserID, args.OperatorID)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// Members returns a page of group members.
func (s *Server) doMembers(ctx context.Context, args membersArgs) (*pb.MembersReply, error) {
	limit := int(args.Limit)
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	members, err := s.repo.Members(ctx, args.GroupID, int(args.Offset), limit)
	if err != nil {
		return nil, status.Error(codes.Internal, "members lookup failed")
	}
	reply := &pb.MembersReply{Members: make([]*pb.Membership, 0, len(members))}
	for _, member := range members {
		reply.Members = append(reply.Members, membership(member))
	}
	return reply, nil
}

// MyGroups returns groups the user belongs to.
func (s *Server) doMyGroups(ctx context.Context, args myGroupsArgs) (*pb.MyGroupsReply, error) {
	groups, err := s.repo.MyGroups(ctx, args.UserID)
	if err != nil {
		return nil, status.Error(codes.Internal, "groups lookup failed")
	}
	reply := &pb.MyGroupsReply{Groups: make([]*pb.GroupInfo, 0, len(groups))}
	for _, group := range groups {
		info, infoErr := s.groupInfo(ctx, group)
		if infoErr != nil {
			return nil, infoErr
		}
		reply.Groups = append(reply.Groups, info)
	}
	return reply, nil
}

// SetAdmin promotes or demotes a member; only the owner may do so.
func (s *Server) doSetAdmin(ctx context.Context, args setAdminArgs) (*pb.OperationReply, error) {
	group, err := s.repo.Get(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if group.OwnerID != args.OperatorID {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	role := model.GroupRoleMember
	if args.Admin {
		role = model.GroupRoleAdmin
	}
	if err = s.repo.SetRole(ctx, group.ID, args.UserID, role); err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	s.record(ctx, group.ID, args.UserID, model.ChangeUpdate)
	return &pb.OperationReply{Ok: true}, nil
}

// MuteMember sets a member mute deadline.
func (s *Server) doMuteMember(ctx context.Context, args muteMemberArgs) (*pb.OperationReply, error) {
	_, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if err = s.repo.SetMemberMute(ctx, args.GroupID, args.UserID, args.Until); err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	s.record(ctx, args.GroupID, args.UserID, model.ChangeUpdate)
	// ADR-0043 G-10: mute/unmute must reach voiceroom to revoke/restore
	// LiveKit publish rights.
	if s.statusPub != nil {
		_ = s.statusPub.PublishMute(ctx, args.GroupID, args.UserID, 0, args.Until, args.OperatorID)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// MuteAll sets the whole-group mute deadline.
func (s *Server) doMuteAll(ctx context.Context, args muteAllArgs) (*pb.OperationReply, error) {
	_, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if err = s.repo.SetMutedUntil(ctx, args.GroupID, args.Until); err != nil {
		return nil, status.Error(codes.Internal, "mute group failed")
	}
	// ADR-0043 G-10: mute-all applies to every member, so the event carries
	// the full-mute flag; voiceroom revokes every room member's publish right.
	if s.statusPub != nil {
		_ = s.statusPub.PublishMute(ctx, args.GroupID, 0, model.MuteFlagAll, args.Until, args.OperatorID)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// GetMembership returns group info and the caller's membership.
func (s *Server) doGetMembership(ctx context.Context, args getMembershipArgs) (*pb.GetMembershipReply, error) {
	group, member, err := s.groupAndMember(ctx, args.GroupID, args.UserID)
	if err != nil {
		return nil, err
	}
	info, err := s.groupInfo(ctx, group)
	if err != nil {
		return nil, err
	}
	// ADR-0043: attach the mute/security facts the message path needs.
	settings, secErr := s.repo.GetSecurity(ctx, args.GroupID)
	if secErr != nil {
		return nil, status.Error(codes.Internal, "security lookup failed")
	}
	restriction, restErr := s.repo.GetRestriction(ctx, args.GroupID, args.UserID)
	if restErr != nil {
		return nil, status.Error(codes.Internal, "restriction lookup failed")
	}
	now := time.Now().UnixMilli()
	return &pb.GetMembershipReply{
		Group:    info,
		Member:   membership(member),
		Security: securityState(settings, restriction, member, now),
	}, nil
}

// MemberIDs returns all member IDs for fanout.
func (s *Server) doMemberIDs(ctx context.Context, args memberIDsArgs) (*pb.MemberIDsReply, error) {
	ids, err := s.repo.MemberIDs(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.Internal, "member IDs failed")
	}
	return &pb.MemberIDsReply{UserIds: ids}, nil
}

func (s *Server) groupAndMember(ctx context.Context, gid, uid int64) (*model.Group, *model.GroupMember, error) {
	group, err := s.repo.Get(ctx, gid)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, nil, status.Error(codes.NotFound, "group not found")
		}
		return nil, nil, status.Error(codes.Internal, "group lookup failed")
	}
	member, err := s.repo.Member(ctx, gid, uid)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, nil, status.Error(codes.PermissionDenied, "not group member")
		}
		return nil, nil, status.Error(codes.Internal, "membership lookup failed")
	}
	return group, member, nil
}

func (s *Server) groupInfo(ctx context.Context, group *model.Group) (*pb.GroupInfo, error) {
	count, err := s.repo.CountMembers(ctx, group.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, "member count failed")
	}
	return &pb.GroupInfo{
		GroupId: group.ID, Name: group.Name, FaceUrl: group.FaceURL, Notification: group.Notification,
		OwnerId: group.OwnerID, GroupType: int32(group.GroupType), MaxMember: group.MaxMember,
		NeedVerification: int32(group.NeedVerification), LookMemberInfo: int32(group.LookMemberInfo),
		ApplyMemberFriend: int32(group.ApplyMemberFriend), Status: int32(group.Status),
		MutedUntil: group.MutedUntil, CreatedAt: group.CreatedAt, MemberCount: int32(count),
	}, nil
}

func membership(member *model.GroupMember) *pb.Membership {
	return &pb.Membership{
		GroupId: member.GroupID, UserId: member.UserID, Role: int32(member.Role),
		MuteUntil: member.MuteUntil, JoinedAt: member.JoinedAt,
	}
}

// TransferGroup transfers ownership to another member.
func (s *Server) doTransferGroup(ctx context.Context, args transferGroupArgs) (*pb.OperationReply, error) {
	group, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if group.OwnerID != args.OperatorID || operator.Role != model.GroupRoleOwner {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if args.NewOwnerID <= 0 || args.NewOwnerID == group.OwnerID {
		return nil, status.Error(codes.InvalidArgument, "invalid new owner")
	}
	if _, err = s.repo.Member(ctx, group.ID, args.NewOwnerID); err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	if err = s.repo.TransferOwner(ctx, group.ID, group.OwnerID, args.NewOwnerID); err != nil {
		return nil, status.Error(codes.Internal, "transfer owner failed")
	}
	s.record(ctx, group.ID, group.OwnerID, model.ChangeUpdate)
	s.record(ctx, group.ID, args.NewOwnerID, model.ChangeUpdate)
	return &pb.OperationReply{Ok: true}, nil
}

// ApplyJoin creates a pending join request.
func (s *Server) doApplyJoin(ctx context.Context, args applyJoinArgs) (*pb.OperationReply, error) {
	group, err := s.repo.Get(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if args.UserID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid user")
	}
	if _, err = s.repo.Member(ctx, group.ID, args.UserID); err == nil {
		return nil, status.Error(codes.AlreadyExists, "already a member")
	}
	if group.NeedVerification == 0 {
		if err = s.repo.AddMembers(ctx, []*model.GroupMember{{
			GroupID: group.ID, UserID: args.UserID, Role: model.GroupRoleMember,
			JoinedAt: time.Now().UnixMilli(),
		}}); err != nil {
			return nil, status.Error(codes.Internal, "add member failed")
		}
		s.record(ctx, group.ID, args.UserID, model.ChangeInsert)
		return &pb.OperationReply{Ok: true}, nil
	}
	if _, pendingErr := s.repo.PendingApply(ctx, group.ID, args.UserID); pendingErr == nil {
		return nil, status.Error(codes.AlreadyExists, "apply pending")
	}
	now := time.Now().UnixMilli()
	if err = s.repo.CreateApply(ctx, &model.GroupApply{
		ID: s.ids.Next(), GroupID: group.ID, UserID: args.UserID, Why: args.Why,
		Status: model.GroupApplyPending, CreatedAt: now,
	}); err != nil {
		return nil, status.Error(codes.Internal, "create apply failed")
	}
	s.notify("group.apply", group.ID, args.UserID, "pending")
	return &pb.OperationReply{Ok: true}, nil
}

// AnswerJoin accepts or rejects a pending join request.
func (s *Server) doAnswerJoin(ctx context.Context, args answerJoinArgs) (*pb.OperationReply, error) {
	group, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	apply, err := s.repo.PendingApply(ctx, group.ID, args.UserID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "apply not found")
	}
	statusValue := model.GroupApplyRejected
	if args.Accept {
		count, countErr := s.repo.CountMembers(ctx, group.ID)
		if countErr != nil {
			return nil, status.Error(codes.Internal, "member count failed")
		}
		if count >= int(group.MaxMember) {
			return nil, status.Error(codes.ResourceExhausted, "group member limit reached")
		}
		if err = s.repo.AddMembers(ctx, []*model.GroupMember{{
			GroupID: group.ID, UserID: args.UserID, Role: model.GroupRoleMember,
			JoinedAt: time.Now().UnixMilli(),
		}}); err != nil {
			return nil, status.Error(codes.Internal, "add member failed")
		}
		s.record(ctx, group.ID, args.UserID, model.ChangeInsert)
		statusValue = model.GroupApplyAccepted
	}
	if err = s.repo.AnswerApply(ctx, apply.ID, statusValue, time.Now().UnixMilli()); err != nil {
		return nil, status.Error(codes.Internal, "answer apply failed")
	}
	return &pb.OperationReply{Ok: true}, nil
}

// ListApplies returns pending or handled join requests for admins.
func (s *Server) doListApplies(ctx context.Context, args listAppliesArgs) (*pb.ListAppliesReply, error) {
	_, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	applies, err := s.repo.ListApplies(ctx, args.GroupID, int16(args.Status),
		int(args.Offset), int(args.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, "list applies failed")
	}
	reply := &pb.ListAppliesReply{Applies: make([]*pb.GroupApplyItem, 0, len(applies))}
	for _, apply := range applies {
		reply.Applies = append(reply.Applies, &pb.GroupApplyItem{
			Id: apply.ID, GroupId: apply.GroupID, UserId: apply.UserID, Why: apply.Why,
			Status: int32(apply.Status), CreatedAt: apply.CreatedAt, HandledAt: apply.HandledAt,
		})
	}
	return reply, nil
}

// SearchGroups finds groups by name.
func (s *Server) doSearchGroups(ctx context.Context, args searchGroupsArgs) (*pb.SearchGroupsReply, error) {
	keyword := strings.TrimSpace(args.Keyword)
	if keyword == "" {
		return nil, status.Error(codes.InvalidArgument, "empty keyword")
	}
	groups, err := s.repo.Search(ctx, keyword, int(args.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, "search groups failed")
	}
	reply := &pb.SearchGroupsReply{Groups: make([]*pb.GroupInfo, 0, len(groups))}
	for _, group := range groups {
		info, infoErr := s.groupInfo(ctx, group)
		if infoErr != nil {
			return nil, infoErr
		}
		reply.Groups = append(reply.Groups, info)
	}
	return reply, nil
}
