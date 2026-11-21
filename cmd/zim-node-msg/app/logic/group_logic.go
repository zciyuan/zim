package logic

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	groupwire "github.com/zciyuan/ZIM/internal/protocol/group"
	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

func (c *core) createGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCreateGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.CreateGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpCreateGroup, nil, errs.ErrInternal)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpCreateGroup, nil, errs.ErrServerBusy)
			return
		}
		info, err := client.CreateGroup(ctx.Context(), &grouppb.CreateGroupArgs{
			OwnerId: uid, Name: req.GetName(), FaceUrl: req.GetFaceUrl(),
			GroupType: req.GetGroupType(), MemberIds: req.GetMemberIds(),
			NeedVerification: req.GetNeedVerification(),
		})
		if err != nil {
			c.respond(ctx, contract.OpCreateGroup, nil, groupError(err))
			return
		}
		payload, _ := proto.Marshal(&groupwire.CreateGroupRes{
			GroupId: strconv.FormatInt(info.GetGroupId(), 10), Name: info.GetName(),
			FaceUrl: info.GetFaceUrl(), OwnerId: strconv.FormatInt(info.GetOwnerId(), 10),
			GroupType: info.GetGroupType(), MaxMember: info.GetMaxMember(),
			MemberCount: info.GetMemberCount(),
		})
		c.respond(ctx, contract.OpCreateGroup, payload, nil)
	})
}

func (c *core) groupMembers(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &groupwire.GroupMembersReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpGroupMembers, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpGroupMembers, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpGroupMembers, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.Members(ctx.Context(), &grouppb.MembersArgs{
			GroupId: gid, Offset: req.GetOffset(), Limit: req.GetLimit(),
		})
		if err != nil {
			c.respond(ctx, contract.OpGroupMembers, nil, groupError(err))
			return
		}
		res := &groupwire.GroupMembersRes{Members: make([]*groupwire.GroupMember, 0, len(reply.GetMembers()))}
		for _, member := range reply.GetMembers() {
			res.Members = append(res.Members, &groupwire.GroupMember{
				Uid: strconv.FormatInt(member.GetUserId(), 10), Role: member.GetRole(),
				MuteUntil: member.GetMuteUntil(), JoinedAt: member.GetJoinedAt(),
			})
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpGroupMembers, payload, nil)
	})
}

func (c *core) inviteGroupMembers(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpInviteGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.InviteGroupMembersReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpInviteGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpInviteGroup, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpInviteGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = client.InviteMembers(ctx.Context(), &grouppb.InviteMembersArgs{
			GroupId: gid, OperatorId: uid, UserIds: req.GetUserIds(),
		})
		if err != nil {
			c.respond(ctx, contract.OpInviteGroup, nil, groupError(err))
			return
		}
		for _, member := range req.GetUserIds() {
			c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
				GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_MEMBER_JOINED),
				ActorId: uid, TargetId: member,
			})
		}
		c.groupOperation(ctx, contract.OpInviteGroup)
	})
}

func (c *core) quitGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpQuitGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.QuitGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpQuitGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpQuitGroup, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpQuitGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = client.QuitGroup(ctx.Context(), &grouppb.QuitGroupArgs{GroupId: gid, UserId: uid})
		if err != nil {
			c.respond(ctx, contract.OpQuitGroup, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_MEMBER_LEFT),
			ActorId: uid, TargetId: uid,
		})
		c.groupOperation(ctx, contract.OpQuitGroup)
	})
}

func (c *core) kickGroupMember(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpKickGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.KickGroupMemberReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpKickGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpKickGroup, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpKickGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = client.RemoveMember(ctx.Context(), &grouppb.RemoveMemberArgs{
			GroupId: gid, OperatorId: uid, UserId: req.GetUserId(),
		})
		if err != nil {
			c.respond(ctx, contract.OpKickGroup, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_MEMBER_KICKED),
			ActorId: uid, TargetId: req.GetUserId(),
		})
		c.groupOperation(ctx, contract.OpKickGroup)
	})
}

func (c *core) dismissGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpDismissGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.DismissGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpDismissGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpDismissGroup, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpDismissGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = client.DismissGroup(ctx.Context(), &grouppb.DismissGroupArgs{
			GroupId: gid, OperatorId: uid,
		})
		if err != nil {
			c.respond(ctx, contract.OpDismissGroup, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_DISMISSED), ActorId: uid,
		})
		c.groupOperation(ctx, contract.OpDismissGroup)
	})
}

func (c *core) groupOperation(ctx node.Context, opcode int32) {
	payload, _ := proto.Marshal(&groupwire.GroupOperationRes{Ok: true})
	c.respond(ctx, opcode, payload, nil)
}

func groupError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errs.ErrGroupNotExist
	case codes.PermissionDenied:
		return errs.ErrNoGroupPermission
	case codes.ResourceExhausted:
		return errs.ErrGroupFull
	case codes.FailedPrecondition:
		return errs.ErrOwnerCannotQuit
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

var _ = context.Background

func (c *core) updateGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpUpdateGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.UpdateGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpUpdateGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpUpdateGroup, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpUpdateGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		info, err := groupClient.UpdateGroup(ctx.Context(), &grouppb.UpdateGroupArgs{
			GroupId: gid, OperatorId: uid, Name: req.GetName(), FaceUrl: req.GetFaceUrl(),
			Notification: req.GetNotification(), NeedVerification: req.GetNeedVerification(),
			LookMemberInfo: req.GetLookMemberInfo(), ApplyMemberFriend: req.GetApplyMemberFriend(),
		})
		if err != nil {
			c.respond(ctx, contract.OpUpdateGroup, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_UPDATED),
			ActorId: uid, Name: info.GetName(),
		})
		payload, _ := proto.Marshal(&groupwire.UpdateGroupRes{
			GroupId: strconv.FormatInt(info.GetGroupId(), 10), Name: info.GetName(),
			FaceUrl: info.GetFaceUrl(), Notification: info.GetNotification(),
			NeedVerification: info.GetNeedVerification(), LookMemberInfo: info.GetLookMemberInfo(),
			ApplyMemberFriend: info.GetApplyMemberFriend(),
		})
		c.respond(ctx, contract.OpUpdateGroup, payload, nil)
	})
}

func (c *core) transferGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpTransferGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.TransferGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpTransferGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpTransferGroup, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpTransferGroup, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = groupClient.TransferGroup(ctx.Context(), &grouppb.TransferGroupArgs{
			GroupId: gid, OperatorId: uid, NewOwnerId: req.GetNewOwnerId(),
		})
		if err != nil {
			c.respond(ctx, contract.OpTransferGroup, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_OWNER_TRANSFERRED),
			ActorId: uid, TargetId: req.GetNewOwnerId(),
		})
		c.groupOperation(ctx, contract.OpTransferGroup)
	})
}

func (c *core) applyJoinGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpApplyJoinGroup, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.ApplyJoinReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpApplyJoinGroup, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpApplyJoinGroup, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpApplyJoinGroup, nil, errs.ErrServerBusy)
			return
		}
		_, err = groupClient.ApplyJoin(ctx.Context(), &grouppb.ApplyJoinArgs{
			GroupId: gid, UserId: uid, Why: req.GetWhy(),
		})
		if err != nil {
			c.respond(ctx, contract.OpApplyJoinGroup, nil, groupError(err))
			return
		}
		c.groupOperation(ctx, contract.OpApplyJoinGroup)
	})
}

func (c *core) answerJoinGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpAnswerJoin, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.AnswerJoinReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpAnswerJoin, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 || req.GetUserId() <= 0 {
			c.respond(ctx, contract.OpAnswerJoin, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpAnswerJoin, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = groupClient.AnswerJoin(ctx.Context(), &grouppb.AnswerJoinArgs{
			GroupId: gid, OperatorId: uid, UserId: req.GetUserId(), Accept: req.GetAccept(),
		})
		if err != nil {
			c.respond(ctx, contract.OpAnswerJoin, nil, groupError(err))
			return
		}
		if req.GetAccept() {
			c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
				GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_MEMBER_JOINED),
				ActorId: uid, TargetId: req.GetUserId(),
			})
		}
		c.groupOperation(ctx, contract.OpAnswerJoin)
	})
}

func (c *core) setGroupAdmin(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSetGroupAdmin, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.SetGroupAdminReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSetGroupAdmin, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 || req.GetUserId() <= 0 {
			c.respond(ctx, contract.OpSetGroupAdmin, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpSetGroupAdmin, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = groupClient.SetAdmin(ctx.Context(), &grouppb.SetAdminArgs{
			GroupId: gid, OperatorId: uid, UserId: req.GetUserId(), Admin: req.GetAdmin(),
		})
		if err != nil {
			c.respond(ctx, contract.OpSetGroupAdmin, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_ADMIN_CHANGED),
			ActorId: uid, TargetId: req.GetUserId(),
		})
		c.groupOperation(ctx, contract.OpSetGroupAdmin)
	})
}

func (c *core) muteGroupMember(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpMuteMember, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.MuteMemberReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpMuteMember, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 || req.GetUserId() <= 0 {
			c.respond(ctx, contract.OpMuteMember, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpMuteMember, nil, errs.ErrServerBusy)
			return
		}
		_, err = groupClient.MuteMember(ctx.Context(), &grouppb.MuteMemberArgs{
			GroupId: gid, OperatorId: uid, UserId: req.GetUserId(), Until: req.GetUntil(),
		})
		if err != nil {
			c.respond(ctx, contract.OpMuteMember, nil, groupError(err))
			return
		}
		c.groupOperation(ctx, contract.OpMuteMember)
	})
}

func (c *core) muteAllGroup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpMuteAll, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.MuteAllReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpMuteAll, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpMuteAll, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpMuteAll, nil, errs.ErrServerBusy)
			return
		}
		targets := c.groupTargets(ctx.Context(), gid, uid)
		_, err = groupClient.MuteAll(ctx.Context(), &grouppb.MuteAllArgs{
			GroupId: gid, OperatorId: uid, Until: req.GetUntil(),
		})
		if err != nil {
			c.respond(ctx, contract.OpMuteAll, nil, groupError(err))
			return
		}
		c.pushGroupEvent(ctx.Context(), targets, &groupwire.GroupEvent{
			GroupId: gid, Kind: int32(groupwire.GroupEventKind_GROUP_EVENT_MUTED),
			ActorId: uid, At: req.GetUntil(),
		})
		c.groupOperation(ctx, contract.OpMuteAll)
	})
}

func (c *core) myGroups(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpMyGroups, nil, errs.ErrUnauthorized)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpMyGroups, nil, errs.ErrServerBusy)
			return
		}
		reply, err := groupClient.MyGroups(ctx.Context(), &grouppb.MyGroupsArgs{UserId: uid})
		if err != nil {
			c.respond(ctx, contract.OpMyGroups, nil, groupError(err))
			return
		}
		res := &groupwire.MyGroupsRes{GroupIds: make([]string, 0, len(reply.GetGroups()))}
		for _, group := range reply.GetGroups() {
			res.GroupIds = append(res.GroupIds, strconv.FormatInt(group.GetGroupId(), 10))
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpMyGroups, payload, nil)
	})
}

func (c *core) searchGroups(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &groupwire.SearchGroupReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSearchGroup, nil, errs.ErrInternal)
			return
		}
		if strings.TrimSpace(req.GetKeyword()) == "" {
			c.respond(ctx, contract.OpSearchGroup, nil, errs.ErrGroupNotExist)
			return
		}
		groupClient, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpSearchGroup, nil, errs.ErrServerBusy)
			return
		}
		reply, err := groupClient.SearchGroups(ctx.Context(), &grouppb.SearchGroupsArgs{
			Keyword: req.GetKeyword(), Limit: req.GetLimit(),
		})
		if err != nil {
			c.respond(ctx, contract.OpSearchGroup, nil, groupError(err))
			return
		}
		res := &groupwire.SearchGroupRes{Groups: make([]*groupwire.GroupSummary, 0, len(reply.GetGroups()))}
		for _, group := range reply.GetGroups() {
			res.Groups = append(res.Groups, &groupwire.GroupSummary{
				GroupId: strconv.FormatInt(group.GetGroupId(), 10), Name: group.GetName(),
				FaceUrl: group.GetFaceUrl(), OwnerId: strconv.FormatInt(group.GetOwnerId(), 10),
				GroupType: group.GetGroupType(), MemberCount: group.GetMemberCount(),
			})
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpSearchGroup, payload, nil)
	})
}

func (c *core) groupNotify(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpGroupNotify, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.GroupNotifyReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpGroupNotify, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpGroupNotify, nil, errs.ErrGroupNotExist)
			return
		}
		if err = c.conv.SetGroupNotify(ctx.Context(), uid, gid, int16(req.GetRecvOpt())); err != nil {
			c.respond(ctx, contract.OpGroupNotify, nil, err)
			return
		}
		c.groupOperation(ctx, contract.OpGroupNotify)
	})
}

// pushGroupEvent fans one group change out to every target user.
//
// The targets are the group members captured before the mutation, so a leaving
// or kicked member still receives the event that closes their view.
func (c *core) pushGroupEvent(ctx context.Context, targets []int64, event *groupwire.GroupEvent) {
	if len(targets) == 0 {
		return
	}
	if event.At == 0 {
		event.At = time.Now().UnixMilli()
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return
	}
	for _, uid := range targets {
		if uid <= 0 {
			continue
		}
		// Real device keys carry a random slot; addressing slot 1 silently
		// misses every device whose slot differs (most of them).
		c.pushDeviceKeys(ctx, c.deviceKeys(ctx, uid), contract.OpPushGroupEvent, payload, false)
	}
}

// groupTargets returns the members used as event targets (best effort).
func (c *core) groupTargets(ctx context.Context, gid, uid int64) []int64 {
	members, err := c.msg.GroupMembers(ctx, gid, uid)
	if err != nil {
		log.Warnf("group event targets failed: group=%d uid=%d err=%v", gid, uid, err)
		return nil
	}
	return members
}
