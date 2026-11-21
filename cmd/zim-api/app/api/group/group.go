// Package group exposes group and family REST endpoints.
package group

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	"github.com/zciyuan/ZIM/internal/service/group/client"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
	groupreadserver "github.com/zciyuan/ZIM/internal/service/groupread/server"
	groupversionserver "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the group REST module.
func NewAPI(proxy *http.Proxy, groupRead *groupreadserver.Service, families ...*familysvc.Service) *API {
	a := &API{proxy: proxy, groupRead: groupRead}
	if len(families) > 0 {
		a.family = families[0]
	}
	return a
}

// ReadCount returns how many members have read up to seq.
func (a *API) ReadCount(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 || a.groupRead == nil {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	seq, _ := strconv.ParseInt(ctx.Query("seq"), 10, 64)
	count, err := a.groupRead.Count(ctx.Context(), gid, seq)
	if err != nil {
		return failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return success(ctx, &readCountRes{ReadCount: count})
}

// MyGroups returns groups the caller belongs to.
func (a *API) MyGroups(ctx http.Context) error {
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.MyGroups(ctx.Context(), &pb.MyGroupsArgs{UserId: middleware.UID(ctx)})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &groupsRes{Groups: reply.GetGroups()})
}

// Get returns group info.
func (a *API) Get(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	info, err := groupClient.GetGroup(ctx.Context(), &pb.GetGroupArgs{GroupId: gid})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, info)
}

// Members returns a group member page.
func (a *API) Members(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	groupInfo, err := groupClient.GetGroup(ctx.Context(), &pb.GetGroupArgs{GroupId: gid})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	reply, err := groupClient.Members(ctx.Context(), &pb.MembersArgs{GroupId: gid, Limit: 200})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	if groupInfo.GetLookMemberInfo() == 0 {
		uid := middleware.UID(ctx)
		member := false
		for offset := int32(0); ; offset += 200 {
			page, pageErr := groupClient.Members(ctx.Context(), &pb.MembersArgs{
				GroupId: gid, Offset: offset, Limit: 200,
			})
			if pageErr != nil {
				return failure(ctx, mapError(pageErr))
			}
			for _, item := range page.GetMembers() {
				if item.GetUserId() == uid {
					member = true
					break
				}
			}
			if member || len(page.GetMembers()) < 200 {
				break
			}
		}
		if !member {
			return failure(ctx, errs.ErrNotGroupMember)
		}
	}
	return success(ctx, &membersRes{Members: reply.GetMembers()})
}

// Applies returns pending or handled join requests for group admins.
func (a *API) Applies(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	statusValue, _ := strconv.Atoi(ctx.Query("status"))
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.ListApplies(ctx.Context(), &pb.ListAppliesArgs{
		GroupId: gid, OperatorId: middleware.UID(ctx), Status: int32(statusValue), Limit: 100,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &appliesRes{Applies: reply.GetApplies()})
}

// CreateFamily creates a family group owned by the caller.
func (a *API) CreateFamily(ctx http.Context) error {
	req := &createFamilyReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.Name == "" {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	info, err := groupClient.CreateGroup(ctx.Context(), &pb.CreateGroupArgs{
		OwnerId: middleware.UID(ctx), Name: req.Name, GroupType: 3, MemberIds: req.MemberIDs,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, info)
}

// InviteFamilyMembers adds members to a family group.
func (a *API) InviteFamilyMembers(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	req := &inviteFamilyReq{}
	if err := ctx.Bind().JSON(req); err != nil || len(req.UserIDs) == 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.InviteMembers(ctx.Context(), &pb.InviteMembersArgs{
		GroupId: gid, OperatorId: middleware.UID(ctx), UserIds: req.UserIDs,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// DismissFamily dismisses a family group owned by the caller.
func (a *API) DismissFamily(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.DismissGroup(ctx.Context(), &pb.DismissGroupArgs{
		GroupId: gid, OperatorId: middleware.UID(ctx),
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// RemoveFamilyMember removes a family member.
func (a *API) RemoveFamilyMember(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.RemoveMember(ctx.Context(), &pb.RemoveMemberArgs{
		GroupId: gid, OperatorId: middleware.UID(ctx), UserId: uid,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

func mapError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errs.ErrGroupNotExist
	case codes.PermissionDenied:
		return errs.ErrNoGroupPermission
	case codes.ResourceExhausted:
		return errs.ErrGroupFull
	case codes.FailedPrecondition:
		return errs.ErrOwnerCannotQuit
	case codes.InvalidArgument:
		return errs.ErrGroupNotExist
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

// Versions returns group member/join versions.
func (a *API) Versions(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 || a.versions == nil {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	member, err := a.versions.Member(ctx.Context(), gid)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	join, err := a.versions.Join(ctx.Context(), gid)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"member_version": member, "join_version": join})
}

// SetGroupVersions enables group member/join version lookups.
func (a *API) SetGroupVersions(versions *groupversionserver.Service) {
	a.versions = versions
}

// Apply requests to join a group.
func (a *API) Apply(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	req := &applyReq{}
	if err = ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.ApplyJoin(ctx.Context(), &pb.ApplyJoinArgs{
		GroupId: gid, UserId: middleware.UID(ctx), Why: req.Why,
	})
	if err != nil {
		return api.Failure(ctx, mapError(err))
	}
	return api.Success(ctx, map[string]any{"ok": reply.GetOk()})
}

// Answer accepts or rejects a join request.
func (a *API) Answer(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	req := &answerApplyReq{}
	if err = ctx.Bind().JSON(req); err != nil || req.UserID <= 0 {
		return api.Failure(ctx, errs.ErrGroupNotExist)
	}
	groupClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := groupClient.AnswerJoin(ctx.Context(), &pb.AnswerJoinArgs{
		GroupId: gid, OperatorId: middleware.UID(ctx), UserId: req.UserID, Accept: req.Accept,
	})
	if err != nil {
		return api.Failure(ctx, mapError(err))
	}
	return api.Success(ctx, map[string]any{"ok": reply.GetOk()})
}
