// Package relation exposes friend and blacklist REST endpoints.
package relation

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	groupPb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/internal/service/relation/client"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	usersvc "github.com/zciyuan/ZIM/internal/service/user/client"
	userpb "github.com/zciyuan/ZIM/internal/service/user/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the relation REST module.
func NewAPI(proxy *http.Proxy) *API {
	return &API{proxy: proxy}
}

// Friends returns the caller's friends with profiles.
func (a *API) Friends(ctx http.Context) error {
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.ListFriends(ctx.Context(), &pb.ListFriendsArgs{Uid: middleware.UID(ctx)})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	uids := make([]int64, 0, len(reply.GetFriends()))
	for _, item := range reply.GetFriends() {
		uids = append(uids, item.GetFriendId())
	}
	profiles, err := a.profiles(ctx, uids)
	if err != nil {
		return failure(ctx, err)
	}
	friends := make([]*friendRes, 0, len(reply.GetFriends()))
	for _, item := range reply.GetFriends() {
		friends = append(friends, &friendRes{
			FriendID: item.GetFriendId(), Remark: item.GetRemark(), Pinned: item.GetPinned(),
			CreatedAt: item.GetCreatedAt(), Profile: profiles[item.GetFriendId()],
		})
	}
	return success(ctx, &friendsRes{Friends: friends})
}

// Requests returns inbound or outbound friend requests.
func (a *API) Requests(ctx http.Context) error {
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	inbound := ctx.Query("type") != "out"
	reply, err := relationClient.ListFriendRequests(ctx.Context(), &pb.ListFriendRequestsArgs{
		Uid: middleware.UID(ctx), Inbound: inbound,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &requestsRes{Requests: reply.GetRequests()})
}

// Blacklist returns blocked users.
func (a *API) Blacklist(ctx http.Context) error {
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.ListBlacklist(ctx.Context(), &pb.ListBlacklistArgs{Uid: middleware.UID(ctx)})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	profiles, err := a.profiles(ctx, reply.GetUids())
	if err != nil {
		return failure(ctx, err)
	}
	users := make([]*userpb.UserProfile, 0, len(reply.GetUids()))
	for _, uid := range reply.GetUids() {
		if profile := profiles[uid]; profile != nil {
			users = append(users, profile)
		}
	}
	return success(ctx, &blacklistRes{Users: users})
}

// AddFriend creates a friend request.
func (a *API) AddFriend(ctx http.Context) error {
	req := &addFriendReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.FriendID <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	if req.GroupID > 0 {
		groupClient, groupErr := groupclient.NewClient(a.proxy.NewMeshClient)
		if groupErr == nil {
			groupInfo, infoErr := groupClient.GetGroup(ctx.Context(), &groupPb.GetGroupArgs{GroupId: req.GroupID})
			if infoErr == nil && groupInfo.GetApplyMemberFriend() == 0 {
				return failure(ctx, errs.ErrNoGroupPermission)
			}
		}
	}
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.SendFriendRequest(ctx.Context(), &pb.SendFriendRequestArgs{
		FromId: middleware.UID(ctx), ToId: req.FriendID, Why: req.Why,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// AnswerFriend accepts or rejects a request from friendID.
func (a *API) AnswerFriend(ctx http.Context) error {
	req := &answerFriendReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.FriendID <= 0 {
		return failure(ctx, errs.ErrFriendRequestNotExist)
	}
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.AnswerFriend(ctx.Context(), &pb.AnswerFriendArgs{
		FromId: req.FriendID, ToId: middleware.UID(ctx), Accept: req.Accept,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// DeleteFriend removes a friend.
func (a *API) DeleteFriend(ctx http.Context) error {
	friendID, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || friendID <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.DeleteFriend(ctx.Context(), &pb.DeleteFriendArgs{
		Uid: middleware.UID(ctx), FriendId: friendID,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

func (a *API) profiles(ctx http.Context, uids []int64) (map[int64]*userpb.UserProfile, error) {
	profiles := make(map[int64]*userpb.UserProfile, len(uids))
	if len(uids) == 0 {
		return profiles, nil
	}
	userClient, err := usersvc.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return nil, errs.ErrServerBusy
	}
	reply, err := userClient.BatchGetUsers(ctx.Context(), &userpb.BatchGetUsersArgs{Uids: uids})
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	for _, profile := range reply.GetUsers() {
		profiles[profile.GetUid()] = profile
	}
	return profiles, nil
}

func mapError(err error) error {
	switch status.Code(err) {
	case codes.AlreadyExists:
		return errs.ErrAlreadyFriend
	case codes.PermissionDenied:
		return errs.ErrBlockedByPeer
	case codes.NotFound:
		return errs.ErrFriendRequestNotExist
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

// AddBlack implements POST /v1/blacklist.
func (a *API) AddBlack(ctx http.Context) error {
	req := &blackReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.UID <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	return a.setBlack(ctx, req.UID, false)
}

// RemoveBlack implements DELETE /v1/blacklist/{uid}.
func (a *API) RemoveBlack(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	return a.setBlack(ctx, uid, true)
}

func (a *API) setBlack(ctx http.Context, blackID int64, remove bool) error {
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.AddBlack(ctx.Context(), &pb.AddBlackArgs{
		Uid: middleware.UID(ctx), BlackId: blackID, Remove: remove,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// UpdateRemark implements PUT /v1/friends/{uid}/remark.
func (a *API) UpdateRemark(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	req := &remarkReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrNicknameInvalid)
	}
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.UpdateRemark(ctx.Context(), &pb.UpdateRemarkArgs{
		Uid: middleware.UID(ctx), FriendId: uid, Remark: req.Remark,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}

// PinFriend implements POST /v1/friends/{uid}/pin.
func (a *API) PinFriend(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	req := &pinReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrFriendRequestNotExist)
	}
	relationClient, err := client.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := relationClient.PinFriend(ctx.Context(), &pb.PinFriendArgs{
		Uid: middleware.UID(ctx), FriendId: uid, Pinned: req.Pinned,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &operationRes{OK: reply.GetOk()})
}
