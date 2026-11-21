// Package user exposes user profile REST endpoints.
package user

import (
	"strconv"
	"strings"

	"github.com/dobyte/due/component/http/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	presenceserver "github.com/zciyuan/ZIM/internal/service/presence/server"
	privacyserver "github.com/zciyuan/ZIM/internal/service/privacy/server"
	"github.com/zciyuan/ZIM/internal/service/user/client"
	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// API is the user REST module.
type API struct {
	proxy    *http.Proxy
	privacy  *privacyserver.Service
	presence *presenceserver.Service
}

// NewAPI returns the user REST module.
func NewAPI(proxy *http.Proxy) *API {
	return &API{proxy: proxy}
}

// Me returns the authenticated user's profile.
func (a *API) Me(ctx http.Context) error {
	return a.get(ctx, middleware.UID(ctx))
}

// Get returns another user's profile.
func (a *API) Get(ctx http.Context) error {
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrUserNotExist)
	}
	return a.get(ctx, uid)
}

// Batch returns users by comma-separated uid list.
func (a *API) Batch(ctx http.Context) error {
	raw := strings.TrimSpace(ctx.Query("uids"))
	if raw == "" {
		return success(ctx, &batchRes{Users: []*pb.UserProfile{}})
	}
	parts := strings.Split(raw, ",")
	uids := make([]int64, 0, len(parts))
	for _, part := range parts {
		uid, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil && uid > 0 {
			uids = append(uids, uid)
		}
	}
	client, err := a.client()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.BatchGetUsers(ctx.Context(), &pb.BatchGetUsersArgs{Uids: uids})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &batchRes{Users: reply.GetUsers()})
}

// Search finds users by username or nickname.
func (a *API) Search(ctx http.Context) error {
	keyword := strings.TrimSpace(ctx.Query("q"))
	if keyword == "" {
		return success(ctx, &batchRes{Users: []*pb.UserProfile{}})
	}
	client, err := a.client()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.SearchUsers(ctx.Context(), &pb.SearchUsersArgs{Keyword: keyword, Limit: 20})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, &batchRes{Users: reply.GetUsers()})
}

// UpdateMe updates the authenticated user's profile.
func (a *API) UpdateMe(ctx http.Context) error {
	req := &updateReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrNicknameInvalid)
	}
	client, err := a.client()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	profile, err := client.UpdateProfile(ctx.Context(), &pb.UpdateProfileArgs{
		Uid: middleware.UID(ctx), Nickname: req.Nickname, Avatar: req.Avatar, Gender: req.Gender,
	})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, profile)
}

func (a *API) get(ctx http.Context, uid int64) error {
	if uid <= 0 {
		return failure(ctx, errs.ErrUnauthorized)
	}
	client, err := a.client()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	profile, err := client.GetProfile(ctx.Context(), &pb.GetProfileArgs{Uid: uid})
	if err != nil {
		return failure(ctx, mapError(err))
	}
	return success(ctx, profile)
}

func (a *API) client() (pb.UserClient, error) {
	return client.NewClient(a.proxy.NewMeshClient)
}

func mapError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errs.ErrUserNotExist
	case codes.InvalidArgument:
		return errs.ErrNicknameInvalid
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

func success(ctx http.Context, data any) error  { return api.Success(ctx, data) }
func failure(ctx http.Context, err error) error { return api.Failure(ctx, err) }

// SetPresence enables online presence lookup.
func (a *API) SetPresence(service *presenceserver.Service) { a.presence = service }

// SetPrivacy enables privacy settings.
func (a *API) SetPrivacy(service *privacyserver.Service) { a.privacy = service }
