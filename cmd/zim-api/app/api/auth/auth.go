// Package auth exposes the client authentication REST endpoints.
package auth

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/transport"
	"github.com/dobyte/due/v2/utils/xuuid"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authsvc "github.com/zciyuan/ZIM/internal/service/auth/client"
	authpb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// API is the auth REST module.
type API struct {
	proxy         *http.Proxy
	clientFactory func(transport.NewMeshClient) (authpb.AuthClient, error)
}

// NewAPI returns the auth REST module.
func NewAPI(proxy *http.Proxy) *API {
	return &API{proxy: proxy, clientFactory: func(fn transport.NewMeshClient) (authpb.AuthClient, error) {
		if fn == nil {
			return nil, errs.ErrServerBusy
		}
		return authsvc.NewClient(fn)
	}}
}

// Token implements POST /v1/auth/token.
func (a *API) Token(ctx http.Context) error {
	req := &tokenReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrPasswordIncorrect)
	}
	client, err := a.newClient()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	pair, err := client.Login(ctx.Context(), &authpb.LoginArgs{
		Username: req.Username, Password: req.Password, Platform: req.Platform,
	})
	if err != nil {
		return failure(ctx, authError(err))
	}
	return ctx.JSON(&response{Code: 0, Data: tokenResOf(pair), RequestID: requestID(ctx)})
}

// Register implements POST /v1/auth/register.
func (a *API) Register(ctx http.Context) error {
	req := &registerReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrPasswordInvalid)
	}
	client, err := a.newClient()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	pair, err := client.Register(ctx.Context(), &authpb.RegisterArgs{
		Username: req.Username, Password: req.Password, Nickname: req.Nickname, Platform: req.Platform,
	})
	if err != nil {
		return failure(ctx, authError(err))
	}
	return ctx.JSON(&response{Code: 0, Data: tokenResOf(pair), RequestID: requestID(ctx)})
}

// Refresh implements POST /v1/auth/refresh.
func (a *API) Refresh(ctx http.Context) error {
	req := &refreshReq{}
	if err := ctx.Bind().JSON(req); err != nil || req.RefreshToken == "" {
		return failure(ctx, errs.ErrUnauthorized)
	}
	client, err := a.newClient()
	if err != nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	pair, err := client.Refresh(ctx.Context(), &authpb.RefreshArgs{RefreshToken: req.RefreshToken})
	if err != nil {
		return failure(ctx, errs.ErrUnauthorized.WithCause(err))
	}
	return ctx.JSON(&response{Code: 0, Data: tokenResOf(pair), RequestID: requestID(ctx)})
}

func (a *API) newClient() (authpb.AuthClient, error) {
	if a.clientFactory == nil {
		return nil, errs.ErrServerBusy
	}
	var fn transport.NewMeshClient
	if a.proxy != nil {
		fn = a.proxy.NewMeshClient
	}
	return a.clientFactory(fn)
}

func tokenResOf(pair *authpb.TokenPair) *tokenRes {
	return &tokenRes{
		UID: strconv.FormatInt(pair.GetUid(), 10), AccessToken: pair.GetAccessToken(),
		RefreshToken: pair.GetRefreshToken(), ExpiresIn: pair.GetExpiresIn(),
		RefreshExpiresIn: pair.GetRefreshExpiresIn(),
	}
}

func authError(err error) error {
	switch status.Code(err) {
	case grpccodes.AlreadyExists:
		return errs.ErrUsernameTaken
	case grpccodes.PermissionDenied:
		return errs.ErrAccountDisabled
	case grpccodes.Unauthenticated:
		return errs.ErrPasswordIncorrect
	default:
		return errs.ErrServerBusy
	}
}

func failure(ctx http.Context, err error) error {
	business := errs.From(err)
	_ = ctx.Status(business.HTTPStatus())
	return ctx.JSON(&response{
		Code:      business.Code().Code(),
		Message:   business.Message(),
		RequestID: requestID(ctx),
	})
}

func requestID(ctx http.Context) string {
	if id := ctx.Get("X-Request-Id"); id != "" {
		return id
	}
	return "req_" + xuuid.UUID()
}
