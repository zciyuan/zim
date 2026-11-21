package user

import (
	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Privacy returns the caller's privacy settings.
func (a *API) Privacy(ctx http.Context) error {
	if a.privacy == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	searchable, addFriend, err := a.privacy.Get(ctx.Context(), middleware.UID(ctx))
	if err != nil {
		return failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return success(ctx, map[string]any{"searchable": searchable, "add_friend": addFriend})
}

// UpdatePrivacy updates the caller's privacy settings.
func (a *API) UpdatePrivacy(ctx http.Context) error {
	if a.privacy == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	req := &privacyReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrInternal)
	}
	if err := a.privacy.Set(ctx.Context(), middleware.UID(ctx), req.Searchable, req.AddFriend); err != nil {
		return failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return success(ctx, map[string]any{"ok": true})
}

type privacyReq struct {
	Searchable bool `json:"searchable"`
	AddFriend  bool `json:"add_friend"`
}
