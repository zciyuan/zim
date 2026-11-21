// Package notification exposes the client announcement endpoint.
package notification

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	notificationserver "github.com/zciyuan/ZIM/internal/service/notification/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// API serves announcements.
type API struct{ service *notificationserver.Service }

// NewAPI returns the notification API.
func NewAPI(service *notificationserver.Service) *API { return &API{service: service} }

// List returns announcements newer than since.
func (a *API) List(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	since, _ := strconv.ParseInt(ctx.Query("since"), 10, 64)
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	items, err := a.service.List(ctx.Context(), since, limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}

// MarkRead marks an announcement as read by the caller.
func (a *API) MarkRead(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	id, err := strconv.ParseInt(ctx.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return api.Failure(ctx, errs.ErrInternal)
	}
	if err = a.service.MarkRead(ctx.Context(), middleware.UID(ctx), id); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// ReadIDs lists announcements read by the caller.
func (a *API) ReadIDs(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	ids, err := a.service.ReadIDs(ctx.Context(), middleware.UID(ctx))
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"ids": ids})
}
