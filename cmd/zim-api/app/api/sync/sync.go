// Package sync exposes append-only version-log reads.
package sync

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the sync REST module.
func NewAPI(changes *changelogsvc.Service) *API {
	return &API{changes: changes}
}

// Changes returns version-log entries after since.
func (a *API) Changes(ctx http.Context) error {
	if a.changes == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	entity := ctx.Query("entity")
	switch entity {
	case model.EntityConversation, model.EntityFriend, model.EntityGroupMember:
	default:
		return api.Failure(ctx, errs.ErrInternal)
	}
	since, _ := strconv.ParseInt(ctx.Query("since"), 10, 64)
	limit, _ := strconv.Atoi(ctx.Query("limit"))
	entries, err := a.changes.List(ctx.Context(), middleware.UID(ctx), entity, since, limit)
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	items := make([]*changeRes, 0, len(entries))
	version := since
	for _, entry := range entries {
		items = append(items, &changeRes{
			ObjID: entry.ObjID, State: entry.State, Version: entry.Version, Mtime: entry.Mtime,
		})
		if entry.Version > version {
			version = entry.Version
		}
	}
	return api.Success(ctx, &changesRes{Entity: entity, Version: version, Changes: items})
}
