package user

import (
	"strconv"
	"strings"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Presence returns online state for the requested users.
func (a *API) Presence(ctx http.Context) error {
	if a.presence == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	parts := strings.Split(ctx.Query("uids"), ",")
	uids := make([]int64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		uid, err := strconv.ParseInt(part, 10, 64)
		if err != nil || uid <= 0 {
			continue
		}
		uids = append(uids, uid)
		if len(uids) >= 200 {
			break
		}
	}
	online, err := a.presence.Online(ctx.Context(), uids)
	if err != nil {
		return failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return success(ctx, map[string]any{"online": online})
}
