package group

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// UpdateFamilySettings updates the family-level auto-answer switch.
func (a *API) UpdateFamilySettings(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	if a.family == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	req := &familySettingsReq{}
	if err = ctx.Bind().JSON(req); err != nil {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	if err = a.family.UpdateSettings(ctx.Context(), middleware.UID(ctx), gid, req.AutoAnswerEnabled, req.FamilyMode); err != nil {
		return failure(ctx, familyError(err))
	}
	return success(ctx, &operationRes{OK: true})
}

// GrantFamilyConsent records a child/elder auto-answer consent.
func (a *API) GrantFamilyConsent(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	uid, err := strconv.ParseInt(ctx.Params("uid"), 10, 64)
	if err != nil || uid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	if a.family == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	req := &familyConsentReq{}
	if err = ctx.Bind().JSON(req); err != nil || req.Scope != "" && req.Scope != "auto_answer" {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	if err = a.family.GrantConsent(ctx.Context(), middleware.UID(ctx), gid, uid, req.Granted, req.GuardianUID, req.Channel); err != nil {
		return failure(ctx, familyError(err))
	}
	return success(ctx, &operationRes{OK: true})
}

// FamilyAudit returns recent family policy audit records.
func (a *API) FamilyAudit(ctx http.Context) error {
	gid, err := strconv.ParseInt(ctx.Params("gid"), 10, 64)
	if err != nil || gid <= 0 {
		return failure(ctx, errs.ErrGroupNotExist)
	}
	if a.family == nil {
		return failure(ctx, errs.ErrServerBusy)
	}
	items, err := a.family.ListAudit(ctx.Context(), gid, 50)
	if err != nil {
		return failure(ctx, familyError(err))
	}
	return success(ctx, &familyAuditRes{Items: items})
}

func familyError(err error) error {
	switch err {
	case familysvc.ErrNotFamily:
		return errs.ErrGroupNotExist
	case familysvc.ErrNotAllowed, familysvc.ErrNotEligible:
		return errs.ErrNotGroupMember
	default:
		return errs.ErrInternal.WithCause(err)
	}
}
