// Package middleware contains REST middleware for zim-api.
package middleware

import (
	"crypto/subtle"
	"strings"
	"time"

	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

const uidLocal = "zim.uid"

// Auth verifies the Bearer access token and stores the uid in locals.
func Auth(issuer *token.Issuer) http.Handler {
	return func(ctx http.Context) error {
		// Public attachment reads: the object key carries a 128-bit random
		// value issued by /v1/upload/token (unguessable), and an <img> tag
		// cannot send an Authorization header. Everything else under /v1 keeps
		// requiring a Bearer token.
		if strings.HasPrefix(ctx.Path(), "/v1/files/") {
			return ctx.Next()
		}
		raw := strings.TrimSpace(ctx.Get("Authorization"))
		raw = strings.TrimPrefix(raw, "Bearer ")
		if raw == "" {
			_ = ctx.Status(401)
			return ctx.Failure(errs.ErrUnauthorized)
		}
		claims, err := issuer.Verify(raw, 0)
		if err != nil {
			_ = ctx.Status(401)
			return ctx.Failure(errs.ErrUnauthorized)
		}
		ctx.Locals(uidLocal, claims.UID)
		started := time.Now()
		err = ctx.Next()
		latency := time.Since(started).Milliseconds()
		result := "ok"
		if err != nil {
			result = "error"
		}
		log.Infof("api request uid=%d method=%s path=%s request_id=%s latency_ms=%d result=%s",
			claims.UID, ctx.Method(), ctx.Path(), ctx.Get("X-Request-Id"), latency, result)
		metrics.Default.Inc("zim_api_requests_total", map[string]string{"result": result})
		return err
	}
}

// Admin verifies the admin service token. An empty token rejects every request.
func Admin(token string, sources ...string) http.Handler {
	return func(ctx http.Context) error {
		got := strings.TrimSpace(ctx.Get("X-ZIM-Admin-Token"))
		if token == "" || got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			_ = ctx.Status(403)
			return ctx.Failure(errs.ErrUnauthorized)
		}
		if len(sources) > 0 && sources[0] != "" {
			if strings.TrimSpace(ctx.Get("X-ZIM-Admin-Source")) != sources[0] {
				_ = ctx.Status(403)
				return ctx.Failure(errs.ErrUnauthorized)
			}
		}
		return ctx.Next()
	}
}

// UID returns the authenticated user ID, or zero when absent.
func UID(ctx http.Context) int64 {
	value, ok := ctx.Locals(uidLocal).(int64)
	if !ok {
		return 0
	}
	return value
}

// Require checks the admin permission header. "*" is a super-admin wildcard.
func Require(permission string) http.Handler {
	return func(ctx http.Context) error {
		raw := strings.TrimSpace(ctx.Get("X-ZIM-Admin-Permissions"))
		if raw == "*" {
			return ctx.Next()
		}
		for _, item := range strings.Split(raw, ",") {
			if strings.TrimSpace(item) == permission {
				return ctx.Next()
			}
		}
		_ = ctx.Status(403)
		return ctx.Failure(errs.ErrUnauthorized)
	}
}
