// Package api contains shared REST response helpers.
package api

import (
	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/v2/utils/xuuid"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Success writes {code:0,data,request_id}.
func Success(ctx http.Context, data any) error {
	return ctx.JSON(&Response{Code: 0, Data: data, RequestID: requestID(ctx)})
}

// Failure writes a business error with its HTTP status and request_id.
func Failure(ctx http.Context, err error) error {
	business := errs.From(err)
	_ = ctx.Status(business.HTTPStatus())
	return ctx.JSON(&Response{
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
