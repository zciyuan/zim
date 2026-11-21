package relation

import (
	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
)

// API is the relation REST module.
type API struct {
	proxy *http.Proxy
}

func success(ctx http.Context, data any) error  { return api.Success(ctx, data) }
func failure(ctx http.Context, err error) error { return api.Failure(ctx, err) }

type blackReq struct {
	UID int64 `json:"uid"`
}

type remarkReq struct {
	Remark string `json:"remark"`
}

type pinReq struct {
	Pinned bool `json:"pinned"`
}
