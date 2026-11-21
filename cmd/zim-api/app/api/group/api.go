package group

import (
	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	groupversionsvc "github.com/zciyuan/ZIM/internal/service/groupversion/server"
)

// API is the group REST module.
type API struct {
	proxy     *http.Proxy
	groupRead *groupreadsvc.Service
	family    *familysvc.Service
	versions  *groupversionsvc.Service
}

func success(ctx http.Context, data any) error  { return api.Success(ctx, data) }
func failure(ctx http.Context, err error) error { return api.Failure(ctx, err) }

type applyReq struct {
	Why string `json:"why"`
}

type answerApplyReq struct {
	UserID int64 `json:"user_id"`
	Accept bool  `json:"accept"`
}
