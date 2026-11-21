// Package clientconfig exposes GET /v1/client-config.
package clientconfig

import (
	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	clientconfigserver "github.com/zciyuan/ZIM/internal/service/clientconfig/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// API serves client configuration.
type API struct{ service *clientconfigserver.Service }

// NewAPI returns the client-config API.
func NewAPI(service *clientconfigserver.Service) *API { return &API{service: service} }

// Get returns all keys prefixed with client.
func (a *API) Get(ctx http.Context) error {
	if a.service == nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	items, err := a.service.All(ctx.Context())
	if err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithCause(err))
	}
	return api.Success(ctx, map[string]any{"items": items})
}
