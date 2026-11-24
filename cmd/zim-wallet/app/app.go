// Package app assembles the wallet mesh application.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"

	walletsvc "github.com/zciyuan/ZIM/internal/service/wallet/server"
)

// Init registers the wallet service provider under the discovery name wallet.
func Init(proxy *mesh.Proxy, service *walletsvc.Service) {
	service.Init()
}
