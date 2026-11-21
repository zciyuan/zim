package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"

	account "github.com/zciyuan/ZIM/internal/service/account/server"
	authsvc "github.com/zciyuan/ZIM/internal/service/auth/server"
)

// Init registers all auth mesh service providers.
func Init(proxy *mesh.Proxy, accountService *account.Service) {
	server := authsvc.NewServer(proxy)
	server.SetAccount(accountService)
	server.Init()
}
