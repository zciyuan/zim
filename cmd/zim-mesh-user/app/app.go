// Package app assembles the user mesh application.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	usersvc "github.com/zciyuan/ZIM/internal/service/user/server"
)

// Init registers the user mesh service provider.
func Init(proxy *mesh.Proxy, repo usersvc.Repository, cache redis.UniversalClient) {
	server := usersvc.NewServer(proxy, repo)
	server.SetCache(cache)
	server.Init()
}
