// Package app assembles the relation mesh application.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	changelog "github.com/zciyuan/ZIM/internal/service/changelog/server"
	relationsvc "github.com/zciyuan/ZIM/internal/service/relation/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
)

// Init registers the relation mesh service provider.
func Init(proxy *mesh.Proxy, repo relationsvc.Repository, changes *changelog.Service, notifier webhook.Notifier, cache redis.UniversalClient) {
	server := relationsvc.NewServer(proxy, repo)
	server.SetCache(cache)
	server.SetChangeLog(changes)
	server.Init()
}
