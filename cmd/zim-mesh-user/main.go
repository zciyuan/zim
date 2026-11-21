// Command zim-mesh-user serves user profile queries.
package main

import (
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-mesh-user/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	"github.com/zciyuan/ZIM/internal/dao/user"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/version"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	database := postgres.New()
	cache := rediscomp.New()
	if database.Pool() == nil {
		log.Fatal("zim-mesh-user requires etc.zim.postgres.dsn")
	}
	component := mesh.NewMesh(
		mesh.WithRegistry(registry),
		mesh.WithTransporter(transporter),
	)
	app.Init(component.Proxy(), user.New(database.Pool()), cache.Client())
	container.Add(component, database, cache, health.New(define.ServiceMeshUser))
	container.Serve()
}
