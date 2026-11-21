// Command zim-mesh-relation serves friend and blacklist operations.
package main

import (
	"context"
	"time"

	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-mesh-relation/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	"github.com/zciyuan/ZIM/internal/dao/friend"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
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
		log.Fatal("zim-mesh-relation requires etc.zim.postgres.dsn")
	}
	mongodb := mongocomp.New()
	if mongodb.Database() == nil {
		log.Fatal("zim-mesh-relation requires etc.zim.mongo.uri")
	}
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	indexCtx, indexCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := changeLog.EnsureIndexes(indexCtx, []string{model.EntityFriend}); err != nil {
		indexCancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	indexCancel()

	component := mesh.NewMesh(
		mesh.WithRegistry(registry),
		mesh.WithTransporter(transporter),
	)
	app.Init(component.Proxy(), friend.New(database.Pool()), changeLog, webhook.New(etc.Get("etc.zim.webhook.url", "").String(), etc.Get("etc.zim.webhook.secret", "").String()), cache.Client())
	container.Add(component, database, mongodb, cache, health.New(define.ServiceMeshRelation))
	container.Serve()
}
