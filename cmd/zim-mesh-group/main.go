// Command zim-mesh-group serves group and family operations.
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

	"github.com/zciyuan/ZIM/cmd/zim-mesh-group/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	convdao "github.com/zciyuan/ZIM/internal/dao/conversation"
	groupdao "github.com/zciyuan/ZIM/internal/dao/group"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	groupserver "github.com/zciyuan/ZIM/internal/service/group/server"
	groupversion "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/version"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	database := postgres.New()
	if database.Pool() == nil {
		log.Fatal("zim-mesh-group requires etc.zim.postgres.dsn")
	}
	mongodb := mongocomp.New()
	if mongodb.Database() == nil {
		log.Fatal("zim-mesh-group requires etc.zim.mongo.uri")
	}
	cache := rediscomp.New()
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	indexCtx, indexCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := changeLog.EnsureIndexes(indexCtx, []string{model.EntityGroupMember}); err != nil {
		indexCancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	indexCancel()

	component := mesh.NewMesh(
		mesh.WithRegistry(registry),
		mesh.WithTransporter(transporter),
	)
	conversationService := conversationsvc.New(convdao.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0))
	app.Init(component.Proxy(), groupdao.New(database.Pool()), changeLog, conversationService, groupversion.New(cache.Client()), groupserver.NewMemberCache(cache.Client()), webhook.New(etc.Get("etc.zim.webhook.url", "").String(), etc.Get("etc.zim.webhook.secret", "").String()))
	container.Add(component, database, mongodb, cache, conversationService, health.New(define.ServiceMeshGroup))
	container.Serve()
}
