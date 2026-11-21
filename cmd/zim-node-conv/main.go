// Command zim-node-conv serves conversation lists, read positions and unread
// counters.
package main

import (
	"context"
	"time"

	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-node-conv/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	"github.com/zciyuan/ZIM/internal/dao/conversation"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	version.HandleFlag()
	contract.InstallPacker()
	container := due.NewContainer()
	locator := redis.NewLocator()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	database := postgres.New()
	cache := rediscomp.New()
	mongodb := mongocomp.New()
	if mongodb.Database() == nil {
		log.Fatal("zim-node-conv requires etc.zim.mongo.uri")
	}
	if database.Pool() == nil {
		log.Fatal("zim-node-conv requires etc.zim.postgres.dsn")
	}
	svc := conversationsvc.New(conversation.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0))
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	indexCtx, indexCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := changeLog.EnsureIndexes(indexCtx, []string{model.EntityConversation}); err != nil {
		indexCancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	indexCancel()
	svc.SetChangeLog(changeLog)

	component := node.NewNode(
		node.WithLocator(locator),
		node.WithRegistry(registry),
		node.WithTransporter(transporter),
	)
	app.Init(component.Proxy(), svc, changeLog)
	container.Add(component, database, cache, mongodb, svc, health.New(define.ServiceNodeConv))
	container.Serve()
}
