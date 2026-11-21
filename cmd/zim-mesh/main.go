// Command zim-mesh hosts all mesh service providers in one process.
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

	"github.com/zciyuan/ZIM/cmd/zim-mesh/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	"github.com/zciyuan/ZIM/internal/component/token"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	convdao "github.com/zciyuan/ZIM/internal/dao/conversation"
	"github.com/zciyuan/ZIM/internal/dao/friend"
	groupdao "github.com/zciyuan/ZIM/internal/dao/group"
	"github.com/zciyuan/ZIM/internal/dao/user"
	"github.com/zciyuan/ZIM/internal/model"
	account "github.com/zciyuan/ZIM/internal/service/account/server"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	groupserver "github.com/zciyuan/ZIM/internal/service/group/server"
	groupversion "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/version"
)

// dependencies owns the shared PostgreSQL, Mongo, Redis and domain services.
type dependencies struct {
	database     *postgres.Postgres
	mongodb      *mongocomp.Mongo
	cache        *rediscomp.Redis
	account      *account.Service
	changeLog    *changelogsvc.Service
	conversation *conversationsvc.Service
}

func main() {
	version.HandleFlag()

	deps := newDependencies()
	component := newMeshComponent()
	app.Init(component.Proxy(), deps.appDeps())

	container := due.NewContainer()
	container.Add(
		component,
		deps.database,
		deps.mongodb,
		deps.cache,
		deps.conversation,
		health.New("zim-mesh"),
	)
	container.Serve()
}

func newDependencies() *dependencies {
	database := postgres.New()
	if database.Pool() == nil {
		log.Fatal("zim-mesh requires etc.zim.postgres.dsn")
	}

	mongodb := mongocomp.New()
	if mongodb.Database() == nil {
		log.Fatal("zim-mesh requires etc.zim.mongo.uri")
	}

	cache := rediscomp.New()
	issuer := token.MustFromEtc()

	accountService, err := account.New(user.New(database.Pool()), issuer)
	if err != nil {
		log.Fatalf("create account service failed: %v", err)
	}

	changeLog := changelogsvc.New(
		changelogdao.New(mongodb.Database()),
		changelogsvc.NewRedisVersionSource(cache.Client()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = changeLog.EnsureIndexes(ctx, []string{
		model.EntityFriend,
		model.EntityGroupMember,
	})
	cancel()
	if err != nil {
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}

	return &dependencies{
		database:     database,
		mongodb:      mongodb,
		cache:        cache,
		account:      accountService,
		changeLog:    changeLog,
		conversation: conversationsvc.New(convdao.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0)),
	}
}

func (d *dependencies) appDeps() app.Deps {
	return app.Deps{
		Account:      d.account,
		UserRepo:     user.New(d.database.Pool()),
		UserCache:    d.cache.Client(),
		FriendRepo:   friend.New(d.database.Pool()),
		FriendCache:  d.cache.Client(),
		GroupRepo:    groupdao.New(d.database.Pool()),
		ChangeLog:    d.changeLog,
		Conversation: d.conversation,
		GroupVersion: groupversion.New(d.cache.Client()),
		MemberCache:  groupserver.NewMemberCache(d.cache.Client()),
		Webhook: webhook.New(
			etc.Get("etc.zim.webhook.url", "").String(),
			etc.Get("etc.zim.webhook.secret", "").String(),
		),
	}
}

func newMeshComponent() *mesh.Mesh {
	return mesh.NewMesh(
		mesh.WithRegistry(nacos.NewRegistry()),
		mesh.WithTransporter(grpc.NewTransporter()),
	)
}
