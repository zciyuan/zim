// Command zim-node-msg 负责消息发送、拉取、撤回和回执。
package main

import (
	"context"
	"time"

	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-node-msg/app"
	"github.com/zciyuan/ZIM/cmd/zim-node-msg/app/logic"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	convdao "github.com/zciyuan/ZIM/internal/dao/conversation"
	devicedao "github.com/zciyuan/ZIM/internal/dao/device"
	familydao "github.com/zciyuan/ZIM/internal/dao/family"
	messagedao "github.com/zciyuan/ZIM/internal/dao/message"
	sensitivedao "github.com/zciyuan/ZIM/internal/dao/sensitive"
	webhookdao "github.com/zciyuan/ZIM/internal/dao/webhook"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	devicesvc "github.com/zciyuan/ZIM/internal/service/device/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	reactionsvc "github.com/zciyuan/ZIM/internal/service/reaction/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	sensitivesvc "github.com/zciyuan/ZIM/internal/service/sensitive/server"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/idem"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	zimroute "github.com/zciyuan/ZIM/zimfw/route"
	"github.com/zciyuan/ZIM/zimfw/rtc"
	"github.com/zciyuan/ZIM/zimfw/seq"
)

func main() {
	version.HandleFlag()
	contract.InstallPacker()

	container := due.NewContainer()
	locator := redis.NewLocator()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()

	database := postgres.New()
	var deviceService *devicesvc.Service
	if database.Pool() != nil {
		deviceService = devicesvc.New(devicedao.New(database.Pool()))
	}

	cache := rediscomp.New()
	connRegistry, err := zimroute.NewRedisConnRegistry(
		cache.Client(),
		etc.Get("etc.locate.redis.prefix", "due:locate").String(),
	)
	if err != nil {
		log.Fatalf("create connection registry failed: %v", err)
	}
	mongodb := mongocomp.New()
	if database.Pool() == nil {
		log.Fatal("zim-node-msg requires etc.zim.postgres.dsn")
	}
	if mongodb.Database() == nil {
		log.Fatal("zim-node-msg requires etc.zim.mongo.uri")
	}

	allocator, err := seq.New(seq.NewRedisRunner(cache.Client()), etc.Get("etc.zim.seq.block", 1).Int64())
	if err != nil {
		log.Fatalf("create seq allocator failed: %v", err)
	}

	conversationService := conversationsvc.New(convdao.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0))
	messageDAO := messagedao.New(mongodb.Database())
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	changeCtx, changeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = changeLog.EnsureIndexes(changeCtx, []string{model.EntityFriend, model.EntityGroupMember}); err != nil {
		changeCancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	changeCancel()
	conversationService.SetChangeLog(changeLog)

	groupRead := groupreadsvc.New(cache.Client())
	reactionService := reactionsvc.New(cache.Client())
	familyService := familysvc.New(familydao.New(database.Pool()))
	rtcCalls := rtcsvc.New(cache.Client())
	rtcProvider := newRTCProvider()
	rtcClientURL := etc.Get("etc.zim.rtc.clientUrl", "wss://rtc.local").String()
	indexCtx, indexCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = messageDAO.EnsureIndexes(indexCtx); err != nil {
		indexCancel()
		log.Fatalf("ensure message indexes failed: %v", err)
	}
	indexCancel()

	component := node.NewNode(
		node.WithLocator(locator),
		node.WithRegistry(registry),
		node.WithTransporter(transporter),
	)
	messageService, err := messagesvc.New(
		messageDAO,
		allocator,
		idem.NewRedisStore(cache.Client(), 10*time.Minute),
		conversationService,
		logic.NewDirectory(component.Proxy()),
	)
	if err != nil {
		log.Fatalf("create message service failed: %v", err)
	}
	sensitiveService := sensitivesvc.New(sensitivedao.New(database.Pool()))
	sensitiveService.SetCache(cache.Client())
	messageService.WithSensitiveFilter(sensitiveService)
	messageService.WithWebhook(webhook.NewWithDLQ(etc.Get("etc.zim.webhook.url", "").String(), etc.Get("etc.zim.webhook.secret", "").String(), webhookdao.New(database.Pool())))
	var pipeline *messagesvc.Pipeline
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 && !etc.Get("etc.zim.degrade.kafka", false).Bool() {
		pipeline, err = messagesvc.NewPipeline(
			messageService,
			pipe.NewKafkaProducer(brokers),
			messagesvc.NewPubSubResultQueue(cache.Client(), time.Minute),
			5*time.Second,
		)
		if err != nil {
			log.Fatalf("create message pipeline failed: %v", err)
		}
		log.Infof("message Kafka pipeline enabled: brokers = %v", brokers)
	} else {
		log.Warn("message Kafka pipeline disabled: fallback to synchronous writes")
	}

	syncService := syncsvc.New(conversationService, messageService, changelogsvc.NewRedisVersionSource(cache.Client()))
	deviceIndex := zimroute.NewRedisDeviceIndex(
		cache.Client(),
		etc.Get("etc.locate.redis.prefix", "due:locate").String(),
		deviceService,
	)

	app.Init(component.Proxy(), messageService, pipeline, conversationService, changeLog, groupRead, rtcCalls, rtcProvider, rtcClientURL, familyService, connRegistry, syncService, deviceService, deviceIndex, reactionService)
	component.Proxy().SetDefaultRouteHandler(func(ctx node.Context) {
		log.Warnf("unhandled node route: cid = %d, route = %d", ctx.CID(), ctx.Route())
	})

	healthComponent := health.New(define.ServiceNodeMsg)
	healthComponent.AddCheck("postgres", func(ctx context.Context) error { return database.Pool().Ping(ctx) })
	healthComponent.AddCheck("redis", func(ctx context.Context) error { return cache.Client().Ping(ctx).Err() })
	healthComponent.AddCheck("mongo", func(ctx context.Context) error { return mongodb.Database().Client().Ping(ctx, nil) })
	container.Add(component, database, cache, mongodb, conversationService, groupRead, healthComponent)
	container.Serve()
}

// newRTCProvider builds the RTC provider from [zim.rtc].
//
// provider = dummy (default) issues local tokens; provider = livekit signs real
// LiveKit tokens and creates rooms through the server API.
func newRTCProvider() rtc.Provider {
	switch etc.Get("etc.zim.rtc.provider", "dummy").String() {
	case "livekit":
		return rtc.NewLiveKit(rtc.LiveKitConfig{
			URL:       etc.Get("etc.zim.rtc.url", "").String(),
			APIKey:    etc.Get("etc.zim.rtc.apiKey", "").String(),
			APISecret: etc.Get("etc.zim.rtc.apiSecret", "").String(),
			TokenTTL:  etc.Get("etc.zim.rtc.tokenTTL", "1h").Duration(),
		})
	default:
		return rtc.NewDummy()
	}
}
