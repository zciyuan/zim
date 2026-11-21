// Command zim-api serves client REST, server integration API and webhooks.
package main

import (
	"context"
	"time"

	"github.com/dobyte/due/component/http/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-api/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	"github.com/zciyuan/ZIM/internal/component/token"
	adminauditdao "github.com/zciyuan/ZIM/internal/dao/admin"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	convdao "github.com/zciyuan/ZIM/internal/dao/conversation"
	familydao "github.com/zciyuan/ZIM/internal/dao/family"
	groupdao "github.com/zciyuan/ZIM/internal/dao/group"
	messagedao "github.com/zciyuan/ZIM/internal/dao/message"
	notificationdao "github.com/zciyuan/ZIM/internal/dao/notification"
	"github.com/zciyuan/ZIM/internal/dao/user"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	clientconfigsvc "github.com/zciyuan/ZIM/internal/service/clientconfig/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	groupversionsvc "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	historysvc "github.com/zciyuan/ZIM/internal/service/history/server"
	notificationsvc "github.com/zciyuan/ZIM/internal/service/notification/server"
	presencesvc "github.com/zciyuan/ZIM/internal/service/presence/server"
	privacysvc "github.com/zciyuan/ZIM/internal/service/privacy/server"
	uploadsvc "github.com/zciyuan/ZIM/internal/service/upload/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	server := http.NewServer(
		http.WithName("api"),
		http.WithRegistry(registry),
		http.WithTransporter(transporter),
	)

	database := postgres.New()
	cache := rediscomp.New()
	mongodb := mongocomp.New()
	issuer := token.MustFromEtc()
	changeLog := newChangeLog(mongodb, cache)
	groupRead := groupreadsvc.New(cache.Client())
	familyService := familysvc.New(familydao.New(database.Pool()))
	adminService := adminsvc.New(user.New(database.Pool()), groupdao.New(database.Pool()), adminauditdao.New(database.Pool()), adminauditdao.New(database.Pool()))
	uploadService := newUploadService()
	conversationService := newConversationService(database, cache)
	if conversationService != nil && changeLog != nil {
		conversationService.SetChangeLog(changeLog)
	}
	historyService := newHistoryService(mongodb)
	notificationService := notificationsvc.New(notificationdao.New(database.Pool()))
	var kickProducer pipe.Producer
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 {
		kickProducer = pipe.NewKafkaProducer(brokers)
		notificationService.SetBroadcaster(notificationsvc.NewKafkaBroadcaster(
			kickProducer, user.New(database.Pool())))
	}

	clientConfigService := clientconfigsvc.New(adminauditdao.New(database.Pool()))
	clientConfigService.SetCache(cache.Client())
	groupVersionService := groupversionsvc.New(cache.Client())
	privacyService := privacysvc.New(user.New(database.Pool()))
	presenceService := presencesvc.New(cache.Client(), etc.Get("etc.locate.redis.prefix", "zim").String())

	app.Init(
		server.Proxy(),
		issuer,
		conversationService,
		historyService,
		changeLog,
		groupRead,
		familyService,
		adminService,
		uploadService,
		cache,
		clientConfigService,
		groupVersionService,
		notificationService,
		privacyService,
		presenceService,
		kickProducer,
	)

	healthComponent := health.New(define.ServiceAPI)
	healthComponent.AddCheck("postgres", func(ctx context.Context) error { return database.Pool().Ping(ctx) })
	healthComponent.AddCheck("redis", func(ctx context.Context) error { return cache.Client().Ping(ctx).Err() })
	healthComponent.AddCheck("mongo", func(ctx context.Context) error { return mongodb.Database().Client().Ping(ctx, nil) })

	if conversationService != nil {
		container.Add(conversationService)
	}
	container.Add(server, database, cache, mongodb, healthComponent)
	container.Serve()
}

func newConversationService(database *postgres.Postgres, cache *rediscomp.Redis) *conversationsvc.Service {
	if database.Pool() == nil {
		return nil
	}
	return conversationsvc.New(convdao.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0))
}

func newChangeLog(database *mongocomp.Mongo, cache *rediscomp.Redis) *changelogsvc.Service {
	if etc.Get("etc.zim.degrade.mongo", false).Bool() {
		log.Warn("change log disabled by etc.zim.degrade.mongo")
		return nil
	}
	if database.Database() == nil {
		log.Warn("version log disabled: etc.zim.mongo.uri is empty")
		return nil
	}
	service := changelogsvc.New(changelogdao.New(database.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := service.EnsureIndexes(ctx, []string{
		model.EntityConversation, model.EntityFriend, model.EntityGroupMember,
	}); err != nil {
		cancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	cancel()
	return service
}

func newHistoryService(database *mongocomp.Mongo) *historysvc.Service {
	if etc.Get("etc.zim.degrade.mongo", false).Bool() {
		log.Warn("message history disabled by etc.zim.degrade.mongo")
		return nil
	}
	if database.Database() == nil {
		log.Warn("message history disabled: etc.zim.mongo.uri is empty")
		return nil
	}
	return historysvc.New(messagedao.New(database.Database()))
}

// newUploadService wires the attachment store (ZIM-02 §3.4).
func newUploadService() *uploadsvc.Service {
	secret := etc.Get("etc.zim.upload.secret", etc.Get("etc.zim.auth.secret").String()).String()
	if secret == "" {
		log.Warn("upload disabled: etc.zim.upload.secret and etc.zim.auth.secret are empty")
		return nil
	}
	service, err := uploadsvc.New(uploadsvc.Config{
		Provider:       etc.Get("etc.zim.upload.provider", "s3").String(),
		Dir:            etc.Get("etc.zim.upload.dir", "./run/upload").String(),
		Secret:         secret,
		MaxBytes:       etc.Get("etc.zim.upload.maxBytes", 0).Int64(),
		TTL:            etc.Get("etc.zim.upload.ttl", "15m").Duration(),
		Endpoint:       etc.Get("etc.zim.upload.s3.endpoint", "").String(),
		AccessKey:      etc.Get("etc.zim.upload.s3.accessKey", "").String(),
		SecretKey:      etc.Get("etc.zim.upload.s3.secretKey", "").String(),
		Bucket:         etc.Get("etc.zim.upload.s3.bucket", "zim").String(),
		UseSSL:         etc.Get("etc.zim.upload.s3.useSSL", false).Bool(),
		PublicEndpoint: etc.Get("etc.zim.upload.s3.publicEndpoint", "").String(),
	})
	if err != nil {
		log.Fatalf("create upload service failed: %v", err)
	}
	return service
}
