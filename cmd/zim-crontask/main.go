// Command zim-crontask runs periodic cleanup jobs under a Redis leader lock.
package main

import (
	"time"

	lockredis "github.com/dobyte/due/lock/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-crontask/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	frienddao "github.com/zciyuan/ZIM/internal/dao/friend"
	messagedao "github.com/zciyuan/ZIM/internal/dao/message"
	"github.com/zciyuan/ZIM/internal/define"
	callrecord "github.com/zciyuan/ZIM/internal/service/callrecord/server"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	gateclean "github.com/zciyuan/ZIM/internal/service/gateclean/server"
	maintenance "github.com/zciyuan/ZIM/internal/service/maintenance/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()
	database := postgres.New()
	mongodb := mongocomp.New()
	if database.Pool() == nil {
		log.Fatal("zim-crontask requires etc.zim.postgres.dsn")
	}
	if mongodb.Database() == nil {
		log.Fatal("zim-crontask requires etc.zim.mongo.uri")
	}

	retention := etc.Get("etc.zim.message.retention", "720h").Duration()
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), nil)
	cache := rediscomp.New()
	service := maintenance.New(frienddao.New(database.Pool()), messagedao.New(mongodb.Database()), retention, changeLog)
	var recorder *callrecord.Recorder
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 {
		recorder = callrecord.New(pipe.NewKafkaProducer(brokers))
	}
	service.SetCallTimeouter(rtcsvc.New(cache.Client()))
	maker := lockredis.NewMaker()
	locker := maker.Make("zim:crontask")
	interval := etc.Get("etc.zim.crontask.interval", "1h").Duration()
	cleaner := gateclean.New(
		cache.Client(),
		nacos.NewRegistry(),
		etc.Get("etc.locate.redis.prefix", "due:locate").String(),
	)
	cleanInterval := etc.Get("etc.zim.gateclean.interval", "30s").Duration()
	crontask := app.New(service, locker, interval, recorder, cleaner, cleanInterval, etc.Get("etc.zim.upload.dir", "").String(), etc.Get("etc.zim.upload.retention", "720h").Duration())

	container.Add(crontask, database, mongodb, cache, health.New(define.ServiceCrontask))
	container.Serve()
}
