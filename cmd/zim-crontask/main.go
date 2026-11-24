// Command zim-crontask runs periodic cleanup jobs under a Redis leader lock.
package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	lockredis "github.com/dobyte/due/lock/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/encoding/json"
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
	burnserver "github.com/zciyuan/ZIM/internal/service/burn/server"
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

	// ADR-0043: burn-after-reading timer sweep. The toPush publisher reuses the
	// burn.delete event path consumed by zim-push; media cleanup goes to its
	// dedicated topic. Both degrade to erase-only when Kafka is not configured.
	messageDAO := messagedao.New(mongodb.Database())
	burnSweepCtx, burnSweepCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if burnErr := messageDAO.EnsureBurnIndexes(burnSweepCtx); burnErr != nil {
		burnSweepCancel()
		log.Fatalf("ensure burn indexes failed: %v", burnErr)
	}
	burnSweepCancel()
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 {
		kafka := burnserver.NewKafkaPublisher(pipe.NewKafkaProducer(brokers))
		crontask.WithBurnSweeper(burnserver.New(burnserver.NewMessageRepo(messageDAO), kafka, kafka),
			etc.Get("etc.zim.burn.sweepInterval", "5s").Duration())
	} else {
		crontask.WithBurnSweeper(burnserver.New(burnserver.NewMessageRepo(messageDAO), nil, nil),
			etc.Get("etc.zim.burn.sweepInterval", "5s").Duration())
	}

	// ADR-0043 §2.4.3: burn media deletion consumer. The event carries the
	// object key; deletion is local-dir (S3 lifecycle is the deployment-level
	// backstop for object stores).
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 {
		mediaConsumer := pipe.NewKafkaConsumer(brokers, define.ConsumerGroupBurnMedia, define.TopicBurnMediaDelete)
		go pipe.Run(context.Background(), mediaConsumer, func(ctx context.Context, message pipe.Message) error {
			event := struct {
				ObjectKey string `json:"object_key"`
			}{}
			if err := json.Unmarshal(message.Value, &event); err != nil {
				log.Warnf("burn media delete decode failed: %v", err)
				return pipe.Poison(err)
			}
			if event.ObjectKey == "" {
				return nil
			}
			full := filepath.Join(etc.Get("etc.zim.upload.dir", "./run/upload").String(), filepath.FromSlash(event.ObjectKey))
			if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
				log.Warnf("burn media delete failed: key = %s, err = %v", event.ObjectKey, err)
				return err
			}
			return nil
		}, func(err error) {
			log.Errorf("burn media delete consumer restarted: %v", err)
		})
	}

	container.Add(crontask, database, mongodb, cache, health.New(define.ServiceCrontask))
	container.Serve()
}
