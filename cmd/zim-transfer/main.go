// Command zim-transfer runs the Kafka message pipeline: seq, persistence,
// conversation fanout and toPush publication.
package main

import (
	"context"
	"time"

	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-transfer/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/metrics"
	mongocomp "github.com/zciyuan/ZIM/internal/component/mongo"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	changelogdao "github.com/zciyuan/ZIM/internal/dao/changelog"
	convdao "github.com/zciyuan/ZIM/internal/dao/conversation"
	messagedao "github.com/zciyuan/ZIM/internal/dao/message"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/idem"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	"github.com/zciyuan/ZIM/zimfw/seq"
)

func main() {
	version.HandleFlag()

	container := due.NewContainer()
	database := postgres.New()
	cache := rediscomp.New()
	mongodb := mongocomp.New()
	if database.Pool() == nil {
		log.Fatal("zim-transfer requires etc.zim.postgres.dsn")
	}
	if mongodb.Database() == nil {
		log.Fatal("zim-transfer requires etc.zim.mongo.uri")
	}

	allocator, err := seq.New(seq.NewRedisRunner(cache.Client()), int64(etc.Get("etc.zim.seq.block", 1).Int()))
	if err != nil {
		log.Fatalf("create seq allocator failed: %v", err)
	}
	conversationService := conversationsvc.New(convdao.New(database.Pool()), conversationsvc.NewRedisReadCache(cache.Client(), 0))
	changeLog := changelogsvc.New(changelogdao.New(mongodb.Database()), changelogsvc.NewRedisVersionSource(cache.Client()))
	indexCtx, indexCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = changeLog.EnsureIndexes(indexCtx, []string{model.EntityConversation}); err != nil {
		indexCancel()
		log.Fatalf("ensure changelog indexes failed: %v", err)
	}
	indexCancel()
	conversationService.SetChangeLog(changeLog)
	messageDAO := messagedao.New(mongodb.Database())
	messageCtx, messageCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = messageDAO.EnsureIndexes(messageCtx); err != nil {
		messageCancel()
		log.Fatalf("ensure message indexes failed: %v", err)
	}
	messageCancel()

	var consumers []pipe.Consumer
	var producer pipe.Producer
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 && !etc.Get("etc.zim.degrade.kafka", false).Bool() {
		producer = pipe.NewKafkaProducer(brokers)
		dlqObserver := func(topic string) {
			metrics.Default.Inc("zim_dlq_total", map[string]string{"topic": topic})
		}
		// Each topic has 24 partitions. One consumer per topic serializes the
		// fetch/process loop and becomes the tail-latency bottleneck; run a
		// small same-group pool so partitions are processed in parallel while
		// per-conversation ordering is preserved by the partition key.
		consumerCount := etc.Get("etc.zim.pipe.consumersPerTopic", 16).Int()
		if consumerCount <= 0 {
			consumerCount = 1
		}
		consumers = make([]pipe.Consumer, 0, consumerCount*2)
		for i := 0; i < consumerCount; i++ {
			consumers = append(consumers,
				pipe.NewKafkaConsumer(brokers, define.ConsumerGroupTransfer, define.TopicToRedis,
					pipe.WithDLQ(producer, define.TopicDLQ(define.TopicToRedis)),
					pipe.WithDLQObserver(dlqObserver)),
				pipe.NewKafkaConsumer(brokers, define.ConsumerGroupTransfer, define.TopicToMongo,
					pipe.WithDLQ(producer, define.TopicDLQ(define.TopicToMongo)),
					pipe.WithDLQObserver(dlqObserver)),
			)
		}
	} else {
		log.Warn("transfer pipeline disabled: etc.zim.pipe.brokers is empty")
	}

	transferApp := app.New(messageDAO, allocator, idem.NewRedisStore(cache.Client(), 10*time.Minute),
		conversationService, consumers, producer,
		messagesvc.NewRedisResultQueue(cache.Client(), time.Minute),
		webhook.New(etc.Get("etc.zim.webhook.url", "").String(), etc.Get("etc.zim.webhook.secret", "").String()),
		app.WithWorkers(etc.Get("etc.zim.transfer.workers", 256).Int()),
	)

	container.Add(transferApp, database, cache, mongodb, conversationService, health.New(define.ServiceTransfer))
	container.Serve()
}
