// Command zim-push consumes toPush and delivers messages to online users.
package main

import (
	"time"

	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-push/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	zimredis "github.com/zciyuan/ZIM/internal/component/redis"
	devicedao "github.com/zciyuan/ZIM/internal/dao/device"
	"github.com/zciyuan/ZIM/internal/define"
	devicesvc "github.com/zciyuan/ZIM/internal/service/device/server"
	pushoffline "github.com/zciyuan/ZIM/internal/service/pushoffline/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	zimroute "github.com/zciyuan/ZIM/zimfw/route"
)

func main() {
	version.HandleFlag()
	contract.InstallPacker()

	container := due.NewContainer()
	redisComponent := zimredis.New()
	database := postgres.New()
	locator := redis.NewLocator()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	component := node.NewNode(
		node.WithLocator(locator),
		node.WithRegistry(registry),
		node.WithTransporter(transporter),
	)

	batchLocator, err := zimroute.NewRedisBatchLocator(
		redisComponent.Client(),
		etc.Get("etc.locate.redis.prefix", "due:locate").String(),
	)
	if err != nil {
		log.Fatalf("create batch locator failed: %v", err)
	}

	var offline *pushoffline.Dispatcher
	if database.Pool() != nil {
		offline = pushoffline.NewDispatcher(
			pushoffline.NewCachedDevices(devicesvc.New(devicedao.New(database.Pool())), redisComponent.Client(), 5*time.Minute),
			newOfflineProvider(),
			pushoffline.NewRedisLimiter(
				redisComponent.Client(),
				int64(etc.Get("etc.zim.push.offline.ratePerMinute", 5).Int()),
				time.Minute,
			),
		)
	} else {
		log.Warn("offline push disabled: etc.zim.postgres.dsn is empty")
	}

	var consumer pipe.Consumer
	var extraConsumers []pipe.Consumer
	var producer pipe.Producer
	var offlineConsumer pipe.Consumer
	brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings()
	if len(brokers) > 0 && !etc.Get("etc.zim.degrade.kafka", false).Bool() {
		producer = pipe.NewKafkaProducer(brokers)
		consumerCount := etc.Get("etc.zim.pipe.pushConsumers", 8).Int()
		if consumerCount <= 0 {
			consumerCount = 1
		}
		pushConsumers := make([]pipe.Consumer, 0, consumerCount)
		for i := 0; i < consumerCount; i++ {
			pushConsumers = append(pushConsumers, pipe.NewKafkaConsumer(
				brokers, define.ConsumerGroupPush, define.TopicToPush,
				pipe.WithDLQ(producer, define.TopicDLQ(define.TopicToPush)),
				pipe.WithDLQObserver(func(topic string) {
					metrics.Default.Inc("zim_dlq_total", map[string]string{"topic": topic})
				}),
				pipe.WithLastOffset(),
			))
		}
		consumer = pushConsumers[0]
		extraConsumers = pushConsumers[1:]
		offlineConsumer = pipe.NewKafkaConsumer(brokers, define.ConsumerGroupOffline, define.TopicToOfflinePush,
			pipe.WithDLQ(producer, define.TopicDLQ(define.TopicToOfflinePush)),
			pipe.WithDLQObserver(func(topic string) {
				metrics.Default.Inc("zim_dlq_total", map[string]string{"topic": topic})
			}),
			pipe.WithLastOffset())
	} else {
		log.Warn("push consumer disabled: etc.zim.pipe.brokers is empty")
	}

	pushApp := app.New(app.Options{
		Proxy: component.Proxy(), Locator: batchLocator,
		Consumer: consumer, Consumers: extraConsumers, Producer: producer,
		OfflineConsumer: offlineConsumer, OfflinePublisher: pushoffline.NewPublisher(producer),
		Devices: zimroute.NewRedisDeviceIndex(redisComponent.Client(),
			etc.Get("etc.locate.redis.prefix", "due:locate").String(),
			devicesvc.New(devicedao.New(database.Pool()))),
		Offline: offline,
	})

	container.Add(redisComponent, database, component, pushApp, health.New(define.ServicePush))
	container.Serve()
}

// newOfflineProvider builds the configured provider router. The default is
// dummy so local tests keep working without external push credentials.
func newOfflineProvider() pushoffline.Provider {
	providers := map[string]pushoffline.Provider{"dummy": pushoffline.Dummy{}}
	if url := etc.Get("etc.zim.push.offline.providers.http.url", "").String(); url != "" {
		providers["http"] = pushoffline.NewHTTPProvider("http", url,
			etc.Get("etc.zim.push.offline.providers.http.secret", "").String(),
			etc.Get("etc.zim.push.offline.providers.http.timeout", "5s").Duration())
	}
	if key := etc.Get("etc.zim.push.offline.providers.fcm.serverKey", "").String(); key != "" {
		providers["fcm"] = pushoffline.NewFCMProvider(key,
			etc.Get("etc.zim.push.offline.providers.fcm.endpoint", "").String(),
			etc.Get("etc.zim.push.offline.providers.fcm.timeout", "5s").Duration())
	}
	defaultName := etc.Get("etc.zim.push.offline.provider", "dummy").String()
	return pushoffline.NewRouter(defaultName, providers, map[int16]string{
		1: etc.Get("etc.zim.push.offline.iosProvider", "").String(),
		2: etc.Get("etc.zim.push.offline.androidProvider", "").String(),
	})
}
