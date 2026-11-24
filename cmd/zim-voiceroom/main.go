// Command zim-voiceroom serves the voice-room lifecycle and seat state
// machine (ADR-0043, op 1808-1819). It is a mesh process: the same
// locator/registry/transporter pattern as zim-mesh-group.
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

	"github.com/zciyuan/ZIM/cmd/zim-voiceroom/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	voiceroomdao "github.com/zciyuan/ZIM/internal/dao/voiceroom"
	"github.com/zciyuan/ZIM/internal/define"
	voiceroom "github.com/zciyuan/ZIM/internal/service/voiceroom/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()
	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()
	database := postgres.New()
	if database.Pool() == nil {
		log.Fatal("zim-voiceroom requires etc.zim.postgres.dsn")
	}
	cache := rediscomp.New()

	component := mesh.NewMesh(
		mesh.WithRegistry(registry),
		mesh.WithTransporter(transporter),
	)

	cfg := voiceroom.DefaultConfig()
	if seatCount := etc.Get("etc.zim.voiceroom.seatCount", 0).Int(); seatCount > 0 {
		cfg.SeatCount = int32(seatCount)
	}
	if value := etc.Get("etc.zim.voiceroom.reconnectTimeoutMs", 30000).Int(); value > 0 {
		cfg.ReconnectTimeout = time.Duration(value) * time.Millisecond
	}
	if value := etc.Get("etc.zim.voiceroom.emptyTimeoutMs", 300000).Int(); value > 0 {
		cfg.EmptyTimeout = time.Duration(value) * time.Millisecond
	}
	if value := etc.Get("etc.zim.voiceroom.roomTTL", "168h").Duration(); value > 0 {
		cfg.RoomTTL = value
	}
	cfg.SFUURL = etc.Get("etc.zim.voiceroom.sfuUrl", "").String()
	cfg.WebhookSecret = etc.Get("etc.zim.voiceroom.webhookSecret", "").String()

	media := newMediaProvider()
	pusher := voiceroom.NewProxyPusher(component.Proxy())
	service, webhook := app.Init(component.Proxy(), voiceroomdao.New(database.Pool()),
		cache.Client(), media, pusher, cfg, etc.Get("etc.zim.voiceroom.webhookAddr", "").String())

	// ADR-0043 G-10: consume group mute/kick events and enforce them on the
	// media plane (revoke CanPublish / evict). Disabled without Kafka brokers.
	startMemberStatusConsumer(service, component.Proxy())

	healthComponent := health.New(define.ServiceVoiceroom)
	healthComponent.AddCheck("postgres", func(ctx context.Context) error { return database.Pool().Ping(ctx) })
	healthComponent.AddCheck("redis", func(ctx context.Context) error { return cache.Client().Ping(ctx).Err() })
	container.Add(component, database, cache, service, webhook, healthComponent)
	container.Serve()
}

// startMemberStatusConsumer consumes zim.pipe.groupMemberStatus and applies
// each event to the rooms of that group.
func startMemberStatusConsumer(service *voiceroom.Service, proxy *mesh.Proxy) {
	brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings()
	if len(brokers) == 0 {
		log.Warn("voiceroom member status consumer disabled: etc.zim.pipe.brokers is empty")
		return
	}
	consumer := pipe.NewKafkaConsumer(brokers, define.ConsumerGroupVoiceroomStatus, define.TopicGroupMemberStatus)
	go pipe.Run(context.Background(), consumer, func(ctx context.Context, message pipe.Message) error {
		event, err := voiceroom.DecodeMemberStatus(message.Value)
		if err != nil {
			log.Warnf("voiceroom member status decode failed: %v", err)
			return pipe.Poison(err)
		}
		if err := service.HandleMemberStatus(ctx, event); err != nil {
			log.Warnf("voiceroom member status apply failed: group = %d, uid = %d, err = %v",
				event.GroupID, event.UID, err)
			return err
		}
		return nil
	}, func(err error) {
		log.Errorf("voiceroom member status consumer restarted: %v", err)
	})
}

// newMediaProvider builds the RTC provider from etc.zim.voiceroom.livekit;
// an empty URL falls back to the in-memory dummy.
func newMediaProvider() rtc.Provider {
	if url := etc.Get("etc.zim.voiceroom.livekitUrl", "").String(); url != "" {
		return rtc.NewLiveKit(rtc.LiveKitConfig{
			URL:       url,
			APIKey:    etc.Get("etc.zim.voiceroom.livekitApiKey", "").String(),
			APISecret: etc.Get("etc.zim.voiceroom.livekitApiSecret", "").String(),
			TokenTTL:  etc.Get("etc.zim.voiceroom.livekitTokenTTL", "1h").Duration(),
		})
	}
	return rtc.NewDummy()
}
