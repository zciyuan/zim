// Package app assembles the zim-voiceroom mesh application.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	voiceroom "github.com/zciyuan/ZIM/internal/service/voiceroom/server"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

// Init registers the voiceroom mesh service provider and returns the service
// plus the webhook HTTP component for the container.
func Init(proxy *mesh.Proxy, repo voiceroom.VoiceRoomRepo, cache redis.UniversalClient,
	media rtc.Provider, pusher voiceroom.Pusher, cfg voiceroom.Config, webhookAddr string) (*voiceroom.Service, *voiceroom.Webhook) {
	service := voiceroom.New(proxy, repo, cache, media, pusher, nil, cfg)
	service.Init()
	webhook := voiceroom.NewWebhook(service, webhookAddr)
	return service, webhook
}
