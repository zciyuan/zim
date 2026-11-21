// Package app assembles the message node application.
package app

import (
	"github.com/dobyte/due/v2/cluster/node"

	"github.com/zciyuan/ZIM/cmd/zim-node-msg/app/logic"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	devicesvc "github.com/zciyuan/ZIM/internal/service/device/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	reactionsvc "github.com/zciyuan/ZIM/internal/service/reaction/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
	zimroute "github.com/zciyuan/ZIM/zimfw/route"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

// Init registers all message node routes and event handlers.
func Init(proxy *node.Proxy, msg *message.Service, pipeline *message.Pipeline,
	conv *conversationsvc.Service, changes *changelogsvc.Service,
	groupRead *groupreadsvc.Service, rtcCalls *rtcsvc.Service, rtcProvider rtcfw.Provider, rtcClientURL string,
	families *familysvc.Service, connRegistry zimroute.ConnRegistry, syncService *syncsvc.Service,
	deviceService *devicesvc.Service, deviceIndex zimroute.DeviceSource, reactionService *reactionsvc.Service) {
	logic.NewCore(proxy, msg, pipeline, conv, changes, groupRead, rtcCalls, rtcProvider, families).
		WithConnRegistry(connRegistry).
		WithSync(syncService).
		WithDevices(deviceService).
		WithDeviceIndex(deviceIndex).
		WithReactions(reactionService).
		WithRTCClientURL(rtcClientURL).
		Init()
}
