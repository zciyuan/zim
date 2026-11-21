package logic

import (
	"context"
	"sync"
	"time"

	"github.com/dobyte/due/v2/cluster/node"

	"github.com/zciyuan/ZIM/internal/component/ratelimit"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	devicesvc "github.com/zciyuan/ZIM/internal/service/device/server"
	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
	groupreadsvc "github.com/zciyuan/ZIM/internal/service/groupread/server"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	reactionsvc "github.com/zciyuan/ZIM/internal/service/reaction/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
	"github.com/zciyuan/ZIM/zimfw/route"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

// core is the message node application core. It owns route registration and
// the login use case; it has no process or transport startup logic.
type core struct {
	proxy         *node.Proxy
	verify        verifyTokenFunc
	bind          func(ctx context.Context, nodeCtx loginContext, uid int64, platform int32, slot int32) error
	msg           *message.Service
	pipeline      *message.Pipeline
	conv          *conversationsvc.Service
	changes       *changelogsvc.Service
	groupRead     *groupreadsvc.Service
	sendLimiter   *ratelimit.Limiter
	typingLimiter *ratelimit.Limiter
	rtcCalls      *rtcsvc.Service
	rtcProvider   rtcfw.Provider
	family        *familysvc.Service
	conns         route.ConnRegistry
	sync          *syncsvc.Service
	devices       *devicesvc.Service
	deviceIndex   route.DeviceSource
	reactions     *reactionsvc.Service
	rtcClientURL  string
	watchMu       sync.Mutex
	watches       map[int64]*presenceWatch
	binds         *keyedActor
	rtcActors     *keyedActor
}

// loginService implements the op:1001 use case without due/node dependencies.
type loginService struct {
	verify verifyTokenFunc
	bind   bindDeviceFunc
	slot   resolveSlotFunc
	now    func() time.Time
}

// directory adapts mesh clients to the message Directory interface.
type directory struct {
	proxy *node.Proxy
}

// presenceWatch is one connection's online-status subscription.
type presenceWatch struct {
	targets []int64
	last    map[int64]bool
	expires time.Time
}
