package server

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

// ProxyPusher delivers room pushes through the mesh proxy using the
// uid → device-key fallback of zim-push: platforms 1..PlatformPad at slot 1.
// It is the standalone-binary pusher; the node side may push per-device
// directly and inject its own Pusher implementation instead.
type ProxyPusher struct {
	proxy *mesh.Proxy
}

// NewProxyPusher returns a proxy-backed pusher.
func NewProxyPusher(proxy *mesh.Proxy) *ProxyPusher {
	return &ProxyPusher{proxy: proxy}
}

// PushRoom multicasts one envelope to the room members' devices.
func (p *ProxyPusher) PushRoom(ctx context.Context, room *Room, opcode int32, payload []byte, uids []int64) {
	if p.proxy == nil || len(uids) == 0 {
		return
	}
	targets := make([]int64, 0, len(uids)*int(multi.PlatformPad))
	for _, uid := range uids {
		for platform := int32(1); platform <= multi.PlatformPad; platform++ {
			if deviceKey := multi.Key(uid, platform, 1); deviceKey != 0 {
				targets = append(targets, deviceKey)
			}
		}
	}
	pushCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := p.proxy.Multicast(pushCtx, &cluster.MulticastArgs{
		Kind: cluster.User, Targets: targets,
		Message: &cluster.Message{Route: opcode, Data: payload},
	}); err != nil {
		// A single stale device key sinks the whole multicast; retry per
		// target so live devices still receive the frame (same fallback as
		// node-msg pushDeviceKeys).
		log.Warnf("voiceroom device multicast failed: route = %d, targets = %d, err = %v; retrying per target",
			opcode, len(targets), err)
		for _, target := range targets {
			targetCtx, targetCancel := context.WithTimeout(ctx, time.Second)
			_ = p.proxy.Push(targetCtx, &cluster.PushArgs{
				Kind: cluster.User, Target: target,
				Message: &cluster.Message{Route: opcode, Data: payload},
			})
			targetCancel()
		}
	}
}
