package logic

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

const presenceWatchTTL = 2 * time.Minute

// subscribeOnline handles 1106 and starts watching the requested users.
func (c *core) subscribeOnline(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		deviceKey := ctx.UID()
		if deviceKey <= 0 {
			c.respond(ctx, contract.OpSubscribeOnline, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgwire.SubscribeOnlineReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSubscribeOnline, nil, errs.ErrMessageEmpty)
			return
		}
		uids := make([]int64, 0, len(req.GetUids()))
		seen := make(map[int64]bool, len(req.GetUids()))
		for _, uid := range req.GetUids() {
			if uid <= 0 || seen[uid] || len(uids) >= 200 {
				continue
			}
			seen[uid] = true
			uids = append(uids, uid)
		}
		now := time.Now()
		last := make(map[int64]bool, len(uids))
		users := make([]*msgwire.OnlineUser, 0, len(uids))
		for _, uid := range uids {
			online := c.anyOnline(ctx.Context(), uid)
			last[uid] = online
			users = append(users, &msgwire.OnlineUser{Uid: uid, Online: online})
		}
		payload, _ := proto.Marshal(&msgwire.SubscribeOnlineRes{Users: users})
		c.respond(ctx, contract.OpSubscribeOnline, payload, nil)
		c.trackPresence(deviceKey, uids, last, now.Add(presenceWatchTTL))
	})
}

// trackPresence records (or refreshes) the online watcher for one device key.
// The map is created lazily as well as in NewCore so a zero-value core (tests,
// future wiring) cannot panic with "assignment to entry in nil map".
func (c *core) trackPresence(deviceKey int64, targets []int64, last map[int64]bool, expires time.Time) {
	c.watchMu.Lock()
	if c.watches == nil {
		c.watches = make(map[int64]*presenceWatch)
	}
	c.watches[deviceKey] = &presenceWatch{targets: targets, last: last, expires: expires}
	c.watchMu.Unlock()
}

// runPresenceWatch polls subscribed targets and pushes 1903 on changes.
func (c *core) runPresenceWatch(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollPresence(ctx)
		}
	}
}

func (c *core) pollPresence(ctx context.Context) {
	now := time.Now()
	type update struct {
		key     int64
		payload []byte
	}
	updates := make([]update, 0)
	c.watchMu.Lock()
	for key, watch := range c.watches {
		if now.After(watch.expires) {
			delete(c.watches, key)
			continue
		}
		for _, uid := range watch.targets {
			online := c.anyOnline(ctx, uid)
			if watch.last[uid] == online {
				continue
			}
			watch.last[uid] = online
			payload, _ := proto.Marshal(&msgwire.OnlineChangePush{Uid: uid, Online: online})
			updates = append(updates, update{key: key, payload: payload})
		}
	}
	c.watchMu.Unlock()
	for _, item := range updates {
		c.pushDeviceKeys(ctx, []int64{item.key}, contract.OpPushOnline, item.payload, false)
	}
}
