package logic

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/redis/go-redis/v9"

	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
)

// spamMemberSeq makes ZSET members unique even when several sends land in the
// same millisecond (ZADD would otherwise collapse same-score members).
var spamMemberSeq atomic.Uint64

// groupPolicy implements message.GroupPolicy over Redis + the group mesh:
// the sliding window lives in zim:group:spam:{gid}:{uid} (ZSET, member =
// send timestamp), the offense counter in zim:group:offense:{gid}:{uid}
// (24h TTL), and AutoMute goes through the group service with operator 0
// (system). ADR-0043 §3.1.4.2.
type groupPolicy struct {
	proxy  *node.Proxy
	client redis.UniversalClient
	now    func() time.Time
}

// NewGroupPolicy returns the Redis-backed group anti-spam policy.
func NewGroupPolicy(proxy *node.Proxy, client redis.UniversalClient) message.GroupPolicy {
	return &groupPolicy{proxy: proxy, client: client, now: time.Now}
}

func spamWindowKey(gid, uid int64) string {
	return "zim:group:spam:" + strconv.FormatInt(gid, 10) + ":" + strconv.FormatInt(uid, 10)
}

func offenseKey(gid, uid int64) string {
	return "zim:group:offense:" + strconv.FormatInt(gid, 10) + ":" + strconv.FormatInt(uid, 10)
}

// CheckSpam records one send in the sliding window and returns the offense
// tier (1..3) when the window exceeded its budget; 0 means allowed.
func (g *groupPolicy) CheckSpam(ctx context.Context, gid, uid int64, windowSec, maxMsgs int32) (int, error) {
	if g == nil || g.client == nil {
		return 0, nil
	}
	if windowSec <= 0 {
		windowSec = 5
	}
	if maxMsgs <= 0 {
		maxMsgs = 5
	}
	windowKey := spamWindowKey(gid, uid)
	now := g.now().UnixMilli()
	member := fmt.Sprintf("%d-%d", now, spamMemberSeq.Add(1))
	pipe := g.client.Pipeline()
	pipe.ZAdd(ctx, windowKey, redis.Z{Score: float64(now), Member: member})
	pipe.ZRemRangeByScore(ctx, windowKey, "0", strconv.FormatInt(now-int64(windowSec)*1000, 10))
	cardCmd := pipe.ZCard(ctx, windowKey)
	pipe.Expire(ctx, windowKey, 2*time.Duration(windowSec)*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	if cardCmd.Val() <= int64(maxMsgs) {
		return 0, nil
	}
	// Over budget: bump the 24h offense counter and derive the tier.
	offense, err := g.client.Incr(ctx, offenseKey(gid, uid)).Result()
	if err != nil {
		return 0, err
	}
	_ = g.client.Expire(ctx, offenseKey(gid, uid), 24*time.Hour).Err()
	if offense >= 3 {
		return 3, nil
	}
	return int(offense), nil
}

// AutoMute mutes uid for seconds through the group service (system operator).
func (g *groupPolicy) AutoMute(ctx context.Context, gid, uid int64, seconds int64) error {
	if g == nil || g.proxy == nil {
		return nil
	}
	client, err := groupclient.NewClient(g.proxy.NewMeshClient)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	_, err = client.MuteMember(callCtx, &grouppb.MuteMemberArgs{
		GroupId: gid, OperatorId: 0, UserId: uid,
		Until: time.Now().UnixMilli() + seconds*1000,
	})
	return err
}
