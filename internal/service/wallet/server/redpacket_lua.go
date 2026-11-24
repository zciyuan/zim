package wallet

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Redis key layout (ADR-0043 §1.2.D):
//
//	zim:rp:meta:{rp_id}     Hash   total_amount / total_count / expire_at / rp_type / target_uid
//	zim:rp:queue:{rp_id}    List   pre-split integer amounts (FIFO)
//	zim:rp:claimed:{rp_id}  Hash   claimer_uid -> amount
//
// All three keys share one TTL (expire_at + slack) so an expired packet's
// Redis state vanishes even when the DB refund job is late.

// claimRedPacketScript atomically validates and pops one share:
//
//	KEYS[1] meta, KEYS[2] queue, KEYS[3] claimed
//	ARGV[1] now ms, ARGV[2] claimer uid, ARGV[3] target uid (dedicated only)
//
// Returns: amount (positive) | -1 expired | -2 not target | -3 already claimed | -4 depleted
var claimRedPacketScript = redis.NewScript(`
local values = redis.call('HMGET', KEYS[1], 'total_amount', 'total_count', 'expire_at', 'rp_type', 'target_uid')
if not values[1] then
  return -1
end
local expireAt = tonumber(values[3] or '0')
if expireAt <= 0 or tonumber(ARGV[1]) > expireAt then
  return -1
end
local rpType = tonumber(values[4] or '2')
if rpType == 4 and ARGV[3] ~= '' and ARGV[3] ~= ARGV[2] then
  return -2
end
if redis.call('HEXISTS', KEYS[3], ARGV[2]) == 1 then
  return -3
end
local amount = redis.call('LPOP', KEYS[2])
if not amount then
  return -4
end
redis.call('HSET', KEYS[3], ARGV[2], amount)
return tonumber(amount)
`)

// redPacketClaimErr maps a negative Lua result to the ZIM business error.
func redPacketClaimErr(code int64) error {
	switch code {
	case -1:
		return errs.ErrRedPacketExpired
	case -2:
		return errs.ErrRedPacketNotTarget
	case -3:
		return errs.ErrRedPacketClaimed
	default:
		return errs.ErrRedPacketDepleted
	}
}

// splitRedPacket pre-splits total into count integer shares using the integer
// double-mean algorithm (ADR-0043 §4.4.1): every share >= minShare and the
// shares sum exactly to total. The result is Fisher-Yates shuffled so early
// claimants cannot infer the share order.
func splitRedPacket(total int64, count int, minShare int64) ([]int64, error) {
	if count <= 0 {
		return nil, errs.ErrRedPacketAmountInvalid
	}
	if minShare <= 0 {
		minShare = 1
	}
	if total < int64(count)*minShare {
		return nil, errs.ErrRedPacketAmountInvalid
	}
	shares := make([]int64, 0, count)
	remaining := total
	remainingCount := int64(count)
	for remainingCount > 1 {
		// max = min(floor(2R/N), R - (N-1)*min) — integer only, no float.
		max := 2 * remaining / remainingCount
		floor := remaining - (remainingCount-1)*minShare
		if max > floor {
			max = floor
		}
		share := randInt(minShare, max)
		shares = append(shares, share)
		remaining -= share
		remainingCount--
	}
	shares = append(shares, remaining) // the last share takes the exact remainder
	fisherYates(shares)
	return shares, nil
}

// randInt returns a uniform integer in [min, max] using crypto/rand so the
// share order cannot be predicted from outside.
func randInt(min, max int64) int64 {
	if max <= min {
		return min
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failure: fall back to the deterministic midpoint rather
		// than leaking or biasing the distribution.
		return min + (max-min)/2
	}
	n := binary.BigEndian.Uint64(buf[:])
	return min + int64(n%uint64(max-min+1))
}

func fisherYates(shares []int64) {
	for i := len(shares) - 1; i > 0; i-- {
		j := int(randInt(0, int64(i)))
		shares[i], shares[j] = shares[j], shares[i]
	}
}

// storeRedPacketRedis writes meta/queue and arms the shared TTL.
func (s *Service) storeRedPacketRedis(ctx context.Context, rp *walletdao.RedPacket, shares []int64) error {
	metaKey := redPacketMetaKey(rp.RPID)
	queueKey := redPacketQueueKey(rp.RPID)
	pipe := s.redis.Pipeline()
	pipe.HSet(ctx, metaKey, map[string]any{
		"total_amount": rp.TotalAmount,
		"total_count":  rp.TotalCount,
		"expire_at":    rp.ExpireAt,
		"rp_type":      rp.RpType,
		"target_uid":   rp.TargetUID,
	})
	values := make([]any, 0, len(shares))
	for _, share := range shares {
		values = append(values, share)
	}
	pipe.RPush(ctx, queueKey, values...)
	// TTL 以服务时钟为准（测试注入固定时钟时不能用真实时钟，否则立刻过期）。
	ttl := time.Duration(rp.ExpireAt-s.now().UnixMilli())*time.Millisecond + time.Hour
	pipe.Expire(ctx, metaKey, ttl)
	pipe.Expire(ctx, queueKey, ttl)
	pipe.Expire(ctx, redPacketClaimedKey(rp.RPID), ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func redPacketMetaKey(rpID int64) string    { return "zim:rp:meta:" + strconv.FormatInt(rpID, 10) }
func redPacketQueueKey(rpID int64) string   { return "zim:rp:queue:" + strconv.FormatInt(rpID, 10) }
func redPacketClaimedKey(rpID int64) string { return "zim:rp:claimed:" + strconv.FormatInt(rpID, 10) }
