package route

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

// DeviceSource returns the online device keys of users.
type DeviceSource interface {
	Keys(ctx context.Context, ownerIDs []int64) ([]int64, error)
}

// RedisDeviceIndex reads the per-user online set maintained by Router.Bind and
// Router.Unbind.
type RedisDeviceIndex struct {
	client    redis.UniversalClient
	prefix    string
	fallback  DeviceSource
	downUntil atomic.Int64
	l1        sync.Map
	l1TTL     time.Duration
}

type deviceCacheEntry struct {
	keys   []int64
	expire int64
}

// NewRedisDeviceIndex returns an online-device index. fallback may be nil.
func NewRedisDeviceIndex(client redis.UniversalClient, prefix string, fallback DeviceSource) *RedisDeviceIndex {
	return &RedisDeviceIndex{client: client, prefix: prefix, fallback: fallback, l1TTL: 2 * time.Second}
}

// Keys returns online device keys, using a fallback on a Redis failure.
func (i *RedisDeviceIndex) Keys(ctx context.Context, ownerIDs []int64) ([]int64, error) {
	if i == nil || i.client == nil || time.Now().UnixNano() < i.downUntil.Load() {
		return i.fallbackKeys(ctx, ownerIDs)
	}
	now := time.Now().UnixMilli()
	keys := make([]int64, 0, len(ownerIDs)*2)
	misses := make([]int64, 0, len(ownerIDs))
	for _, uid := range ownerIDs {
		if value, ok := i.l1.Load(uid); ok {
			entry := value.(deviceCacheEntry)
			if entry.expire > now {
				keys = append(keys, entry.keys...)
				continue
			}
			i.l1.Delete(uid)
		}
		misses = append(misses, uid)
	}
	for start := 0; start < len(misses); start += 500 {
		end := start + 500
		if end > len(misses) {
			end = len(misses)
		}
		pipeline := i.client.Pipeline()
		commands := make([]*redis.StringSliceCmd, 0, end-start)
		for _, uid := range misses[start:end] {
			commands = append(commands, pipeline.SMembers(ctx, i.udevKey(uid)))
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			i.downUntil.Store(time.Now().Add(5 * time.Second).UnixNano())
			return i.fallbackKeys(ctx, ownerIDs)
		}
		for index, command := range commands {
			uid := misses[start+index]
			entryKeys := make([]int64, 0, len(command.Val()))
			for _, raw := range command.Val() {
				var key int64
				if _, err := fmt.Sscan(raw, &key); err != nil {
					continue
				}
				if _, _, err := multi.Parse(key); err == nil {
					entryKeys = append(entryKeys, key)
				}
			}
			keys = append(keys, entryKeys...)
			i.l1.Store(uid, deviceCacheEntry{keys: entryKeys, expire: now + i.l1TTL.Milliseconds()})
		}
	}
	return keys, nil
}

// AddUdev records a device in the user's online set.
func (i *RedisDeviceIndex) AddUdev(ctx context.Context, uid, deviceKey int64) error {
	if i == nil || i.client == nil {
		return nil
	}
	if err := i.client.SAdd(ctx, i.udevKey(uid), deviceKey).Err(); err != nil {
		return err
	}
	i.updateCacheAdd(uid, deviceKey)
	return nil
}

// RemoveUdev drops a device from the user's online set.
func (i *RedisDeviceIndex) RemoveUdev(ctx context.Context, uid, deviceKey int64) error {
	if i == nil || i.client == nil {
		return nil
	}
	if err := i.client.SRem(ctx, i.udevKey(uid), deviceKey).Err(); err != nil {
		return err
	}
	i.updateCacheRemove(uid, deviceKey)
	return nil
}

func (i *RedisDeviceIndex) updateCacheAdd(uid, deviceKey int64) {
	value, ok := i.l1.Load(uid)
	if !ok {
		return
	}
	entry := value.(deviceCacheEntry)
	if entry.expire <= time.Now().UnixMilli() {
		i.l1.Delete(uid)
		return
	}
	for _, key := range entry.keys {
		if key == deviceKey {
			return
		}
	}
	entry.keys = append(entry.keys, deviceKey)
	entry.expire = time.Now().Add(i.l1TTL).UnixMilli()
	i.l1.Store(uid, entry)
}

func (i *RedisDeviceIndex) updateCacheRemove(uid, deviceKey int64) {
	value, ok := i.l1.Load(uid)
	if !ok {
		return
	}
	entry := value.(deviceCacheEntry)
	filtered := entry.keys[:0]
	for _, key := range entry.keys {
		if key != deviceKey {
			filtered = append(filtered, key)
		}
	}
	entry.keys = filtered
	entry.expire = time.Now().Add(i.l1TTL).UnixMilli()
	i.l1.Store(uid, entry)
}

func (i *RedisDeviceIndex) fallbackKeys(ctx context.Context, ownerIDs []int64) ([]int64, error) {
	if i == nil || i.fallback == nil {
		return nil, fmt.Errorf("device index unavailable")
	}
	return i.fallback.Keys(ctx, ownerIDs)
}

func (i *RedisDeviceIndex) udevKey(uid int64) string {
	prefix := i.prefix
	if prefix == "" {
		prefix = "zciyuan"
	}
	return fmt.Sprintf("%s:udev:%d", prefix, uid)
}
