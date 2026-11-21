package pushoffline

import (
	"context"
	"strconv"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/model"
)

const deviceCacheTTL = 5 * time.Minute

// CachedDevices 用 Redis 批量缓存设备查询，避免高 fanout 离线推送时
// 每条消息都对 PostgreSQL 执行一次 owner_id = ANY(...)。
type CachedDevices struct {
	source DeviceSource
	client redis.UniversalClient
	ttl    time.Duration
}

// NewCachedDevices wraps a device source with a per-uid Redis cache.
func NewCachedDevices(source DeviceSource, client redis.UniversalClient, ttl time.Duration) *CachedDevices {
	if ttl <= 0 {
		ttl = deviceCacheTTL
	}
	return &CachedDevices{source: source, client: client, ttl: ttl}
}

func deviceKey(uid int64) string {
	return "zim:push:devices:" + strconv.FormatInt(uid, 10)
}

// Targets returns cached devices and loads only cache misses in one query.
func (c *CachedDevices) Targets(ctx context.Context, ownerIDs []int64) ([]*model.Device, error) {
	if c == nil || c.source == nil {
		return nil, nil
	}
	if c.client == nil || len(ownerIDs) == 0 {
		return c.source.Targets(ctx, ownerIDs)
	}
	keys := make([]string, 0, len(ownerIDs))
	for _, uid := range ownerIDs {
		keys = append(keys, deviceKey(uid))
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		metrics.Default.Inc("zim_device_cache_total", map[string]string{"result": "error"})
		return c.source.Targets(ctx, ownerIDs)
	}
	devices := make([]*model.Device, 0, len(ownerIDs))
	misses := make([]int64, 0, len(ownerIDs))
	for index, value := range values {
		text, ok := value.(string)
		if !ok || text == "" {
			misses = append(misses, ownerIDs[index])
			continue
		}
		var cached []*model.Device
		if json.Unmarshal([]byte(text), &cached) != nil {
			misses = append(misses, ownerIDs[index])
			continue
		}
		devices = append(devices, cached...)
	}
	if len(misses) == 0 {
		metrics.Default.Inc("zim_device_cache_total", map[string]string{"result": "hit"})
		return devices, nil
	}
	loaded, err := c.source.Targets(ctx, misses)
	if err != nil {
		return nil, err
	}
	grouped := make(map[int64][]*model.Device, len(misses))
	for _, device := range loaded {
		if device != nil {
			grouped[device.OwnerID] = append(grouped[device.OwnerID], device)
		}
	}
	pipe := c.client.Pipeline()
	for _, uid := range misses {
		payload, marshalErr := json.Marshal(grouped[uid])
		if marshalErr != nil {
			continue
		}
		pipe.Set(ctx, deviceKey(uid), payload, c.ttl)
	}
	if _, err = pipe.Exec(ctx); err != nil {
		metrics.Default.Inc("zim_device_cache_total", map[string]string{"result": "write_error"})
	}
	metrics.Default.Inc("zim_device_cache_total", map[string]string{"result": "miss"})
	return append(devices, loaded...), nil
}

// Invalidate drops cached device lists after token registration/logout.
func (c *CachedDevices) Invalidate(ctx context.Context, ownerIDs ...int64) {
	if c == nil || c.client == nil || len(ownerIDs) == 0 {
		return
	}
	keys := make([]string, 0, len(ownerIDs))
	for _, uid := range ownerIDs {
		keys = append(keys, deviceKey(uid))
	}
	_ = c.client.Del(ctx, keys...).Err()
}
