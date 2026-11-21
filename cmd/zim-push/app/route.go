package app

import (
	"context"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// buildTargets expands recipients into their persisted device keys.
//
// The sender is included on purpose: its other online devices need the
// authoritative seq echo required by ZIM-02 G7, and clients dedupe by
// server_msg_id.
func (a *App) buildTargets(ctx context.Context, recipients []int64) []int64 {
	if a.devices != nil {
		if keys, err := a.devices.Keys(ctx, recipients); err == nil {
			return keys
		}
	}
	return buildFallbackTargets(recipients)
}

func buildFallbackTargets(recipients []int64) []int64 {
	targets := make([]int64, 0, len(recipients)*int(multi.PlatformPad))
	for _, uid := range recipients {
		for platform := int32(1); platform <= multi.PlatformPad; platform++ {
			if deviceKey := multi.Key(uid, platform, 1); deviceKey != 0 {
				targets = append(targets, deviceKey)
			}
		}
	}
	return targets
}

// resolveRoutes turns device keys into gate buckets, using the L1 cache first
// and one pipelined batch lookup per chunk of misses.
//
// Offline keys are deliberately not cached: they must be re-checked on the next
// message, otherwise a reconnecting client would stay "offline" in the cache.
func (a *App) resolveRoutes(ctx context.Context, targets []int64) (map[string][]int64, int) {
	buckets := make(map[string][]int64)
	if len(targets) == 0 {
		return buckets, 0
	}

	online := 0
	hits := 0
	misses := make([]int64, 0, len(targets))
	for _, deviceKey := range targets {
		if gateID, ok := a.cache.Get(deviceKey); ok {
			buckets[gateID] = append(buckets[gateID], deviceKey)
			online++
			hits++
			continue
		}
		misses = append(misses, deviceKey)
	}
	// Counters are aggregated once per message: per-key metrics would add a
	// mutex + atomic per device key (millions per second at peak).
	metrics.Default.Add("zim_push_route_total", map[string]string{"result": "hit"}, int64(hits))
	metrics.Default.Add("zim_push_route_total", map[string]string{"result": "miss"}, int64(len(misses)))

	if a.locator == nil || len(misses) == 0 {
		return buckets, online
	}

	var locateErrors int64
	for start := 0; start < len(misses); start += a.locateBatchSize {
		end := start + a.locateBatchSize
		if end > len(misses) {
			end = len(misses)
		}
		chunk := misses[start:end]
		found, err := a.locator.LocateDeviceKeys(ctx, chunk)
		if err != nil {
			locateErrors += int64(len(chunk))
			log.Warnf("batch locate failed: keys = %d, err = %v", len(chunk), err)
			continue
		}
		for _, deviceKey := range chunk {
			gateID := found[deviceKey]
			if gateID == "" {
				continue
			}
			a.cache.Put(deviceKey, gateID)
			buckets[gateID] = append(buckets[gateID], deviceKey)
			online++
		}
	}
	if locateErrors > 0 {
		metrics.Default.Add("zim_push_route_total", map[string]string{"result": "error"}, locateErrors)
	}
	return buckets, online
}
