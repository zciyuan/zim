// Package gateclean reclaims locations left behind by gateways that died
// without unbinding (ADR-0022).
package gateclean

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dobyte/due/v2/registry"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

const gateServiceName = "gate"

// udevReclaimScript removes the user-device member when the device has no live
// location left after the dead-gateway reclaim.
const udevReclaimScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return redis.call('SREM', KEYS[2], ARGV[1])
end
return 0
`

// reclaimScript deletes a location only when it still points at the dead
// gateway. A device that already rebound to a live gateway must be left alone.
const reclaimScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// New returns the reclaimer. Prefix defaults to due's "due:locate".
func New(client redis.UniversalClient, discovery registry.Discovery, prefix string) *Service {
	if prefix == "" {
		prefix = "due:locate"
	}
	return &Service{
		client:    client,
		discovery: discovery,
		prefix:    prefix,
		service:   gateServiceName,
		batch:     500,
	}
}

// Run performs one cleanup pass and returns what it reclaimed.
func (s *Service) Run(ctx context.Context) (Stats, error) {
	var stats Stats
	if s.client == nil || s.discovery == nil {
		return stats, nil
	}

	instances, err := s.discovery.Services(ctx, s.service)
	if err != nil {
		return stats, err
	}
	live := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		if instance.ID != "" {
			live[instance.ID] = struct{}{}
		}
	}
	stats.LiveGates = len(live)

	gateIDs, err := s.client.SMembers(ctx, s.idsKey()).Result()
	if err != nil {
		return stats, err
	}
	for _, gateID := range gateIDs {
		if _, ok := live[gateID]; ok {
			continue
		}
		stats.DeadGates++
		members, memberErr := s.client.SMembers(ctx, s.connsKey(gateID)).Result()
		if memberErr != nil {
			return stats, memberErr
		}
		deleted, skipped, reclaimErr := s.reclaim(ctx, gateID, members)
		if reclaimErr != nil {
			return stats, reclaimErr
		}
		if _, udevErr := s.reclaimUdev(ctx, members); udevErr != nil {
			return stats, udevErr
		}
		stats.Members += len(members)
		stats.Deleted += deleted
		stats.Skipped += skipped

		pipeline := s.client.Pipeline()
		pipeline.Del(ctx, s.connsKey(gateID))
		pipeline.SRem(ctx, s.idsKey(), gateID)
		if _, err := pipeline.Exec(ctx); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *Service) reclaim(ctx context.Context, gateID string, members []string) (int, int, error) {
	var deleted, skipped int
	for start := 0; start < len(members); start += s.batch {
		end := start + s.batch
		if end > len(members) {
			end = len(members)
		}
		chunk := members[start:end]
		pipeline := s.client.Pipeline()
		commands := make([]*redis.Cmd, len(chunk))
		for i, deviceKey := range chunk {
			commands[i] = pipeline.Eval(ctx, reclaimScript, []string{s.locateKey(deviceKey)}, gateID)
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return deleted, skipped, err
		}
		for _, command := range commands {
			if value, ok := command.Val().(int64); ok && value > 0 {
				deleted++
				continue
			}
			skipped++
		}
	}
	return deleted, skipped, nil
}

func (s *Service) idsKey() string {
	return s.prefix + ":gate:ids"
}

func (s *Service) connsKey(gateID string) string {
	return fmt.Sprintf("%s:gate:%s:conns", s.prefix, gateID)
}

func (s *Service) locateKey(deviceKey string) string {
	return fmt.Sprintf("%s:locate:user:%s:gate", s.prefix, deviceKey)
}

// reclaimUdev removes devices whose location no longer exists after a gate
// reclaim. Devices that rebound to a live gate are preserved.
func (s *Service) reclaimUdev(ctx context.Context, members []string) (int, error) {
	removed := 0
	for start := 0; start < len(members); start += s.batch {
		end := start + s.batch
		if end > len(members) {
			end = len(members)
		}
		chunk := members[start:end]
		pipeline := s.client.Pipeline()
		commands := make([]*redis.Cmd, 0, len(chunk))
		for _, raw := range chunk {
			deviceKey, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				continue
			}
			uid, _, parseErr := multi.Parse(deviceKey)
			if parseErr != nil {
				continue
			}
			commands = append(commands, pipeline.Eval(ctx, udevReclaimScript,
				[]string{s.locateKey(raw), s.udevKey(uid)}, raw))
		}
		if len(commands) == 0 {
			continue
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return removed, err
		}
		for _, command := range commands {
			if value, ok := command.Val().(int64); ok && value > 0 {
				removed++
			}
		}
	}
	return removed, nil
}

func (s *Service) udevKey(uid int64) string {
	return fmt.Sprintf("%s:udev:%d", s.prefix, uid)
}
