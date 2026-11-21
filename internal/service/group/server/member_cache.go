package server

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/localcache"
)

// MemberCache caches a group's member ID snapshot by version.
type MemberCache struct {
	client redis.UniversalClient
	l1     *localcache.Cache[string, []int64]
}

// NewMemberCache returns a versioned member cache.
func NewMemberCache(client redis.UniversalClient) *MemberCache {
	return &MemberCache{client: client, l1: localcache.New[string, []int64](5*time.Second, 10000)}
}

func (c *MemberCache) key(gid, version int64) string {
	return "zim:group:members:" + strconv.FormatInt(gid, 10) + ":" + strconv.FormatInt(version, 10)
}

// Get returns a member snapshot.
func (c *MemberCache) Get(ctx context.Context, gid, version int64) ([]int64, bool) {
	if c == nil || version <= 0 {
		return nil, false
	}
	key := c.key(gid, version)
	if ids, ok := c.l1.Get(key); ok {
		return ids, true
	}
	values, err := c.client.SMembers(ctx, key).Result()
	if err != nil || len(values) == 0 {
		return nil, false
	}
	ids := make([]int64, 0, len(values))
	for _, value := range values {
		id, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr == nil {
			ids = append(ids, id)
		}
	}
	c.l1.Set(key, ids)
	return ids, true
}

// Set stores a member snapshot for one version.
func (c *MemberCache) Set(ctx context.Context, gid, version int64, ids []int64) {
	if c == nil || version <= 0 || len(ids) == 0 {
		return
	}
	key := c.key(gid, version)
	values := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		values = append(values, id)
	}
	pipe := c.client.Pipeline()
	pipe.SAdd(ctx, key, values...)
	pipe.Expire(ctx, key, time.Hour)
	_, _ = pipe.Exec(ctx)
	c.l1.Set(key, ids)
}
