package conversation

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisReadCache returns a read_seq cache with a 30-day TTL.
func NewRedisReadCache(client redis.UniversalClient, ttl time.Duration) *RedisReadCache {
	if ttl <= 0 {
		ttl = 30 * 24 * time.Hour
	}
	return &RedisReadCache{client: client, ttl: ttl}
}

// Get returns the cached read sequence and whether it was present.
func (c *RedisReadCache) Get(ctx context.Context, conversationID string, uid int64) (int64, bool, error) {
	value, err := c.client.Get(ctx, readSeqKey(conversationID, uid)).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, false, nil
		}
		return 0, false, err
	}
	return value, true, nil
}

// Set stores the read sequence.
func (c *RedisReadCache) Set(ctx context.Context, conversationID string, uid, seq int64) error {
	return c.client.Set(ctx, readSeqKey(conversationID, uid), seq, c.ttl).Err()
}

func readSeqKey(conversationID string, uid int64) string {
	return "zim:readseq:" + conversationID + ":" + strconv.FormatInt(uid, 10)
}

// BatchReadCache optionally loads read_seq for many conversations in one call.
type BatchReadCache interface {
	GetMany(ctx context.Context, conversationIDs []string, uid int64) (map[string]int64, error)
}

// GetMany loads read_seq for a conversation batch with one MGET.
func (c *RedisReadCache) GetMany(ctx context.Context, conversationIDs []string, uid int64) (map[string]int64, error) {
	result := make(map[string]int64, len(conversationIDs))
	if len(conversationIDs) == 0 {
		return result, nil
	}
	keys := make([]string, 0, len(conversationIDs))
	for _, conversationID := range conversationIDs {
		keys = append(keys, readSeqKey(conversationID, uid))
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for index, value := range values {
		text, ok := value.(string)
		if !ok || text == "" {
			continue
		}
		seq, parseErr := strconv.ParseInt(text, 10, 64)
		if parseErr == nil {
			result[conversationIDs[index]] = seq
		}
	}
	return result, nil
}
