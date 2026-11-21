// Package idem implements the Redis fast-path deduplication for messages.
package idem

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"
)

// ErrPending reports an in-flight duplicate reservation.
var ErrPending = errors.New("idempotency reservation pending")

var _ Store = (*RedisStore)(nil)

// NewRedisStore returns a Redis store with the 10-minute message window.
func NewRedisStore(client redis.UniversalClient, ttl time.Duration) *RedisStore {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &RedisStore{client: client, ttl: ttl}
}

// Reserve implements Store.
func (s *RedisStore) Reserve(ctx context.Context, uid int64, clientMsgID string) (*Result, bool, error) {
	key := key(uid, clientMsgID)
	ok, err := s.client.SetNX(ctx, key, "pending", s.ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if ok {
		return nil, true, nil
	}
	value, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return s.Reserve(ctx, uid, clientMsgID)
		}
		return nil, false, err
	}
	if value == "pending" {
		return nil, false, ErrPending
	}
	serverMsgID, seq, ok := strings.Cut(value, ":")
	if !ok {
		return nil, false, errors.New("invalid idempotency value")
	}
	parsed, err := parseInt(seq)
	if err != nil {
		return nil, false, err
	}
	return &Result{ServerMsgID: serverMsgID, Seq: parsed}, false, nil
}

// Complete implements Store.
func (s *RedisStore) Complete(ctx context.Context, uid int64, clientMsgID string, result *Result) error {
	return s.client.Set(ctx, key(uid, clientMsgID), result.ServerMsgID+":"+formatInt(result.Seq), s.ttl).Err()
}

// Release implements Store.
func (s *RedisStore) Release(ctx context.Context, uid int64, clientMsgID string) error {
	return s.client.Del(ctx, key(uid, clientMsgID)).Err()
}

func key(uid int64, clientMsgID string) string {
	return "zim:idem:" + formatInt(uid) + ":" + clientMsgID
}

func parseInt(raw string) (int64, error) { return strconv.ParseInt(raw, 10, 64) }
func formatInt(value int64) string       { return strconv.FormatInt(value, 10) }
