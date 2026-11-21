package idem

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result is the persisted identity of a message.
type Result struct {
	ServerMsgID string
	Seq         int64
}

// Store reserves and completes message idempotency keys.
type Store interface {
	// Reserve returns an existing result when the key was completed, or
	// acquired=true when the caller owns the key.
	Reserve(ctx context.Context, uid int64, clientMsgID string) (*Result, bool, error)
	// Complete stores the result under the reserved key.
	Complete(ctx context.Context, uid int64, clientMsgID string, result *Result) error
	// Release removes a reservation after a failed write.
	Release(ctx context.Context, uid int64, clientMsgID string) error
}

// RedisStore is the Redis-backed Store implementation.
type RedisStore struct {
	client redis.UniversalClient
	ttl    time.Duration
}
