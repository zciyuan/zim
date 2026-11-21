package seq

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Runner allocates sequence blocks and reads the current maximum.
type Runner interface {
	// Malloc reserves size sequence numbers and returns the inclusive block.
	Malloc(ctx context.Context, conversationID string, size int64) (start, end int64, err error)
	// Max returns the last allocated sequence, or zero when none exists.
	Max(ctx context.Context, conversationID string) (int64, error)
}

// Allocator hands out per-conversation sequences from local blocks.
type Allocator struct {
	runner   Runner
	block    int64
	local    sync.Map
	refillMu sync.Mutex
}

type localBlock struct {
	mu   sync.Mutex
	curr int64
	last int64
}

type redisRunner struct {
	client redis.UniversalClient
	malloc *redis.Script
	max    *redis.Script
	floor  *redis.Script
}
