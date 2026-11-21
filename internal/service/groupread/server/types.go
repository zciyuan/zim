package groupread

import (
	"sync"

	"github.com/redis/go-redis/v9"
)

// Service coalesces group read receipts and flushes them every 5 seconds.
type Service struct {
	client redis.UniversalClient
	ttl    int64

	mu      sync.Mutex
	pending map[int64]map[int64]int64
	stop    chan struct{}
	start   sync.Once
	close   sync.Once
}
