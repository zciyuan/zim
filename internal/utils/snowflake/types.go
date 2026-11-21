package snowflake

import (
	"sync"
	"time"
)

// Generator is a concurrency-safe snowflake generator.
type Generator struct {
	mu       sync.Mutex
	worker   int64
	lastTime int64
	sequence int64
	now      func() time.Time
}
