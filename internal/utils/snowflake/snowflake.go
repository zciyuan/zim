// Package snowflake generates ZIM int64 IDs. The layout is 41 bits of
// milliseconds since 2024-01-01, 10 bits of worker ID and 12 bits of sequence.
package snowflake

import (
	"time"

	"github.com/dobyte/due/v2/etc"
)

const (
	epoch        int64 = 1704067200000 // 2024-01-01T00:00:00Z in milliseconds
	workerBits         = 10
	sequenceBits       = 12
	maxWorker          = 1<<workerBits - 1
	maxSequence        = 1<<sequenceBits - 1
	workerShift        = sequenceBits
	timeShift          = sequenceBits + workerBits
)

// Generator is a concurrency-safe snowflake generator.

// New returns a generator configured by etc.zim.snowflake.workerId.
func New() *Generator {
	return NewWithWorker(int64(etc.Get("etc.zim.snowflake.workerId", 1).Int()))
}

// NewWithWorker returns a generator for a worker in [0, 1023].
func NewWithWorker(worker int64) *Generator {
	if worker < 0 || worker > maxWorker {
		worker = 0
	}
	return &Generator{worker: worker, now: time.Now}
}

// NewWithClock is the deterministic test seam.
func NewWithClock(worker int64, now func() time.Time) *Generator {
	generator := NewWithWorker(worker)
	if now != nil {
		generator.now = now
	}
	return generator
}

// Next returns the next unique ID.
func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now().UnixMilli()
	if now < g.lastTime {
		now = g.lastTime
	}
	if now == g.lastTime {
		g.sequence = (g.sequence + 1) & maxSequence
		if g.sequence == 0 {
			for now <= g.lastTime {
				now = g.now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}
	g.lastTime = now
	return (now-epoch)<<timeShift | g.worker<<workerShift | g.sequence
}
