// Package seq implements per-conversation sequence allocation with a Redis
// block allocator and an in-process block cache.
package seq

import (
	"context"

	"github.com/dobyte/due/v2/errors"
)

// ErrInvalidBlock reports a non-positive block size.
var ErrInvalidBlock = errors.New("invalid sequence block")

// ErrExhausted reports a local block that cannot produce another sequence.
var ErrExhausted = errors.New("sequence block exhausted")

// New returns an allocator. block is the number of sequences reserved per Redis
// call (200 by default in ZIM).
func New(runner Runner, block int64) (*Allocator, error) {
	if runner == nil {
		return nil, errors.New("sequence runner missing")
	}
	if block <= 0 {
		return nil, ErrInvalidBlock
	}
	return &Allocator{runner: runner, block: block}, nil
}

// Next returns the next sequence for conversationID. It is safe for
// concurrent use: the fast path locks only the per-conversation block, and
// refills are serialized by refillMu so two goroutines cannot reserve
// overlapping blocks.
func (a *Allocator) Next(ctx context.Context, conversationID string) (int64, error) {
	if value, ok := a.local.Load(conversationID); ok {
		if next, ok := value.(*localBlock).next(); ok {
			return next, nil
		}
	}

	a.refillMu.Lock()
	defer a.refillMu.Unlock()

	// Another goroutine may have refilled while we waited.
	if value, ok := a.local.Load(conversationID); ok {
		block := value.(*localBlock)
		if next, ok := block.next(); ok {
			return next, nil
		}
		a.local.Delete(conversationID)
	}

	start, end, err := a.runner.Malloc(ctx, conversationID, a.block)
	if err != nil {
		return 0, err
	}
	block := &localBlock{curr: start - 1, last: end}
	a.local.Store(conversationID, block)
	next, _ := block.next()
	return next, nil
}

// next returns the next sequence and reports whether the block had capacity.
func (b *localBlock) next() (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.curr < b.last {
		b.curr++
		return b.curr, true
	}
	return 0, false
}

// Max returns the current maximum sequence.
func (a *Allocator) Max(ctx context.Context, conversationID string) (int64, error) {
	return a.runner.Max(ctx, conversationID)
}

// NextFloor allocates after raising the Redis floor to at least floor.
func (a *Allocator) NextFloor(ctx context.Context, conversationID string, floor int64) (int64, error) {
	if value, ok := a.local.Load(conversationID); ok {
		block := value.(*localBlock)
		if block.last < floor {
			a.local.Delete(conversationID)
		} else if next, ok := block.next(); ok {
			return next, nil
		}
	}

	a.refillMu.Lock()
	defer a.refillMu.Unlock()
	if value, ok := a.local.Load(conversationID); ok {
		block := value.(*localBlock)
		if block.last >= floor {
			if next, ok := block.next(); ok {
				return next, nil
			}
		}
		a.local.Delete(conversationID)
	}
	var start, end int64
	var err error
	if runner, ok := a.runner.(interface {
		MallocFloor(context.Context, string, int64, int64) (int64, int64, error)
	}); ok && floor > 0 {
		start, end, err = runner.MallocFloor(ctx, conversationID, a.block, floor)
	} else {
		start, end, err = a.runner.Malloc(ctx, conversationID, a.block)
	}
	if err != nil {
		return 0, err
	}
	block := &localBlock{curr: start - 1, last: end}
	a.local.Store(conversationID, block)
	next, _ := block.next()
	return next, nil
}
