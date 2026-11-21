package seq

import (
	"context"
	"sync"
)

type fakeRunner struct {
	mu    sync.Mutex
	last  map[string]int64
	calls int
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{last: make(map[string]int64)}
}

func (f *fakeRunner) Malloc(_ context.Context, conversationID string, size int64) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	start := f.last[conversationID] + 1
	end := start + size - 1
	f.last[conversationID] = end
	return start, end, nil
}

func (f *fakeRunner) Max(_ context.Context, conversationID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[conversationID], nil
}
