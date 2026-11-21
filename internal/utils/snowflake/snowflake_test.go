package snowflake

import (
	"sync"
	"testing"
	"time"
)

func TestNextUnique(t *testing.T) {
	generator := NewWithWorker(3)
	var wg sync.WaitGroup
	seen := make(chan int64, 1000)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				seen <- generator.Next()
			}
		}()
	}
	wg.Wait()
	close(seen)
	ids := make(map[int64]struct{}, 1000)
	for id := range seen {
		if id <= 0 {
			t.Fatalf("non-positive ID: %d", id)
		}
		if _, ok := ids[id]; ok {
			t.Fatalf("duplicate ID: %d", id)
		}
		ids[id] = struct{}{}
	}
}

func TestMonotonicWithClock(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	generator := NewWithClock(1, func() time.Time { return now })
	first := generator.Next()
	second := generator.Next()
	if second <= first {
		t.Fatalf("IDs not monotonic: %d, %d", first, second)
	}
}

func TestWorkerClamp(t *testing.T) {
	if got := NewWithWorker(-1).worker; got != 0 {
		t.Fatalf("negative worker = %d", got)
	}
	if got := NewWithWorker(5000).worker; got != 0 {
		t.Fatalf("oversize worker = %d", got)
	}
}
