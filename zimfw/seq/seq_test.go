package seq

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestNextStrictlyIncrements(t *testing.T) {
	allocator, err := New(newFakeRunner(), 200)
	if err != nil {
		t.Fatal(err)
	}
	last := int64(0)
	for i := 0; i < 1000; i++ {
		next, nextErr := allocator.Next(context.Background(), "s_1_2")
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if next != last+1 {
			t.Fatalf("seq = %d, want %d", next, last+1)
		}
		last = next
	}
}

func TestAllocatorConcurrent(t *testing.T) {
	const (
		goroutines = 100
		perRoutine = 200
		total      = goroutines * perRoutine
	)
	allocator, err := New(newFakeRunner(), 200)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan int64, total)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perRoutine; j++ {
				next, nextErr := allocator.Next(context.Background(), "s_1_2")
				if nextErr != nil {
					t.Error(nextErr)
					return
				}
				results <- next
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[int64]struct{}, total)
	for next := range results {
		if _, ok := seen[next]; ok {
			t.Fatalf("duplicate seq %d", next)
		}
		seen[next] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("unique seqs = %d, want %d", len(seen), total)
	}
	for i := int64(1); i <= total; i++ {
		if _, ok := seen[i]; !ok {
			t.Fatalf("missing seq %d", i)
		}
	}
}

func TestBlocksAreReused(t *testing.T) {
	runner := newFakeRunner()
	allocator, _ := New(runner, 10)
	for i := 0; i < 10; i++ {
		if _, err := allocator.Next(context.Background(), "g_1"); err != nil {
			t.Fatal(err)
		}
	}
	if runner.calls != 1 {
		t.Fatalf("malloc calls = %d, want 1", runner.calls)
	}
	if _, err := allocator.Next(context.Background(), "g_1"); err != nil {
		t.Fatal(err)
	}
	if runner.calls != 2 {
		t.Fatalf("malloc calls = %d, want 2", runner.calls)
	}
}

func TestMaxAndValidation(t *testing.T) {
	allocator, err := New(newFakeRunner(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = allocator.Next(context.Background(), "g_2"); err != nil {
		t.Fatal(err)
	}
	max, err := allocator.Max(context.Background(), "g_2")
	if err != nil || max != 5 {
		t.Fatalf("max = %d, err = %v", max, err)
	}
	if _, err = New(nil, 1); err == nil {
		t.Fatal("nil runner must fail")
	}
	if _, err = New(newFakeRunner(), 0); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("block err = %v", err)
	}
}
