package app

import (
	"sync"
	"testing"
	"time"
)

func TestRouteCachePutGet(t *testing.T) {
	cache := newRouteCache(time.Minute, 10)
	cache.Put(1, "g1")
	if gateID, ok := cache.Get(1); !ok || gateID != "g1" {
		t.Fatalf("get = %q, %v", gateID, ok)
	}
	if _, ok := cache.Get(2); ok {
		t.Fatal("missing key must miss")
	}
}

func TestRouteCacheTTL(t *testing.T) {
	cache := newRouteCache(time.Second, 10)
	now := time.Unix(0, 0)
	cache.now = func() time.Time { return now }

	cache.Put(1, "g1")
	now = now.Add(500 * time.Millisecond)
	if _, ok := cache.Get(1); !ok {
		t.Fatal("entry must be alive before TTL")
	}
	now = now.Add(600 * time.Millisecond)
	if _, ok := cache.Get(1); ok {
		t.Fatal("entry must expire after TTL")
	}
}

func TestRouteCacheEvictsOldest(t *testing.T) {
	cache := newRouteCache(time.Minute, 2)
	cache.Put(1, "g1")
	cache.Put(2, "g2")
	if _, ok := cache.Get(1); !ok {
		t.Fatal("first entry must exist")
	}
	cache.Put(3, "g3")
	if cache.Len() != 2 {
		t.Fatalf("len = %d", cache.Len())
	}
	if _, ok := cache.Get(2); ok {
		t.Fatal("least recently used entry must be evicted")
	}
	if _, ok := cache.Get(1); !ok {
		t.Fatal("recently used entry must survive")
	}
}

func TestRouteCacheDelete(t *testing.T) {
	cache := newRouteCache(time.Minute, 2)
	cache.Put(1, "g1")
	cache.Delete(1)
	if _, ok := cache.Get(1); ok {
		t.Fatal("deleted entry must miss")
	}
	cache.Delete(99)
}

func TestNewRouteCacheDefaults(t *testing.T) {
	cache := newRouteCache(0, 0)
	if cache.ttl != 5*time.Second || cache.max != 1 {
		t.Fatalf("ttl = %s, max = %d", cache.ttl, cache.max)
	}
}

func TestRouteCacheConcurrent(t *testing.T) {
	cache := newRouteCache(time.Minute, 100)
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for i := 0; i < 1000; i++ {
				key := int64(worker*1000 + i)
				cache.Put(key, "g")
				_, _ = cache.Get(key)
			}
		}(worker)
	}
	waitGroup.Wait()
	if cache.Len() > 100 {
		t.Fatalf("len = %d", cache.Len())
	}
}
