package localcache

import (
	"testing"
	"time"
)

func TestCacheTTLAndCapacity(t *testing.T) {
	cache := New[int, string](10*time.Millisecond, 2)
	cache.Set(1, "a")
	if value, ok := cache.Get(1); !ok || value != "a" {
		t.Fatalf("value = %q ok = %v", value, ok)
	}
	time.Sleep(15 * time.Millisecond)
	if _, ok := cache.Get(1); ok {
		t.Fatal("expired value returned")
	}
	cache.Set(2, "b")
	cache.Set(3, "c")
	cache.Set(4, "d")
	if cache.Len() > 2 {
		t.Fatalf("len = %d", cache.Len())
	}
}
