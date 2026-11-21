// Package localcache is a bounded TTL cache for L1 hot data.
package localcache

import (
	"sync"
	"time"
)

type item[V any] struct {
	value  V
	expire int64
}

// Cache is a sharded-by-key TTL cache. It intentionally has no eviction
// callback; callers keep values small and set a hard capacity.
type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]item[V]
	ttl   time.Duration
	max   int
}

// New returns a local cache. A non-positive ttl defaults to one second.
func New[K comparable, V any](ttl time.Duration, max int) *Cache[K, V] {
	if ttl <= 0 {
		ttl = time.Second
	}
	if max <= 0 {
		max = 10000
	}
	return &Cache[K, V]{items: make(map[K]item[V]), ttl: ttl, max: max}
}

// Get returns a live value.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || entry.expire <= time.Now().UnixNano() {
		return entry.value, false
	}
	return entry.value, true
}

// Set stores a value, clearing the map when the capacity is exceeded.
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= c.max {
		now := time.Now().UnixNano()
		for current, entry := range c.items {
			if entry.expire <= now {
				delete(c.items, current)
			}
		}
		if len(c.items) >= c.max {
			c.items = make(map[K]item[V])
		}
	}
	c.items[key] = item[V]{value: value, expire: time.Now().Add(c.ttl).UnixNano()}
}

// Delete removes one key.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Len returns the number of live and stale entries.
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
