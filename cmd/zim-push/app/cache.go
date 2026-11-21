package app

import (
	"container/list"
	"time"
)

// newRouteCache returns a bounded LRU route cache.
func newRouteCache(ttl time.Duration, max int) *routeCache {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if max <= 0 {
		max = 1
	}
	return &routeCache{
		ttl:     ttl,
		max:     max,
		now:     time.Now,
		entries: make(map[int64]*routeEntry, max),
		order:   list.New(),
	}
}

// Get returns the cached gate ID for a device key.
func (c *routeCache) Get(deviceKey int64) (string, bool) {
	c.mu.Lock()
	entry, ok := c.entries[deviceKey]
	if !ok {
		c.mu.Unlock()
		return "", false
	}
	if c.now().After(entry.expireAt) {
		c.remove(entry)
		c.mu.Unlock()
		return "", false
	}
	c.order.MoveToFront(entry.element)
	gateID := entry.gateID
	c.mu.Unlock()
	return gateID, true
}

// Put stores a deviceKey -> gateID mapping, refreshing its TTL.
func (c *routeCache) Put(deviceKey int64, gateID string) {
	if gateID == "" {
		return
	}
	c.mu.Lock()
	if entry, ok := c.entries[deviceKey]; ok {
		entry.gateID = gateID
		entry.expireAt = c.now().Add(c.ttl)
		c.order.MoveToFront(entry.element)
		c.mu.Unlock()
		return
	}
	entry := &routeEntry{
		key:      deviceKey,
		gateID:   gateID,
		expireAt: c.now().Add(c.ttl),
	}
	entry.element = c.order.PushFront(entry)
	c.entries[deviceKey] = entry
	for len(c.entries) > c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.remove(oldest.Value.(*routeEntry))
	}
	c.mu.Unlock()
}

// Delete drops a stale mapping after a delivery failure.
func (c *routeCache) Delete(deviceKey int64) {
	c.mu.Lock()
	if entry, ok := c.entries[deviceKey]; ok {
		c.remove(entry)
	}
	c.mu.Unlock()
}

// Len reports the number of cached entries.
func (c *routeCache) Len() int {
	c.mu.Lock()
	length := len(c.entries)
	c.mu.Unlock()
	return length
}

func (c *routeCache) remove(entry *routeEntry) {
	c.order.Remove(entry.element)
	delete(c.entries, entry.key)
}
