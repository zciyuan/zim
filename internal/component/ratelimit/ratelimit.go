// Package ratelimit provides a small per-key token bucket for storm control.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a concurrency-safe token bucket map.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New returns a limiter with per-second rate and burst capacity.
func New(rate, burst int) *Limiter {
	if rate <= 0 {
		rate = 20
	}
	if burst <= 0 {
		burst = rate
	}
	return &Limiter{rate: float64(rate), burst: float64(burst),
		buckets: make(map[string]*bucket), now: time.Now}
}

// Allow consumes one token for key.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	entry := l.buckets[key]
	if entry == nil {
		entry = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = entry
	}
	elapsed := now.Sub(entry.last).Seconds()
	entry.tokens += elapsed * l.rate
	if entry.tokens > l.burst {
		entry.tokens = l.burst
	}
	entry.last = now
	if entry.tokens < 1 {
		return false
	}
	entry.tokens--
	return true
}
