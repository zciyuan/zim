package ratelimit

import (
	"testing"
	"time"
)

func TestLimiter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := New(2, 2)
	limiter.now = func() time.Time { return now }
	if !limiter.Allow("a") {
		t.Fatal("first request must pass")
	}
	if !limiter.Allow("a") {
		t.Fatal("burst should be allowed")
	}
	if limiter.Allow("a") {
		t.Fatal("third request must be limited")
	}
	if !limiter.Allow("b") {
		t.Fatal("other key must be independent")
	}
	now = now.Add(time.Second)
	if !limiter.Allow("a") {
		t.Fatal("token must refill")
	}
}
