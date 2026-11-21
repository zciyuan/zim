package server

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMemberCacheRoundTrip(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	cache := NewMemberCache(client)
	ctx := context.Background()
	cache.Set(ctx, 42, 7, []int64{1, 2, 3})
	ids, ok := cache.Get(ctx, 42, 7)
	if !ok || len(ids) != 3 {
		t.Fatalf("ids = %v, ok = %v", ids, ok)
	}
	if _, ok = cache.Get(ctx, 42, 8); ok {
		t.Fatal("different version must miss")
	}
}
