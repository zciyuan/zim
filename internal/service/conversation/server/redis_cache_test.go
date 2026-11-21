package conversation

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisReadCache(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	cache := NewRedisReadCache(client, time.Minute)
	ctx := context.Background()

	if _, ok, err := cache.Get(ctx, "s_1_2", 1); err != nil || ok {
		t.Fatalf("miss: ok=%v err=%v", ok, err)
	}
	if err := cache.Set(ctx, "s_1_2", 1, 9); err != nil {
		t.Fatal(err)
	}
	value, ok, err := cache.Get(ctx, "s_1_2", 1)
	if err != nil || !ok || value != 9 {
		t.Fatalf("value=%d ok=%v err=%v", value, ok, err)
	}
}
