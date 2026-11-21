package idem

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisStore(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	store := NewRedisStore(client, time.Minute)
	ctx := context.Background()

	existing, acquired, err := store.Reserve(ctx, 1, "c1")
	if err != nil || !acquired || existing != nil {
		t.Fatalf("first = %+v, %v, %v", existing, acquired, err)
	}
	if _, _, err = store.Reserve(ctx, 1, "c1"); err != ErrPending {
		t.Fatalf("pending err = %v", err)
	}
	if err = store.Complete(ctx, 1, "c1", &Result{ServerMsgID: "s1", Seq: 7}); err != nil {
		t.Fatal(err)
	}
	existing, acquired, err = store.Reserve(ctx, 1, "c1")
	if err != nil || acquired || existing == nil || existing.Seq != 7 || existing.ServerMsgID != "s1" {
		t.Fatalf("duplicate = %+v, %v, %v", existing, acquired, err)
	}
	if err = store.Release(ctx, 1, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err = store.Reserve(ctx, 1, "c1"); err != nil || !acquired {
		t.Fatalf("after release acquired = %v, err = %v", acquired, err)
	}
}

func TestKeyAndDefaultTTL(t *testing.T) {
	store := NewRedisStore(nil, 0)
	if store.ttl != 10*time.Minute {
		t.Fatalf("ttl = %v", store.ttl)
	}
	if key(1, "c") != "zim:idem:1:c" {
		t.Fatalf("key = %s", key(1, "c"))
	}
}

func TestMalformedValue(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	store := NewRedisStore(client, time.Minute)
	ctx := context.Background()
	if err := client.Set(ctx, key(1, "bad"), "no-colon", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reserve(ctx, 1, "bad"); err == nil {
		t.Fatal("malformed value must fail")
	}
	if err := client.Set(ctx, key(1, "bad-seq"), "s:not-a-number", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reserve(ctx, 1, "bad-seq"); err == nil {
		t.Fatal("malformed seq must fail")
	}
}
