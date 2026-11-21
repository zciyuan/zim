package route

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

func TestRedisDeviceIndex(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	index := NewRedisDeviceIndex(client, "zim", nil)
	ctx := context.Background()
	keyA := multi.Key(10001, multi.PlatformWeb, 1)
	keyB := multi.Key(10001, multi.PlatformWeb, 2)
	if err := index.AddUdev(ctx, 10001, keyA); err != nil {
		t.Fatal(err)
	}
	if err := index.AddUdev(ctx, 10001, keyB); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Keys(ctx, []int64{10001})
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys = %v, err = %v", keys, err)
	}
	if err = index.RemoveUdev(ctx, 10001, keyA); err != nil {
		t.Fatal(err)
	}
	keys, err = index.Keys(ctx, []int64{10001})
	if err != nil || len(keys) != 1 || keys[0] != keyB {
		t.Fatalf("keys after remove = %v, err = %v", keys, err)
	}
}

func TestRedisDeviceIndexFallback(t *testing.T) {
	index := NewRedisDeviceIndex(nil, "zim", fakeDeviceSource{keys: []int64{7}})
	keys, err := index.Keys(context.Background(), []int64{1})
	if err != nil || len(keys) != 1 || keys[0] != 7 {
		t.Fatalf("fallback = %v, err = %v", keys, err)
	}
}

type fakeDeviceSource struct{ keys []int64 }

func (f fakeDeviceSource) Keys(context.Context, []int64) ([]int64, error) {
	return f.keys, nil
}

func TestRedisDeviceIndexL1Cache(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	index := NewRedisDeviceIndex(client, "zim", nil)
	index.l1TTL = 10 * time.Millisecond
	ctx := context.Background()
	keyA := multi.Key(20001, multi.PlatformWeb, 1)
	if err := index.AddUdev(ctx, 20001, keyA); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Keys(ctx, []int64{20001}); err != nil {
		t.Fatal(err)
	}
	keyB := multi.Key(20001, multi.PlatformWeb, 2)
	if err := client.SAdd(ctx, "zim:udev:20001", keyB).Err(); err != nil {
		t.Fatal(err)
	}
	keys, err := index.Keys(ctx, []int64{20001})
	if err != nil || len(keys) != 1 {
		t.Fatalf("cached keys = %v, err = %v", keys, err)
	}
	time.Sleep(15 * time.Millisecond)
	keys, err = index.Keys(ctx, []int64{20001})
	if err != nil || len(keys) != 2 {
		t.Fatalf("refreshed keys = %v, err = %v", keys, err)
	}
}
