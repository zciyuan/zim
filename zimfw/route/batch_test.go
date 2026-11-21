package route

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

func newBatchLocator(t *testing.T) (*RedisBatchLocator, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	locator, err := NewRedisBatchLocator(client, "zim")
	if err != nil {
		t.Fatal(err)
	}
	return locator, server
}

func TestLocateDeviceKeys(t *testing.T) {
	locator, server := newBatchLocator(t)
	online := multi.Key(10001, 2, 1)
	offline := multi.Key(10001, 3, 1)
	if err := server.Set("zim:locate:user:"+strconv.FormatInt(online, 10)+":gate", "gate-a"); err != nil {
		t.Fatal(err)
	}

	found, err := locator.LocateDeviceKeys(context.Background(), []int64{online, offline})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[online] != "gate-a" {
		t.Fatalf("found = %v", found)
	}
	if _, ok := found[offline]; ok {
		t.Fatalf("offline key must be omitted: %v", found)
	}
}

func TestLocateDeviceKeysEmpty(t *testing.T) {
	locator, _ := newBatchLocator(t)
	found, err := locator.LocateDeviceKeys(context.Background(), nil)
	if err != nil || len(found) != 0 {
		t.Fatalf("found = %v, err = %v", found, err)
	}
}

func TestNewRedisBatchLocatorRequiresClient(t *testing.T) {
	if _, err := NewRedisBatchLocator(nil, "zim"); err == nil {
		t.Fatal("nil client must be rejected")
	}
}

func TestNewRedisBatchLocatorDefaultPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	locator, err := NewRedisBatchLocator(client, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Set("due:locate:locate:user:1:gate", "g"); err != nil {
		t.Fatal(err)
	}
	found, err := locator.LocateDeviceKeys(context.Background(), []int64{1})
	if err != nil || found[1] != "g" {
		t.Fatalf("found = %v, err = %v", found, err)
	}
}
