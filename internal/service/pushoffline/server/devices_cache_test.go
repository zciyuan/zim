package pushoffline

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
)

type cacheDeviceSource struct{ calls int }

func (s *cacheDeviceSource) Targets(context.Context, []int64) ([]*model.Device, error) {
	s.calls++
	return []*model.Device{{OwnerID: 1, PushToken: "t", Status: 0}}, nil
}

func TestCachedDevicesReadsEachUIDOnce(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	source := &cacheDeviceSource{}
	cache := NewCachedDevices(source, client, time.Minute)
	for i := 0; i < 3; i++ {
		devices, err := cache.Targets(context.Background(), []int64{1})
		if err != nil || len(devices) != 1 {
			t.Fatalf("devices = %d err = %v", len(devices), err)
		}
	}
	if source.calls != 1 {
		t.Fatalf("source calls = %d, want 1", source.calls)
	}
	cache.Invalidate(context.Background(), 1)
	if _, err := cache.Targets(context.Background(), []int64{1}); err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 {
		t.Fatalf("source calls after invalidate = %d, want 2", source.calls)
	}
}
