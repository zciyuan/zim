package groupread

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestReportFlushAndCount(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	service := New(client)
	ctx := context.Background()
	service.Report(ctx, 1, 1, 10)
	service.Report(ctx, 1, 2, 20)
	service.Report(ctx, 1, 1, 5)
	count, err := service.Count(ctx, 1, 15)
	if err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	if count, err = service.Count(ctx, 1, 10); err != nil || count != 2 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	if count, err = service.Count(ctx, 2, 1); err != nil || count != 0 {
		t.Fatalf("empty count = %d, err = %v", count, err)
	}
}

func TestLifecycleAndInvalidReport(t *testing.T) {
	service := New(nil)
	if service.Name() != "group-read" {
		t.Fatalf("name = %s", service.Name())
	}
	service.Start()
	service.Close()
	service.Report(context.Background(), 1, 1, 1)
	service.Flush(context.Background())
}
