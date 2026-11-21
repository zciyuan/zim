package seq

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisRunner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	runner := NewRedisRunner(client)
	start, end, err := runner.Malloc(context.Background(), "s_1_2", 200)
	if err != nil {
		t.Fatal(err)
	}
	if start != 1 || end != 200 {
		t.Fatalf("block = %d..%d", start, end)
	}
	max, err := runner.Max(context.Background(), "s_1_2")
	if err != nil || max != 200 {
		t.Fatalf("max = %d, err = %v", max, err)
	}
	start, end, err = runner.Malloc(context.Background(), "s_1_2", 10)
	if err != nil || start != 201 || end != 210 {
		t.Fatalf("next block = %d..%d, err = %v", start, end, err)
	}
}

func TestRedisRunnerEmptyMax(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	max, err := NewRedisRunner(client).Max(context.Background(), "missing")
	if err != nil || max != 0 {
		t.Fatalf("max = %d, err = %v", max, err)
	}
}
