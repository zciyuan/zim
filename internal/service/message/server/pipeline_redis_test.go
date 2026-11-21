package message

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisResultQueue(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	queue := NewRedisResultQueue(client, time.Minute)
	ctx := context.Background()
	result := &SendResult{ServerMsgID: "s1", Seq: 7}
	if err := queue.Publish(ctx, 1, "c1", result); err != nil {
		t.Fatal(err)
	}
	got, err := queue.Wait(ctx, 1, "c1", time.Second)
	if err != nil || got.Seq != 7 || got.ServerMsgID != "s1" {
		t.Fatalf("got = %+v, err = %v", got, err)
	}
	if _, err = queue.Wait(ctx, 1, "missing", 10*time.Millisecond); err == nil {
		t.Fatal("timeout must fail")
	}
}
