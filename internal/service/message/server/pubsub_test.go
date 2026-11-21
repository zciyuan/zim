package message

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPubSubResultQueueWaitWithPublish(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	queue := NewPubSubResultQueue(client, time.Minute)
	defer queue.Close()
	publisher := NewRedisResultQueue(client, time.Minute)

	result, err := queue.WaitWithPublish(context.Background(), 42, "c42", time.Second,
		func(ctx context.Context) error {
			return publisher.Publish(ctx, 42, "c42", &SendResult{ServerMsgID: "s42", Seq: 8})
		})
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.ServerMsgID != "s42" || result.Seq != 8 {
		t.Fatalf("result = %+v", result)
	}
}

func TestPubSubResultQueueFallsBackToList(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	queue := NewPubSubResultQueue(client, time.Minute)
	defer queue.Close()
	publisher := NewRedisResultQueue(client, time.Minute)
	// Simulate a lost subscription while transfer still persisted the result.
	_ = queue.sub.Close()
	if err := publisher.Publish(context.Background(), 43, "c43", &SendResult{ServerMsgID: "s43", Seq: 9}); err != nil {
		t.Fatal(err)
	}
	result, err := queue.WaitWithPublish(context.Background(), 43, "c43", 2*time.Second,
		func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("fallback wait: %v", err)
	}
	if result.ServerMsgID != "s43" || result.Seq != 9 {
		t.Fatalf("result = %+v", result)
	}
}

func TestPubSubResultQueueCloseIsIdempotent(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	queue := NewPubSubResultQueue(client, time.Minute)
	queue.Close()
	queue.Close()
}
