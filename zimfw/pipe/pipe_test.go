package pipe

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeProducer struct {
	mu       sync.Mutex
	messages []Message
	err      error
}

func (f *fakeProducer) Publish(_ context.Context, message Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func TestProcessRetriesThenSucceeds(t *testing.T) {
	consumer := &KafkaConsumer{maxAttempts: 3, retryBackoff: time.Millisecond}
	attempts := 0
	err := consumer.process(context.Background(), Message{Topic: "toRedis", Key: "c1"},
		func(context.Context, Message) error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestProcessPoisonSkipsRetries(t *testing.T) {
	dlq := &fakeProducer{}
	consumer := &KafkaConsumer{
		maxAttempts: 3, retryBackoff: time.Millisecond, dlq: dlq, dlqTopic: "zim.pipe.toPush.DLQ",
	}
	attempts := 0
	err := consumer.process(context.Background(), Message{Topic: "toPush", Value: []byte("bad")},
		func(context.Context, Message) error {
			attempts++
			return Poison(errors.New("invalid json"))
		})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("dlq = %+v", dlq.messages)
	}
}

func TestProcessMovesToDLQ(t *testing.T) {
	dlq := &fakeProducer{}
	observed := ""
	consumer := &KafkaConsumer{
		maxAttempts: 2, retryBackoff: time.Millisecond, dlq: dlq, dlqTopic: "zim.pipe.toRedis.DLQ",
		onDLQ: func(topic string) { observed = topic },
	}
	attempts := 0
	err := consumer.process(context.Background(), Message{Topic: "toRedis", Key: "c1", Value: []byte("x")},
		func(context.Context, Message) error {
			attempts++
			return errors.New("permanent")
		})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if len(dlq.messages) != 1 || dlq.messages[0].Topic != "zim.pipe.toRedis.DLQ" {
		t.Fatalf("dlq = %+v", dlq.messages)
	}
	if dlq.messages[0].Headers["x-original-topic"] != "toRedis" {
		t.Fatalf("headers = %+v", dlq.messages[0].Headers)
	}
	if observed != "zim.pipe.toRedis.DLQ" {
		t.Fatalf("observer = %q", observed)
	}
}

func TestProcessDLQFailureKeepsError(t *testing.T) {
	dlq := &fakeProducer{err: errors.New("kafka down")}
	consumer := &KafkaConsumer{maxAttempts: 1, dlq: dlq, dlqTopic: "dlq"}
	if err := consumer.process(context.Background(), Message{Topic: "t"}, func(context.Context, Message) error {
		return errors.New("fail")
	}); err == nil {
		t.Fatal("DLQ failure must return an error")
	}
}

func TestProcessContextCancel(t *testing.T) {
	consumer := &KafkaConsumer{maxAttempts: 3, retryBackoff: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.process(ctx, Message{Topic: "t"}, func(context.Context, Message) error {
		return errors.New("fail")
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestKafkaConstructorsAndOptions(t *testing.T) {
	dlq := &fakeProducer{}
	consumer := NewKafkaConsumer([]string{"127.0.0.1:1"}, "g", "topic",
		WithMaxAttempts(5), WithRetryBackoff(time.Millisecond), WithDLQ(dlq, "topic.DLQ"))
	if consumer.Topic() != "topic" || consumer.maxAttempts != 5 || consumer.dlqTopic != "topic.DLQ" {
		t.Fatalf("consumer = %+v", consumer)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	producer := NewKafkaProducer([]string{"127.0.0.1:1"})
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
}
