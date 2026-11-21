package pipe

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message is one pipeline record.
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// Producer publishes records.
type Producer interface {
	Publish(ctx context.Context, message Message) error
	Close() error
}

// Handler processes one record. Returning an error leaves the offset
// uncommitted so Kafka redelivers it.
type Handler func(ctx context.Context, message Message) error

// Consumer runs a consumer group.
type Consumer interface {
	Topic() string
	Consume(ctx context.Context, handler Handler) error
	Close() error
}

// ConsumerOption configures retry/DLQ behaviour.
type ConsumerOption func(*KafkaConsumer)

// KafkaProducer is the kafka-go producer.
type KafkaProducer struct {
	writer *kafka.Writer
}

// KafkaConsumer is the kafka-go consumer-group reader.
type KafkaConsumer struct {
	reader       *kafka.Reader
	topic        string
	maxAttempts  int
	retryBackoff time.Duration
	startOffset  int64
	dlq          Producer
	dlqTopic     string
	onDLQ        func(topic string)
}
