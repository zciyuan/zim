// Package pipe is the ZIM Kafka message pipeline. It wraps kafka-go with
// explicit commit semantics: a message is committed only after the handler
// returns nil or is moved to the DLQ after the configured attempts.
package pipe

import (
	"context"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/segmentio/kafka-go"
)

const (
	defaultMaxAttempts  = 3
	defaultRetryBackoff = 200 * time.Millisecond
)

// poisonError marks a record that must not be retried (bad payload/schema).
type poisonError struct{ err error }

func (e *poisonError) Error() string { return e.err.Error() }
func (e *poisonError) Unwrap() error { return e.err }

// Poison marks an error as non-retryable; the consumer writes it to the DLQ
// immediately instead of retrying the same bad record three times.
func Poison(err error) error {
	if err == nil {
		return nil
	}
	return &poisonError{err: err}
}

// IsPoison reports whether err was marked as non-retryable by Poison.
func IsPoison(err error) bool {
	var target *poisonError
	return dueerrors.As(err, &target)
}

// NewKafkaProducer returns a producer for the given brokers.
func NewKafkaProducer(brokers []string) *KafkaProducer {
	return &KafkaProducer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 5 * time.Millisecond,
	}}
}

// Publish writes one record.
func (p *KafkaProducer) Publish(ctx context.Context, message Message) error {
	headers := make([]kafka.Header, 0, len(message.Headers))
	for key, value := range message.Headers {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: message.Topic, Key: []byte(message.Key), Value: message.Value, Headers: headers,
	})
}

// Close closes the writer.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// WithMaxAttempts sets how many times one record is processed before it is
// moved to the DLQ. Non-positive values fall back to 3.
func WithMaxAttempts(attempts int) ConsumerOption {
	return func(consumer *KafkaConsumer) {
		if attempts > 0 {
			consumer.maxAttempts = attempts
		}
	}
}

// WithRetryBackoff sets the delay between attempts for one record.
func WithRetryBackoff(backoff time.Duration) ConsumerOption {
	return func(consumer *KafkaConsumer) {
		if backoff > 0 {
			consumer.retryBackoff = backoff
		}
	}
}

// WithDLQ enables the dead-letter producer and topic.
func WithDLQ(producer Producer, topic string) ConsumerOption {
	return func(consumer *KafkaConsumer) {
		consumer.dlq = producer
		consumer.dlqTopic = topic
	}
}

// WithDLQObserver registers a callback invoked after a record is written to
// the DLQ, so callers can increment a metric without importing metrics here.
func WithDLQObserver(observer func(topic string)) ConsumerOption {
	return func(consumer *KafkaConsumer) {
		consumer.onDLQ = observer
	}
}

// WithStartOffset sets the initial offset for a new consumer group.
// Use kafka.LastOffset for best-effort topics such as toPush, and
// kafka.FirstOffset for durable topics such as toRedis/toMongo.
func WithStartOffset(offset int64) ConsumerOption {
	return func(consumer *KafkaConsumer) {
		consumer.startOffset = offset
	}
}

// WithLastOffset starts a new consumer group at the latest record. Use it for
// best-effort topics (toPush) where replaying history would re-push old
// messages; durable topics should keep the default kafka.FirstOffset.
func WithLastOffset() ConsumerOption {
	return WithStartOffset(kafka.LastOffset)
}

// NewKafkaConsumer returns a consumer for one topic and group.
func NewKafkaConsumer(brokers []string, group, topic string, options ...ConsumerOption) *KafkaConsumer {
	consumer := &KafkaConsumer{
		topic:        topic,
		maxAttempts:  defaultMaxAttempts,
		retryBackoff: defaultRetryBackoff,
		startOffset:  kafka.FirstOffset,
	}
	for _, option := range options {
		option(consumer)
	}
	consumer.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
		StartOffset:    consumer.startOffset,
	})
	return consumer
}

// Topic returns the consumed topic.
func (c *KafkaConsumer) Topic() string {
	return c.topic
}

// Consume processes messages until ctx is cancelled or the reader fails.
func (c *KafkaConsumer) Consume(ctx context.Context, handler Handler) error {
	for {
		record, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		message := Message{
			Topic: record.Topic, Key: string(record.Key), Value: record.Value,
			Headers: make(map[string]string, len(record.Headers)),
		}
		for _, header := range record.Headers {
			message.Headers[header.Key] = string(header.Value)
		}
		if err = c.process(ctx, message, handler); err != nil {
			return err
		}
		if err = c.reader.CommitMessages(ctx, record); err != nil {
			return err
		}
	}
}

// process retries one record synchronously. A successful DLQ write is treated
// as handled so the offset can be committed; a failed DLQ write keeps the
// offset uncommitted.
func (c *KafkaConsumer) process(ctx context.Context, message Message, handler Handler) error {
	attempts := c.maxAttempts
	if attempts <= 0 {
		attempts = defaultMaxAttempts
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := handler(ctx, message); err == nil {
			return nil
		} else {
			lastErr = err
			if IsPoison(err) {
				break
			}
		}
		if attempt == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.retryBackoff):
		}
	}

	if c.dlq == nil || c.dlqTopic == "" {
		log.Errorf("pipeline message dropped after %d attempts: topic = %s, err = %v",
			attempts, message.Topic, lastErr)
		return nil
	}
	headers := make(map[string]string, len(message.Headers)+1)
	for key, value := range message.Headers {
		headers[key] = value
	}
	headers["x-original-topic"] = message.Topic
	if err := c.dlq.Publish(ctx, Message{
		Topic: c.dlqTopic, Key: message.Key, Value: message.Value, Headers: headers,
	}); err != nil {
		log.Errorf("pipeline DLQ publish failed: topic = %s, dlq = %s, err = %v",
			message.Topic, c.dlqTopic, err)
		return err
	}
	log.Warnf("pipeline message moved to DLQ: topic = %s, dlq = %s, attempts = %d",
		message.Topic, c.dlqTopic, attempts)
	if c.onDLQ != nil {
		c.onDLQ(c.dlqTopic)
	}
	return nil
}

// Run consumes until ctx is cancelled, restarting the consumer with
// exponential backoff after an error. onRestart is optional and is called
// before the backoff wait so callers can record a restart metric.
func Run(ctx context.Context, consumer Consumer, handler Handler, onRestart func(error)) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := consumer.Consume(ctx, handler)
		if ctx.Err() != nil {
			return
		}
		if onRestart != nil {
			onRestart(err)
		}
		log.Errorf("consumer stopped: topic = %s, err = %v, retry in %s", consumer.Topic(), err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Close closes the reader.
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}

// BatchHandler processes a bounded batch. The consumer commits offsets only
// after the whole batch returns nil.
type BatchHandler func(ctx context.Context, messages []Message) error

// ConsumeBatch fetches up to max records within window and invokes handler
// once. This amortizes Mongo/Redis/MySQL round trips across records.
func (c *KafkaConsumer) ConsumeBatch(ctx context.Context, handler BatchHandler, max int, window time.Duration) error {
	if max <= 0 {
		max = 100
	}
	if window <= 0 {
		window = 3 * time.Millisecond
	}
	records := make([]kafka.Message, 0, max)
	for len(records) < max {
		batchCtx, cancel := context.WithTimeout(ctx, window)
		record, err := c.reader.FetchMessage(batchCtx)
		deadline := batchCtx.Err() == context.DeadlineExceeded
		cancel()
		if err != nil {
			if len(records) > 0 {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if deadline {
				continue
			}
			return err
		}
		records = append(records, record)
	}
	if len(records) == 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
	messages := make([]Message, 0, len(records))
	for _, record := range records {
		message := Message{Topic: record.Topic, Key: string(record.Key), Value: record.Value,
			Headers: make(map[string]string, len(record.Headers))}
		for _, header := range record.Headers {
			message.Headers[header.Key] = string(header.Value)
		}
		messages = append(messages, message)
	}
	if err := handler(ctx, messages); err != nil {
		return err
	}
	return c.reader.CommitMessages(ctx, records...)
}

// RunBatch consumes batches until ctx is cancelled, restarting with backoff.
func RunBatch(ctx context.Context, consumer *KafkaConsumer, handler BatchHandler,
	max int, window time.Duration, onRestart func(error)) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := consumer.ConsumeBatch(ctx, handler, max, window)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			backoff = time.Second
			continue
		}
		if onRestart != nil {
			onRestart(err)
		}
		log.Errorf("batch consumer stopped: topic = %s, err = %v, retry in %s", consumer.Topic(), err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
