package message

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewPubSubResultQueue returns a result queue backed by Redis Pub/Sub and a
// local waiter map. It keeps the per-key Redis list as a durable fallback, but
// the normal path never holds one Redis connection per in-flight send.
func NewPubSubResultQueue(client redis.UniversalClient, ttl time.Duration) *PubSubResultQueue {
	if ttl <= 0 {
		ttl = time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	queue := &PubSubResultQueue{
		fallback: NewRedisResultQueue(client, ttl),
		waiters:  make(map[string]chan *SendResult),
		sub:      client.Subscribe(ctx, resultChannel),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	if err := queue.sub.Ping(ctx); err != nil {
		log.Warnf("subscribe message result channel failed: %v", err)
	}
	go queue.consume()
	return queue
}

// Publish keeps the ResultQueue contract. The transfer service normally calls
// RedisResultQueue.Publish directly; this delegation is for completeness.
func (q *PubSubResultQueue) Publish(ctx context.Context, uid int64, clientMsgID string, result *SendResult) error {
	return q.fallback.Publish(ctx, uid, clientMsgID, result)
}

// Wait is the non-atomic fallback path. Pipeline uses WaitWithPublish instead.
func (q *PubSubResultQueue) Wait(ctx context.Context, uid int64, clientMsgID string, timeout time.Duration) (*SendResult, error) {
	return q.fallback.Wait(ctx, uid, clientMsgID, timeout)
}

// WaitWithPublish registers the waiter before publishing the event, which
// removes the result-vs-waiter race. Pub/Sub wakes the waiter immediately; if
// no broadcast arrives within the grace period, the durable per-key list is
// polled once so a disconnected subscriber cannot turn a persisted message into
// a false timeout.
func (q *PubSubResultQueue) WaitWithPublish(ctx context.Context, uid int64, clientMsgID string,
	timeout time.Duration, publish func(context.Context) error) (*SendResult, error) {
	key := resultKey(uid, clientMsgID)
	waiter := make(chan *SendResult, 1)

	q.mu.Lock()
	if _, exists := q.waiters[key]; exists {
		q.mu.Unlock()
		return nil, errs.ErrServerBusy
	}
	q.waiters[key] = waiter
	q.mu.Unlock()
	defer q.removeWaiter(key, waiter)

	if err := publish(ctx); err != nil {
		return nil, errs.ErrServerBusy.WithCause(err)
	}

	grace := timeout
	if grace <= 0 || grace > pubSubGrace {
		grace = pubSubGrace
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case result := <-waiter:
		return result, nil
	case <-ctx.Done():
		metrics.Default.Inc("zim_msg_result_fail_total", map[string]string{"reason": "context"})
		return nil, errs.ErrServerBusy.WithCause(ctx.Err())
	case <-timer.C:
		metrics.Default.Inc("zim_msg_result_fallback_total", nil)
		remaining := timeout - grace
		if remaining <= 0 {
			remaining = 100 * time.Millisecond
		}
		return q.fallback.Wait(ctx, uid, clientMsgID, remaining)
	}
}

// consume dispatches Pub/Sub results to local waiters. Every node-msg instance
// receives every result; an instance without the matching waiter simply drops
// it, which is expected with multiple gateways/nodes.
func (q *PubSubResultQueue) consume() {
	defer close(q.done)

	channel := q.sub.Channel(redis.WithChannelSize(8192))
	for message := range channel {
		event := &resultEvent{}
		if err := json.UnmarshalString(message.Payload, event); err != nil {
			metrics.Default.Inc("zim_msg_result_pubsub_total", map[string]string{"result": "decode_error"})
			continue
		}
		key := resultKey(event.UID, event.ClientMsgID)
		q.mu.Lock()
		waiter := q.waiters[key]
		q.mu.Unlock()
		if waiter == nil {
			metrics.Default.Inc("zim_msg_result_pubsub_total", map[string]string{"result": "unmatched"})
			continue
		}
		result := &SendResult{
			ServerMsgID: event.ServerMsgID,
			Seq:         event.Seq,
			SendTime:    event.SendTime,
			Duplicate:   event.Duplicate,
		}
		select {
		case waiter <- result:
			metrics.Default.Inc("zim_msg_result_pubsub_total", map[string]string{"result": "delivered"})
		default:
			metrics.Default.Inc("zim_msg_result_pubsub_total", map[string]string{"result": "dropped"})
		}
	}
}

// removeWaiter deletes the waiter only when it is still the registered one.
func (q *PubSubResultQueue) removeWaiter(key string, waiter chan *SendResult) {
	q.mu.Lock()
	if q.waiters[key] == waiter {
		delete(q.waiters, key)
	}
	q.mu.Unlock()
}

// Close stops the subscription. It is idempotent.
func (q *PubSubResultQueue) Close() {
	q.close.Do(func() {
		q.cancel()
		if q.sub != nil {
			_ = q.sub.Close()
		}
		<-q.done
	})
}
