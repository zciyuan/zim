package message

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// NewPipeline returns the Kafka send pipeline.
func NewPipeline(service *Service, producer pipe.Producer, results ResultQueue,
	timeout time.Duration) (*Pipeline, error) {
	if service == nil {
		return nil, ErrRepositoryMissing
	}
	if producer == nil {
		return nil, errors.New("pipeline producer missing")
	}
	if results == nil {
		return nil, errors.New("pipeline result queue missing")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Pipeline{service: service, producer: producer, results: results, timeout: timeout, now: time.Now}, nil
}

// SendSingle validates and publishes a direct message.
func (p *Pipeline) SendSingle(ctx context.Context, sender int64, req *SendRequest) (*SendResult, error) {
	started := time.Now()
	if err := p.service.checkSensitive(ctx, req); err != nil {
		return nil, err
	}
	observeSendStage("sensitive", started)
	started = time.Now()
	prepared, err := p.service.PrepareSingle(ctx, sender, req)
	if err != nil {
		return nil, err
	}
	observeSendStage("prepare", started)
	started = time.Now()
	result, err := p.send(ctx, sender, prepared, req)
	if err == nil {
		observeSendStage("total", started)
	}
	return result, err
}

// SendGroup validates and publishes a group message.
func (p *Pipeline) SendGroup(ctx context.Context, sender int64, req *SendRequest) (*SendResult, error) {
	if err := p.service.checkSensitive(ctx, req); err != nil {
		return nil, err
	}
	prepared, err := p.service.PrepareGroup(ctx, sender, req)
	if err != nil {
		return nil, err
	}
	return p.send(ctx, sender, prepared, req)
}

func (p *Pipeline) send(ctx context.Context, sender int64, prepared *Prepared, req *SendRequest) (*SendResult, error) {
	now := time.Now
	if p.now != nil {
		now = p.now
	}
	event := &Event{
		ClientMsgID: req.ClientMsgID, ConversationID: prepared.ConversationID,
		SendID: sender, RecvID: req.ReceiverID, GroupID: req.GroupID,
		MsgType: req.MsgType, Content: req.Content, AtUserIDs: req.AtUserIDs,
		SendTime: now().UnixMilli(), Recipients: prepared.Recipients,
	}
	publish := func(publishCtx context.Context) error {
		publishStart := time.Now()
		value, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return errs.ErrInternal.WithCause(marshalErr)
		}
		publishErr := p.producer.Publish(publishCtx, pipe.Message{
			Topic: define.TopicToRedis, Key: event.ConversationID, Value: value,
		})
		observeSendStage("publish", publishStart)
		return publishErr
	}

	var (
		result *SendResult
		err    error
	)
	waitStart := time.Now()
	if queue, ok := p.results.(PublishingResultQueue); ok {
		result, err = queue.WaitWithPublish(ctx, sender, req.ClientMsgID, p.timeout, publish)
	} else {
		if err = publish(ctx); err != nil {
			return nil, errs.ErrServerBusy.WithCause(err)
		}
		result, err = p.results.Wait(ctx, sender, req.ClientMsgID, p.timeout)
	}
	observeSendStage("wait", waitStart)
	if err != nil {
		return nil, err
	}
	result.UnreadCount = prepared.UnreadCount
	result.Recipients = prepared.Recipients
	return result, nil
}

const (
	// resultChannel is the Redis Pub/Sub channel for send results. Every
	// node-msg instance subscribes; only the instance owning the waiter acts on
	// a message, so a result is delivered with one broadcast per node.
	resultChannel = "zim:msg:result:events"
	// pubSubGrace bounds how long a waiter trusts Pub/Sub before falling back
	// to the durable per-key list. It is only reached when the subscription is
	// down or a result was lost, not on the normal path.
	pubSubGrace = 500 * time.Millisecond
)

// NewRedisResultQueue returns the Redis result queue.
func NewRedisResultQueue(client redis.UniversalClient, ttl time.Duration) *RedisResultQueue {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &RedisResultQueue{client: client, ttl: ttl}
}

// Publish stores a result for a waiting sender and broadcasts it on Pub/Sub.
// The durable list keeps a fallback for a lost/degraded subscription; the
// broadcast is what wakes the local waiter without holding a Redis connection.
func (q *RedisResultQueue) Publish(ctx context.Context, uid int64, clientMsgID string, result *SendResult) error {
	value, err := json.Marshal(result)
	if err != nil {
		return err
	}
	broadcast, err := json.Marshal(resultEvent{
		UID: uid, ClientMsgID: clientMsgID, ServerMsgID: result.ServerMsgID,
		Seq: result.Seq, SendTime: result.SendTime, Duplicate: result.Duplicate,
	})
	if err != nil {
		return err
	}

	key := resultKey(uid, clientMsgID)
	pipe := q.client.Pipeline()
	pipe.LPush(ctx, key, value)
	pipe.Expire(ctx, key, q.ttl)
	pipe.Publish(ctx, resultChannel, broadcast)
	_, err = pipe.Exec(ctx)
	return err
}

// Wait blocks until the result is available or the timeout expires.
func (q *RedisResultQueue) Wait(ctx context.Context, uid int64, clientMsgID string,
	timeout time.Duration) (*SendResult, error) {
	values, err := q.client.BLPop(ctx, timeout, resultKey(uid, clientMsgID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, errs.ErrServerBusy
		}
		return nil, errs.ErrServerBusy.WithCause(err)
	}
	if len(values) != 2 {
		return nil, errs.ErrServerBusy
	}
	result := &SendResult{}
	if err = json.UnmarshalString(values[1], result); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return result, nil
}

// observeSendStage records one node-msg stage of the send hot path.
func observeSendStage(stage string, start time.Time) {
	metrics.Default.Observe("zim_msg_send_stage_ms", map[string]string{"stage": stage},
		time.Since(start).Milliseconds())
}

func resultKey(uid int64, clientMsgID string) string {
	return "zim:msg:result:" + strconv.FormatInt(uid, 10) + ":" + clientMsgID
}
