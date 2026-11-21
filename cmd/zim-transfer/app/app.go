// Package app runs the Kafka message pipeline: sequence allocation, message
// persistence, conversation previews and toPush publication.
package app

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/idem"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	"github.com/zciyuan/ZIM/zimfw/seq"
)

// New returns the transfer application.
func New(repo messagesvc.Repository, allocator *seq.Allocator, idemStore idem.Store,
	conv *conversationsvc.Service, consumers []pipe.Consumer, producer pipe.Producer,
	results messagesvc.ResultQueue, notifier webhook.Notifier, options ...Option) *App {
	app := &App{
		repo: repo, seq: allocator, idem: idemStore, conv: conv,
		consumers: consumers, producer: producer, results: results, ids: snowflake.New(),
		webhook: notifier, batchCh: make(chan *batchRequest, 4096),
		previews: make(map[string]*model.Conversation), workerCount: 256,
	}
	for _, option := range options {
		option(app)
	}
	return app
}

// Name returns the component name.
func (a *App) Name() string {
	return "transfer-pipeline"
}

// Start runs one consumer goroutine per topic.
func (a *App) Start() {
	if len(a.consumers) == 0 {
		log.Warn("transfer pipeline disabled: etc.zim.pipe.brokers is empty")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.startWorkers(ctx)
	go a.runBatch(ctx)
	go a.runPreviewWriter(ctx)
	for i := range a.consumers {
		consumer := a.consumers[i]
		go func() {
			onRestart := func(error) {
				metrics.Default.Inc("zim_consumer_restarts_total", map[string]string{
					"service": "transfer", "topic": consumer.Topic(),
				})
			}
			if batch, ok := consumer.(*pipe.KafkaConsumer); ok {
				switch consumer.Topic() {
				case define.TopicToMongo:
					pipe.RunBatch(ctx, batch, a.handleMongoBatch, 500, 5*time.Millisecond, onRestart)
					return
				case define.TopicToRedis:
					pipe.RunBatch(ctx, batch, a.handleRedisBatch, 500, 5*time.Millisecond, onRestart)
					return
				}
			}
			handler := a.handleToMongo
			if consumer.Topic() == define.TopicToRedis {
				handler = a.handleToRedis
			}
			pipe.Run(ctx, consumer, handler, onRestart)
		}()
	}
}

// Close stops consumers and the producer.
func (a *App) Close() {
	if a.cancel != nil {
		a.cancel()
	}
	for _, consumer := range a.consumers {
		_ = consumer.Close()
	}
	if a.producer != nil {
		_ = a.producer.Close()
	}
}

func (a *App) handleToRedis(ctx context.Context, message pipe.Message) error {
	event, err := messagesvc.DecodeEvent(message.Value)
	if err != nil {
		log.Errorf("decode toRedis event failed: %v", err)
		return pipe.Poison(err)
	}
	started := time.Now()
	err = a.consume(ctx, event, true)
	metrics.Default.Observe("zim_transfer_latency_ms", map[string]string{"topic": "toRedis"},
		time.Since(started).Milliseconds())
	return err
}

func (a *App) handleToMongo(ctx context.Context, message pipe.Message) error {
	event, err := messagesvc.DecodeEvent(message.Value)
	if err != nil {
		log.Errorf("decode toMongo event failed: %v", err)
		return pipe.Poison(err)
	}
	return a.enqueue(ctx, event)
}

func (a *App) consume(ctx context.Context, event *messagesvc.Event, fanout bool) error {
	if event.ConversationID == "" || event.ClientMsgID == "" {
		log.Warn("drop invalid pipeline event: missing conversation or client_msg_id")
		return nil
	}
	started := time.Now()
	if event.SendTime > 0 {
		metrics.Default.Observe("zim_transfer_event_age_ms", map[string]string{"stage": "received"},
			time.Now().UnixMilli()-event.SendTime)
	}
	if a.idem != nil {
		existing, acquired, err := a.idem.Reserve(ctx, event.SendID, event.ClientMsgID)
		if err != nil {
			if errors.Is(err, idem.ErrPending) {
				return err
			}
			return err
		}
		if !acquired && existing != nil {
			if fanout {
				return a.publishResult(ctx, event, &messagesvc.SendResult{
					ServerMsgID: existing.ServerMsgID, Seq: existing.Seq,
					SendTime: event.SendTime, Duplicate: true,
				})
			}
			return nil
		}
		if !acquired {
			return errors.New("idempotency reservation pending")
		}
	}
	observeStage("idem", started)
	started = time.Now()

	seqValue := event.Seq
	if seqValue <= 0 {
		var err error
		seqValue, err = a.nextSeq(ctx, event.ConversationID)
		if err != nil {
			_ = a.releaseIdem(ctx, event)
			return err
		}
	}
	observeStage("seq", started)
	started = time.Now()
	serverMsgID := event.ServerMsgID
	if serverMsgID == "" {
		serverMsgID = strconv.FormatInt(a.ids.Next(), 10)
	}
	msg := &model.Message{
		ServerMsgID: serverMsgID, ClientMsgID: event.ClientMsgID,
		ConversationID: event.ConversationID, Seq: seqValue, SendID: event.SendID,
		RecvID: event.RecvID, GroupID: event.GroupID, MsgType: event.MsgType,
		Content: event.Content, AtUserIDs: event.AtUserIDs, SendTime: event.SendTime,
		Status: model.MessageNormal,
	}
	result, err := a.repo.Append(ctx, msg)
	if err != nil {
		_ = a.releaseIdem(ctx, event)
		return err
	}
	observeStage("append", started)
	started = time.Now()
	log.Debugf("transfer consumed uid=%d conv=%s client_msg_id=%s server_msg_id=%s seq=%d duplicate=%v",
		event.SendID, event.ConversationID, event.ClientMsgID, msg.ServerMsgID, seqValue, result.Duplicate)
	metrics.Default.Inc("zim_transfer_total", map[string]string{"result": "ok"})
	if !result.Duplicate && a.webhook != nil {
		a.webhook.Notify(webhook.Event{
			Type: "message.created", ServerMsgID: msg.ServerMsgID, ConversationID: msg.ConversationID,
			Seq: msg.Seq, SendID: msg.SendID, RecvID: msg.RecvID, GroupID: msg.GroupID,
			MsgType: msg.MsgType, SendTime: msg.SendTime,
		})
	}
	if result.Duplicate {
		if a.idem != nil {
			_ = a.idem.Complete(ctx, event.SendID, event.ClientMsgID,
				&idem.Result{ServerMsgID: result.ServerMsgID, Seq: result.Seq})
		}
		if fanout {
			// A redelivery must still publish toPush; the first attempt may have
			// crashed after persistence but before/while publishing. Clients
			// dedupe by server_msg_id, so at-least-once is acceptable here.
			if a.producer != nil && len(event.Recipients) > 0 {
				event.ServerMsgID = result.ServerMsgID
				event.Seq = result.Seq
				if publishErr := a.publish(ctx, event); publishErr != nil {
					metrics.Default.Inc("zim_transfer_total", map[string]string{"result": "push_error"})
					log.Warnf("republish duplicate toPush failed: server_msg_id = %s, err = %v",
						result.ServerMsgID, publishErr)
					return publishErr
				}
			}
			return a.publishResult(ctx, event, &messagesvc.SendResult{
				ServerMsgID: result.ServerMsgID, Seq: result.Seq,
				SendTime: event.SendTime, Duplicate: true,
			})
		}
		return nil
	}
	if a.idem != nil {
		if err = a.idem.Complete(ctx, event.SendID, event.ClientMsgID,
			&idem.Result{ServerMsgID: result.ServerMsgID, Seq: result.Seq}); err != nil {
			log.Warnf("complete transfer idempotency failed: uid = %d, err = %v", event.SendID, err)
		}
	}

	recipients := event.Recipients
	if len(recipients) == 0 {
		recipients = []int64{event.SendID}
		if event.RecvID > 0 {
			recipients = append(recipients, event.RecvID)
		}
	}
	now := event.SendTime
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	if fanout {
		preview := &model.Conversation{
			ConvID: event.ConversationID, LastMsgID: serverMsgID, LastMsgSeq: seqValue,
			LastMsgAt: now, LastMsgBrief: event.Content, UpdatedAt: now,
		}
		a.queuePreview(preview)
		a.ensureConversations(ctx, event, recipients, serverMsgID, seqValue, now)
		if a.producer != nil {
			event.ServerMsgID = serverMsgID
			event.Seq = seqValue
			event.Recipients = recipients
			if publishErr := a.publish(ctx, event); publishErr != nil {
				metrics.Default.Inc("zim_transfer_total", map[string]string{"result": "push_error"})
				log.Warnf("publish toPush failed: %v", publishErr)
				// Keep the message retryable: the idem row is committed, so the
				// next delivery takes the duplicate branch and republishes.
				return publishErr
			}
			observeStage("push", started)
			started = time.Now()
			metrics.Default.Observe("zim_transfer_event_age_ms", map[string]string{"stage": "result"},
				time.Now().UnixMilli()-event.SendTime)
			if err = a.publishResult(ctx, event, &messagesvc.SendResult{
				ServerMsgID: serverMsgID, Seq: seqValue, SendTime: now, Recipients: recipients,
			}); err != nil {
				return err
			}
			observeStage("result", started)
		}
	}
	return nil
}

// observeStage records one stage of the transfer hot path. Histogram buckets
// are reused by /metrics to locate the current bottleneck without logging.
func observeStage(stage string, start time.Time) {
	metrics.Default.Observe("zim_transfer_stage_ms", map[string]string{"stage": stage},
		time.Since(start).Milliseconds())
}

func (a *App) publishResult(ctx context.Context, event *messagesvc.Event,
	result *messagesvc.SendResult) error {
	if a.results == nil {
		return nil
	}
	return a.results.Publish(ctx, event.SendID, event.ClientMsgID, result)
}

func (a *App) releaseIdem(ctx context.Context, event *messagesvc.Event) error {
	if a.idem == nil {
		return nil
	}
	return a.idem.Release(ctx, event.SendID, event.ClientMsgID)
}

func (a *App) publish(ctx context.Context, event *messagesvc.Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return a.producer.Publish(ctx, pipe.Message{
		Topic: define.TopicToPush, Key: event.ConversationID, Value: value,
	})
}

func convType(event *messagesvc.Event) int16 {
	if event.GroupID > 0 {
		return model.ConversationGroup
	}
	return model.ConversationSingle
}

func peer(owner int64, event *messagesvc.Event) int64 {
	if event.GroupID > 0 {
		return 0
	}
	if owner == event.SendID {
		return event.RecvID
	}
	return event.SendID
}

// ensureConversations creates per-owner rows only for members not seen by this
// process. The per-message preview lives in zim_conversation_preview, so a
// 2,000-member group writes one preview row instead of 2,000 conversation rows.
func (a *App) ensureConversations(ctx context.Context, event *messagesvc.Event,
	recipients []int64, serverMsgID string, seq, now int64) {
	value, _ := a.initialized.LoadOrStore(event.ConversationID, &sync.Map{})
	seen := value.(*sync.Map)
	batch := make([]*model.Conversation, 0, len(recipients))
	for _, owner := range recipients {
		if _, loaded := seen.LoadOrStore(owner, struct{}{}); loaded {
			continue
		}
		batch = append(batch, &model.Conversation{
			OwnerID: owner, ConvID: event.ConversationID, Type: convType(event),
			PeerID: peer(owner, event), GroupID: event.GroupID,
			LastMsgID: serverMsgID, LastMsgSeq: seq, LastMsgAt: now,
			LastMsgBrief: event.Content, UpdatedAt: now,
		})
	}
	if len(batch) == 0 {
		return
	}
	if err := a.conv.TouchBatch(ctx, batch); err != nil {
		for _, conv := range batch {
			seen.Delete(conv.OwnerID)
		}
		log.Warnf("transfer ensure conversations failed: conv = %s, size = %d, err = %v",
			event.ConversationID, len(batch), err)
	}
}

// nextSeq uses the fast Redis path after the first message. Mongo is read only
// when Redis has no allocator state, which is the recovery case in ADR-0019.
func (a *App) nextSeq(ctx context.Context, conversationID string) (int64, error) {
	if value, ok := a.seqChecked.Load(conversationID); ok {
		if time.Since(value.(time.Time)) < 30*time.Second {
			return a.seq.Next(ctx, conversationID)
		}
	}
	floor := a.conv.PreviewSeq(ctx, conversationID)
	max, _ := a.seq.Max(ctx, conversationID)
	var (
		next int64
		err  error
	)
	if floor > max {
		next, err = a.seq.NextFloor(ctx, conversationID, floor)
	} else {
		next, err = a.seq.Next(ctx, conversationID)
	}
	if err == nil {
		a.seqChecked.Store(conversationID, time.Now())
	}
	return next, err
}
