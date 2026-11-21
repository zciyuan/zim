// Package app consumes toPush and delivers online messages through due gates.
package app

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// New returns the push application.
func New(opts Options) *App {
	return &App{
		proxy: opts.Proxy, locator: opts.Locator, consumer: opts.Consumer,
		consumers: opts.Consumers,
		producer:  opts.Producer, offlineConsumer: opts.OfflineConsumer,
		offlinePublisher: opts.OfflinePublisher, offline: opts.Offline,
		devices: opts.Devices,
	}
}

// Name returns the component name.
func (a *App) Name() string {
	return "push-consumer"
}

// Start runs the toPush and toOfflinePush consumers.
func (a *App) Start() {
	consumers := a.pushConsumers()
	if len(consumers) == 0 && a.offlineConsumer == nil {
		log.Warn("push consumer disabled: etc.zim.pipe.brokers is empty")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	for _, consumer := range consumers {
		topic := consumer.Topic()
		go pipe.Run(ctx, consumer, a.handle, func(error) {
			metrics.Default.Inc("zim_consumer_restarts_total", map[string]string{
				"service": "push", "topic": topic,
			})
		})
	}
	if a.offlineConsumer != nil {
		topic := a.offlineConsumer.Topic()
		go pipe.Run(ctx, a.offlineConsumer, a.handleOffline, func(error) {
			metrics.Default.Inc("zim_consumer_restarts_total", map[string]string{
				"service": "push-offline", "topic": topic,
			})
		})
	}
}

// Close stops the consumers.
func (a *App) Close() {
	if a.cancel != nil {
		a.cancel()
	}
	for _, consumer := range a.pushConsumers() {
		_ = consumer.Close()
	}
	if a.offlineConsumer != nil {
		_ = a.offlineConsumer.Close()
	}
	if a.producer != nil {
		_ = a.producer.Close()
	}
}

func (a *App) pushConsumers() []pipe.Consumer {
	consumers := make([]pipe.Consumer, 0, len(a.consumers)+1)
	if a.consumer != nil {
		consumers = append(consumers, a.consumer)
	}
	return append(consumers, a.consumers...)
}

func (a *App) init() {
	a.initOnce.Do(func() {
		a.cache = newRouteCache(
			etc.Get("etc.zim.push.routeCacheTTL", "5s").Duration(),
			etc.Get("etc.zim.push.routeCacheSize", 200000).Int(),
		)
		a.locateBatchSize = etc.Get("etc.zim.push.locateBatchSize", 500).Int()
		a.batchSize = etc.Get("etc.zim.push.batchSize", 500).Int()
		a.sendConcurrency = etc.Get("etc.zim.push.sendConcurrency", 16).Int()
		a.rpcTimeout = etc.Get("etc.zim.push.rpcTimeout", "3s").Duration()
		a.latencyWarnMs = int64(etc.Get("etc.zim.push.latencyWarnMs", 200).Int())
		if a.locateBatchSize <= 0 {
			a.locateBatchSize = 500
		}
		if a.batchSize <= 0 {
			a.batchSize = 500
		}
		if a.sendConcurrency <= 0 {
			a.sendConcurrency = 16
		}
		if a.rpcTimeout <= 0 {
			a.rpcTimeout = 3 * time.Second
		}
		if a.locator == nil {
			log.Warn("push batch locator missing: online push disabled")
		}
	})
}

func (a *App) handle(ctx context.Context, message pipe.Message) error {
	a.init()

	event, err := messagesvc.DecodeEvent(message.Value)
	if err != nil {
		log.Errorf("decode push event failed: %v", err)
		return pipe.Poison(err)
	}
	if event.SendTime > 0 {
		// The handler metric alone hides Kafka consumer queueing; this is the
		// delivery-side age at the moment push starts processing the event.
		metrics.Default.Observe("zim_push_event_age_ms", map[string]string{"stage": "received"},
			time.Now().UnixMilli()-event.SendTime)
	}
	if event.Type == "admin.kick" {
		return a.handleAdminKick(ctx, event)
	}
	started := time.Now()
	payload, err := proto.Marshal(&msgwire.MessageItem{
		ServerMsgId: event.ServerMsgID, ClientMsgId: event.ClientMsgID,
		ConversationId: event.ConversationID, Seq: event.Seq, SendId: event.SendID,
		RecvId: event.RecvID, GroupId: event.GroupID, MsgType: event.MsgType,
		Content: event.Content, AtUserIds: event.AtUserIDs, SendTime: event.SendTime,
	})
	if err != nil {
		log.Errorf("marshal push message failed: %v", err)
		return pipe.Poison(err)
	}
	envelope, err := contract.EncodeEnvelope(&contract.Envelope{
		Opcode: contract.OpPushMsg, Payload: payload,
	})
	if err != nil {
		log.Errorf("encode push envelope failed: %v", err)
		return pipe.Poison(err)
	}

	targets := a.buildTargets(ctx, event.Recipients)
	buckets, online := a.resolveRoutes(ctx, targets)
	sent, failed := a.deliver(ctx, buckets, envelope)
	offline := a.publishOffline(ctx, event, buckets)

	elapsed := time.Since(started).Milliseconds()
	log.Debugf("push delivered send_id=%d server_msg_id=%s targets=%d online=%d offline=%d gates=%d sent=%d failed=%d queued_offline=%d latency_ms=%d",
		event.SendID, event.ServerMsgID, len(targets), online, len(targets)-online, len(buckets), sent, failed, offline, elapsed)

	result := "ok"
	if failed > 0 {
		result = "partial"
	}
	metrics.Default.Inc("zim_push_total", map[string]string{"result": result})
	metrics.Default.Add("zim_push_targets_total", map[string]string{"result": "online"}, int64(online))
	metrics.Default.Add("zim_push_targets_total", map[string]string{"result": "offline"}, int64(len(targets)-online))
	metrics.Default.Observe("zim_push_latency_ms", nil, elapsed)
	if elapsed > a.latencyWarnMs {
		metrics.Default.Inc("zim_push_slow_total", nil)
		log.Warnf("push latency exceeded: server_msg_id = %s, latency_ms = %d", event.ServerMsgID, elapsed)
	}
	return nil
}

// deliver fans out to gates with bounded concurrency: one RPC per (gate, batch).
func (a *App) deliver(ctx context.Context, buckets map[string][]int64, envelope []byte) (int, int) {
	if len(buckets) == 0 {
		return 0, 0
	}

	var (
		waitGroup sync.WaitGroup
		semaphore = make(chan struct{}, a.sendConcurrency)
		sent      atomic.Int64
		failed    atomic.Int64
	)

	for gateID, deviceKeys := range buckets {
		waitGroup.Add(1)
		semaphore <- struct{}{}
		go func(gateID string, deviceKeys []int64) {
			gateSent, gateFailed := a.deliverGate(ctx, gateID, deviceKeys, envelope)
			sent.Add(int64(gateSent))
			failed.Add(int64(gateFailed))
			<-semaphore
			waitGroup.Done()
		}(gateID, deviceKeys)
	}
	waitGroup.Wait()
	return int(sent.Load()), int(failed.Load())
}

func (a *App) deliverGate(ctx context.Context, gateID string, deviceKeys []int64, envelope []byte) (int, int) {
	var sent, failed int
	for start := 0; start < len(deviceKeys); start += a.batchSize {
		end := start + a.batchSize
		if end > len(deviceKeys) {
			end = len(deviceKeys)
		}
		batch := deviceKeys[start:end]
		callCtx, cancel := context.WithTimeout(ctx, a.rpcTimeout)
		delivered, err := a.proxy.Multicast(callCtx, &cluster.MulticastArgs{
			GID: gateID, Kind: cluster.User, Targets: batch, Ack: true,
			Message: &cluster.Message{Route: contract.OpPushMsg, Data: envelope},
		})
		cancel()
		if err != nil {
			failed += len(batch)
			for _, deviceKey := range batch {
				a.cache.Delete(deviceKey)
			}
			metrics.Default.Inc("zim_push_rpc_total", map[string]string{"result": "error"})
			log.Warnf("multicast failed: gate = %s, targets = %d, err = %v", gateID, len(batch), err)
			continue
		}
		metrics.Default.Inc("zim_push_rpc_total", map[string]string{"result": "ok"})
		if int(delivered) >= len(batch) {
			sent += len(batch)
			continue
		}
		sent += int(delivered)
		failed += len(batch) - int(delivered)
	}
	return sent, failed
}

// handleAdminKick pushes 1902 and closes all device sessions of a user.
func (a *App) handleAdminKick(ctx context.Context, event *messagesvc.Event) error {
	reason := 2
	if parsed, err := strconv.Atoi(event.Content); err == nil && parsed > 0 && parsed <= 4 {
		reason = parsed
	}
	payload := []byte{0x08, byte(reason)}
	envelope, err := contract.EncodeEnvelope(&contract.Envelope{
		Opcode: contract.OpPushKick, Payload: payload,
	})
	if err != nil {
		return pipe.Poison(err)
	}
	targets := a.buildTargets(ctx, event.Recipients)
	buckets, _ := a.resolveRoutes(ctx, targets)
	for gateID, keys := range buckets {
		callCtx, cancel := context.WithTimeout(ctx, a.rpcTimeout)
		_, _ = a.proxy.Multicast(callCtx, &cluster.MulticastArgs{
			GID: gateID, Kind: cluster.User, Targets: keys, Disconnect: true, Ack: true,
			Message: &cluster.Message{Route: contract.OpPushKick, Data: envelope},
		})
		cancel()
	}
	return nil
}
