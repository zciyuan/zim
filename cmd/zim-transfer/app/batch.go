package app

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

const (
	// A larger dispatch batch lets all conversations of a burst run in
	// parallel across the shard workers instead of forming a second batch
	// behind the first one. Workers still serialize each conversation.
	transferBatchSize   = 500
	transferBatchWindow = 5 * time.Millisecond
)

func (a *App) enqueue(ctx context.Context, event *messagesvc.Event) error {
	if a.batchCh == nil {
		return a.consume(ctx, event, false)
	}
	request := &batchRequest{ctx: ctx, event: event, done: make(chan error, 1)}
	select {
	case a.batchCh <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) runBatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first := <-a.batchCh:
			batch := []*batchRequest{first}
			timer := time.NewTimer(transferBatchWindow)
		collect:
			for len(batch) < transferBatchSize {
				select {
				case request := <-a.batchCh:
					batch = append(batch, request)
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if err := a.processBatch(batch); err != nil {
				log.Warnf("transfer batch failed: %v", err)
			}
		}
	}
}

// processBatch dispatches each event to its conversation-shard worker. Events
// of one conversation always land on the same worker and are processed in
// arrival order, while different conversations run in parallel.
func (a *App) processBatch(batch []*batchRequest) error {
	if len(a.workers) == 0 {
		for _, request := range batch {
			request.done <- a.consume(request.ctx, request.event, request.fanout)
		}
		return nil
	}
	pending := make([]*batchRequest, 0, len(batch))
	for _, request := range batch {
		select {
		case a.workerFor(request.event.ConversationID) <- request:
			pending = append(pending, request)
		case <-request.ctx.Done():
			request.done <- request.ctx.Err()
		}
	}
	var firstErr error
	for _, request := range pending {
		if err := <-request.done; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	metrics.Default.Observe("zim_transfer_batch_size", nil, int64(len(batch)))
	return firstErr
}

func (a *App) handleMongoBatch(ctx context.Context, messages []pipe.Message) error {
	batch := decodeBatch(ctx, messages, false)
	if len(batch) == 0 {
		return nil
	}
	return a.processBatch(batch)
}

func (a *App) handleRedisBatch(ctx context.Context, messages []pipe.Message) error {
	batch := decodeBatch(ctx, messages, true)
	if len(batch) == 0 {
		return nil
	}
	return a.processBatch(batch)
}

func decodeBatch(ctx context.Context, messages []pipe.Message, fanout bool) []*batchRequest {
	batch := make([]*batchRequest, 0, len(messages))
	for _, message := range messages {
		event, err := messagesvc.DecodeEvent(message.Value)
		if err != nil {
			log.Errorf("decode transfer batch event failed: %v", err)
			continue
		}
		batch = append(batch, &batchRequest{
			ctx: ctx, event: event, fanout: fanout, done: make(chan error, 1),
		})
	}
	return batch
}
