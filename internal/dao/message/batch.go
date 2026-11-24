package message

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zciyuan/ZIM/internal/model"
)

const (
	// At 1,000 msg/s a 2ms window only collects ~2 messages, so every message
	// pays three Mongo round trips. A 10ms/200 batch amortizes the bulk writes
	// while still adding at most 10ms of intentional latency.
	appendBatchSize   = 200
	appendBatchWindow = 10 * time.Millisecond
	appendQueueSize   = 4096
	// Mongo bulk writes to different conversation blocks are independent.
	// Sharding the batcher keeps one large conversation from holding every
	// other conversation behind a single bulk write.
	appendShardCount = 8
)

type appendOutcome struct {
	result *AppendResult
	err    error
}

type appendRequest struct {
	ctx     context.Context
	msg     *model.Message
	outcome chan appendOutcome
}

// Append enqueues one message into the shared batch window. The caller still
// waits for its own result; the batch collapses idem insert, block $push and
// commit into three Mongo round trips for up to 100 messages.
func (d *DAO) Append(ctx context.Context, msg *model.Message) (*AppendResult, error) {
	if d == nil || d.blocks == nil || d.idem == nil || msg == nil {
		return nil, mongo.ErrNilDocument
	}
	d.startBatcher()
	request := &appendRequest{ctx: ctx, msg: msg, outcome: make(chan appendOutcome, 1)}
	batcher := d.batchers[appendShard(msg.ConversationID)]
	select {
	case batcher.queue <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case outcome := <-request.outcome:
		return outcome.result, outcome.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *DAO) startBatcher() {
	d.batchOnce.Do(func() {
		d.batchers = make([]*appendBatcher, appendShardCount)
		for index := range d.batchers {
			batcher := &appendBatcher{dao: d, queue: make(chan *appendRequest, appendQueueSize)}
			d.batchers[index] = batcher
			go batcher.run()
		}
	})
}

// appendShard picks a stable batcher by conversation, so one conversation's
// messages stay in the same batch/order.
func appendShard(conversationID string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(conversationID))
	return int(hash.Sum32() % appendShardCount)
}

type appendBatcher struct {
	dao   *DAO
	queue chan *appendRequest
}

func (b *appendBatcher) run() {
	for first := range b.queue {
		batch := []*appendRequest{first}
		timer := time.NewTimer(appendBatchWindow)
	collect:
		for len(batch) < appendBatchSize {
			select {
			case request := <-b.queue:
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
		b.flush(batch)
	}
}

func (b *appendBatcher) flush(batch []*appendRequest) {
	ctx := context.Background()
	if batch[0].ctx != nil {
		ctx = batch[0].ctx
	}
	now := time.Now().UnixMilli()
	idemModels := make([]mongo.WriteModel, 0, len(batch))
	for _, request := range batch {
		key := request.msg.ConversationID + "|" + request.msg.ClientMsgID
		idemModels = append(idemModels, mongo.NewInsertOneModel().SetDocument(&idemDoc{
			ID: key, ServerMsgID: request.msg.ServerMsgID, Seq: request.msg.Seq,
			State: idemPending, UpdatedAt: now,
		}))
	}
	inserted, _, err := bulkWriteResult(ctx, b.dao.idem, idemModels, len(batch))
	if err != nil {
		for _, request := range batch {
			request.outcome <- appendOutcome{err: err}
		}
		return
	}
	groups := make(map[string][]int)
	docMsgs := make(map[string][]blockMsg)
	burnPayloads := make([]*model.Message, 0, len(batch))
	for index, request := range batch {
		if !inserted[index] {
			continue
		}
		msg := request.msg
		blockIdx := (msg.Seq - 1) / model.MessageBlockSize
		docID := fmt.Sprintf("%s:%d", msg.ConversationID, blockIdx)
		groups[docID] = append(groups[docID], index)
		// Burn messages keep the payload in burn_messages with TTL erasure;
		// the main block holds only light metadata (ADR-0043 §2.4.1).
		content, tokens := msg.Content, searchTokens(msg.Content)
		if msg.BurnTTL > 0 {
			content, tokens = "", nil
			burnPayloads = append(burnPayloads, msg)
		}
		docMsgs[docID] = append(docMsgs[docID], blockMsg{Msg: messageDoc{
			ServerMsgID: msg.ServerMsgID, ClientMsgID: msg.ClientMsgID, SendID: msg.SendID,
			RecvID: msg.RecvID, GroupID: msg.GroupID, MsgType: msg.MsgType,
			Content: content, SearchTokens: tokens, AtUserIDs: msg.AtUserIDs, Seq: msg.Seq,
			SendTime: msg.SendTime, Status: msg.Status,
			BurnTTL: msg.BurnTTL, BurnExpireAt: msg.BurnExpireAt,
			MaxRetentionAt: msg.MaxRetentionAt, SecurityFlags: msg.SecurityFlags,
			ExtType: msg.ExtType, ExtPayload: msg.ExtPayload,
		}})
	}
	blockModels := make([]mongo.WriteModel, 0, len(groups))
	docIDs := make([]string, 0, len(groups))
	for docID, indexes := range groups {
		first := batch[indexes[0]].msg
		serverIDs := make([]string, 0, len(indexes))
		for _, index := range indexes {
			serverIDs = append(serverIDs, batch[index].msg.ServerMsgID)
		}
		blockModels = append(blockModels, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": docID, "msgs.msg.server_msg_id": bson.M{"$nin": serverIDs}}).
			SetUpdate(bson.M{
				"$setOnInsert": bson.M{
					"doc_id": docID, "conversation_id": first.ConversationID,
					"block_idx": (first.Seq - 1) / model.MessageBlockSize,
				},
				"$push": bson.M{"msgs": bson.M{"$each": docMsgs[docID]}},
			}).SetUpsert(true))
		docIDs = append(docIDs, docID)
	}
	blockFailed := make(map[string]bool)
	if len(blockModels) > 0 {
		_, blockErr := b.dao.blocks.BulkWrite(ctx, blockModels, options.BulkWrite().SetOrdered(false))
		if blockErr != nil {
			// A bulk failure can leave a partially written batch. Fall back to
			// the single-message repair path so each caller gets a definitive
			// result and pending idem rows stay repairable.
			for _, request := range batch {
				result, directErr := b.dao.appendDirect(ctx, request.msg)
				request.outcome <- appendOutcome{result: result, err: directErr}
			}
			return
		}
	}
	commitModels := make([]mongo.WriteModel, 0, len(batch))
	commitIndexes := make([]int, 0, len(batch))
	for index, request := range batch {
		if !inserted[index] {
			continue
		}
		serverID := request.msg.ServerMsgID
		for _, failedID := range docIDs {
			if failedID == fmt.Sprintf("%s:%d", request.msg.ConversationID,
				(request.msg.Seq-1)/model.MessageBlockSize) && blockFailed[failedID] {
				break
			}
		}
		key := request.msg.ConversationID + "|" + request.msg.ClientMsgID
		commitModels = append(commitModels, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": key}).
			SetUpdate(bson.M{"$set": bson.M{"state": idemCommitted, "updated_at": now}}))
		_ = serverID
		commitIndexes = append(commitIndexes, index)
	}
	if len(commitModels) > 0 {
		if _, commitErr := b.dao.idem.BulkWrite(ctx, commitModels,
			options.BulkWrite().SetOrdered(false)); commitErr != nil {
			for _, request := range batch {
				if inserted[indexOf(batch, request)] {
					request.outcome <- appendOutcome{err: commitErr}
				}
			}
			return
		}
	}
	commitSet := make(map[int]bool, len(commitIndexes))
	for _, index := range commitIndexes {
		commitSet[index] = true
	}
	// Burn payloads move to burn_messages after the block is durable; a failed
	// store keeps the idem row committed so a redelivery repairs it (same
	// ordering as appendDirect, ADR-0043 §2.4.1).
	for _, burnMsg := range burnPayloads {
		if burnErr := b.dao.StoreBurnPayload(ctx, burnMsg); burnErr != nil {
			for _, request := range batch {
				if !inserted[indexOf(batch, request)] {
					continue
				}
				request.outcome <- appendOutcome{err: burnErr}
			}
			return
		}
	}
	for index, request := range batch {
		if !inserted[index] {
			result, directErr := b.dao.appendDirect(ctx, request.msg)
			request.outcome <- appendOutcome{result: result, err: directErr}
			continue
		}
		if !commitSet[index] {
			request.outcome <- appendOutcome{err: fmt.Errorf("mongo batch commit missing index %d", index)}
			continue
		}
		request.outcome <- appendOutcome{result: &AppendResult{
			ServerMsgID: request.msg.ServerMsgID, Seq: request.msg.Seq,
		}}
	}
}

func indexOf(batch []*appendRequest, target *appendRequest) int {
	for index, item := range batch {
		if item == target {
			return index
		}
	}
	return -1
}

func bulkWriteResult(ctx context.Context, collection *mongo.Collection,
	models []mongo.WriteModel, count int) (inserted []bool, duplicate []bool, err error) {
	inserted = make([]bool, count)
	duplicate = make([]bool, count)
	_, err = collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err == nil {
		for i := range inserted {
			inserted[i] = true
		}
		return inserted, duplicate, nil
	}
	bulkErr, ok := err.(mongo.BulkWriteException)
	if !ok {
		return nil, nil, err
	}
	failed := make(map[int]bool, len(bulkErr.WriteErrors))
	for _, writeErr := range bulkErr.WriteErrors {
		failed[writeErr.Index] = true
		if mongo.IsDuplicateKeyError(writeErr) {
			duplicate[writeErr.Index] = true
		}
	}
	for i := range inserted {
		inserted[i] = !failed[i]
	}
	return inserted, duplicate, nil
}
