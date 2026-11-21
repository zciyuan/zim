// Package message is the MongoDB DAO for aggregate message documents.
package message

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zciyuan/ZIM/internal/model"
)

const (
	blocksCollection = "msg"
	idemCollection   = "msg_idem"
)

// New returns a message DAO.
func New(database *mongo.Database) *DAO {
	return &DAO{
		blocks: database.Collection(blocksCollection),
		idem:   database.Collection(idemCollection),
	}
}

// EnsureIndexes creates the block query indexes. The block collection is
// sharded by {conversation_id, block_idx}; MongoDB only allows unique indexes
// whose prefix is the shard key, so doc_id uniqueness comes from _id = doc_id
// (same conversation -> same shard), not from a separate unique index. L2
// idempotency lives in msg_idem (ADR-0013).
func (d *DAO) EnsureIndexes(ctx context.Context) error {
	_, err := d.blocks.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "msgs.msg.send_id", Value: 1}, {Key: "msgs.msg.send_time", Value: -1}}},
		{Keys: bson.D{{Key: "msgs.msg.seq", Value: 1}}},
		{Keys: bson.D{{Key: "msgs.msg.server_msg_id", Value: 1}}},
		// Compound shard-key index: same conversation blocks stay on one shard.
		{Keys: bson.D{{Key: "conversation_id", Value: 1}, {Key: "block_idx", Value: 1}}},
		{Keys: bson.D{{Key: "msgs.msg.search_tokens", Value: 1}}},
		{Keys: bson.D{{Key: "msgs.msg.content", Value: "text"}}},
	})
	return err
}

// Append stores one message in its 100-message block. It uses a small state
// machine in msg_idem (pending -> committed) so a crash between the idem write
// and the block write is repaired on redelivery instead of losing the message
// (ADR-0013). A duplicate client_msg_id returns the original identity.
func (d *DAO) appendDirect(ctx context.Context, msg *model.Message) (*AppendResult, error) {
	key := msg.ConversationID + "|" + msg.ClientMsgID
	now := time.Now().UnixMilli()
	_, err := d.idem.InsertOne(ctx, &idemDoc{
		ID: key, ServerMsgID: msg.ServerMsgID, Seq: msg.Seq, State: idemPending, UpdatedAt: now,
	})
	if err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return nil, err
		}
		existing := &idemDoc{}
		if findErr := d.idem.FindOne(ctx, bson.M{"_id": key}).Decode(existing); findErr != nil {
			return nil, findErr
		}
		// Documents written before ADR-0013 have no state; treat them as committed.
		if existing.State == "" || existing.State == idemCommitted {
			return &AppendResult{ServerMsgID: existing.ServerMsgID, Seq: existing.Seq, Duplicate: true}, nil
		}
		// Pending: the previous attempt may have crashed before the block write.
		// Repair the block with the identity reserved by the first attempt.
		repaired := *msg
		if existing.ServerMsgID != "" && existing.Seq > 0 {
			repaired.ServerMsgID = existing.ServerMsgID
			repaired.Seq = existing.Seq
		}
		if writeErr := d.writeBlock(ctx, &repaired); writeErr != nil {
			return nil, writeErr
		}
		if markErr := d.markCommitted(ctx, key, now); markErr != nil {
			return nil, markErr
		}
		return &AppendResult{ServerMsgID: repaired.ServerMsgID, Seq: repaired.Seq, Duplicate: true}, nil
	}

	if err = d.writeBlock(ctx, msg); err != nil {
		// Keep the pending idem row so a later retry can repair the block.
		return nil, err
	}
	if err = d.markCommitted(ctx, key, now); err != nil {
		return nil, err
	}
	return &AppendResult{ServerMsgID: msg.ServerMsgID, Seq: msg.Seq}, nil
}

// writeBlock upserts the aggregate document without appending the same
// server_msg_id twice. A duplicate-key error means a concurrent writer already
// inserted the block; the guard filter below distinguishes "message present"
// from a real failure.
func (d *DAO) writeBlock(ctx context.Context, msg *model.Message) error {
	blockIdx := (msg.Seq - 1) / model.MessageBlockSize
	docID := fmt.Sprintf("%s:%d", msg.ConversationID, blockIdx)
	filter := bson.M{"_id": docID, "msgs.msg.server_msg_id": bson.M{"$ne": msg.ServerMsgID}, "msgs.msg.seq": bson.M{"$ne": msg.Seq}}
	update := bson.M{
		"$setOnInsert": bson.M{
			"doc_id":          docID,
			"conversation_id": msg.ConversationID,
			"block_idx":       blockIdx,
		},
		"$push": bson.M{
			"msgs": blockMsg{
				Msg: messageDoc{
					ServerMsgID: msg.ServerMsgID, ClientMsgID: msg.ClientMsgID, SendID: msg.SendID,
					RecvID: msg.RecvID, GroupID: msg.GroupID, MsgType: msg.MsgType,
					Content: msg.Content, SearchTokens: searchTokens(msg.Content), AtUserIDs: msg.AtUserIDs, Seq: msg.Seq,
					SendTime: msg.SendTime, Status: msg.Status,
				},
			},
		},
	}
	if _, err := d.blocks.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true)); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return err
		}
		present, countErr := d.blocks.CountDocuments(ctx, bson.M{
			"_id": docID, "msgs.msg.server_msg_id": msg.ServerMsgID,
		})
		if countErr != nil {
			return countErr
		}
		if present == 0 {
			return err
		}
	}
	return nil
}

func (d *DAO) markCommitted(ctx context.Context, key string, now int64) error {
	_, err := d.idem.UpdateOne(ctx, bson.M{"_id": key},
		bson.M{"$set": bson.M{"state": idemCommitted, "updated_at": now}})
	return err
}

// FindBySeq returns messages in [begin, end], ordered by seq.
func (d *DAO) FindBySeq(ctx context.Context, conversationID string, begin, end int64, limit int) ([]*model.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	beginBlock := (begin - 1) / model.MessageBlockSize
	endBlock := (end - 1) / model.MessageBlockSize
	cursor, err := d.blocks.Find(ctx, bson.M{
		"conversation_id": conversationID,
		"block_idx":       bson.M{"$gte": beginBlock, "$lte": endBlock},
	}, options.Find().SetSort(bson.D{{Key: "block_idx", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	result := make([]*model.Message, 0, limit)
	for cursor.Next(ctx) {
		block := &blockDoc{}
		if err = cursor.Decode(block); err != nil {
			return nil, err
		}
		for _, item := range block.Msgs {
			if item.Msg.Seq >= begin && item.Msg.Seq <= end {
				converted := convert(item.Msg)
				converted.ConversationID = block.ConversationID
				result = append(result, converted)
			}
		}
	}
	if err = cursor.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Seq < result[j].Seq })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// FindOne returns one message by server message ID.
// FindOne returns one message. The projection must keep conversation_id:
// positional projections otherwise return only _id and the matched array
// element, which would leave the message without its conversation.
func (d *DAO) FindOne(ctx context.Context, serverMsgID string) (*model.Message, error) {
	block := &blockDoc{}
	opts := options.FindOne().SetProjection(bson.M{"msgs.$": 1, "conversation_id": 1, "block_idx": 1})
	err := d.blocks.FindOne(ctx, bson.M{"msgs.msg.server_msg_id": serverMsgID}, opts).Decode(block)
	if err != nil {
		return nil, err
	}
	if len(block.Msgs) == 0 {
		return nil, mongo.ErrNoDocuments
	}
	converted := convert(block.Msgs[0].Msg)
	converted.ConversationID = block.ConversationID
	return converted, nil
}

// Recall marks a message revoked.
func (d *DAO) Recall(ctx context.Context, serverMsgID string) error {
	_, err := d.blocks.UpdateOne(ctx,
		bson.M{"msgs.msg.server_msg_id": serverMsgID},
		bson.M{"$set": bson.M{
			"msgs.$.revoke":     1,
			"msgs.$.msg.status": model.MessageRecalled,
		}},
	)
	return err
}

// DeleteBefore removes messages older than cutoff from aggregate documents,
// then deletes empty blocks. It is used by zim-crontask retention.
func (d *DAO) DeleteBefore(ctx context.Context, cutoff int64) (int64, error) {
	result, err := d.blocks.UpdateMany(ctx, bson.M{"msgs.msg.send_time": bson.M{"$lt": cutoff}},
		bson.M{"$pull": bson.M{"msgs": bson.M{"msg.send_time": bson.M{"$lt": cutoff}}}})
	if err != nil {
		return 0, err
	}
	if _, err = d.blocks.DeleteMany(ctx, bson.M{"msgs": bson.M{"$size": 0}}); err != nil {
		return result.ModifiedCount, err
	}
	return int64(result.ModifiedCount), nil
}

func convert(doc messageDoc) *model.Message {
	return &model.Message{
		ServerMsgID: doc.ServerMsgID, ClientMsgID: doc.ClientMsgID, SendID: doc.SendID,
		RecvID: doc.RecvID, GroupID: doc.GroupID, MsgType: doc.MsgType, Content: doc.Content,
		AtUserIDs: doc.AtUserIDs, Seq: doc.Seq, SendTime: doc.SendTime, Status: doc.Status,
	}
}

// MaxSeq returns the highest committed sequence in a conversation.
func (d *DAO) MaxSeq(ctx context.Context, conversationID string) (int64, error) {
	block := &blockDoc{}
	err := d.blocks.FindOne(ctx,
		bson.M{"conversation_id": conversationID},
		options.FindOne().SetProjection(bson.M{"msgs.msg.seq": 1}).
			SetSort(bson.M{"msgs.msg.seq": -1})).Decode(block)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var max int64
	for _, item := range block.Msgs {
		if item.Msg.Seq > max {
			max = item.Msg.Seq
		}
	}
	return max, nil
}
