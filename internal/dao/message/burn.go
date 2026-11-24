package message

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zciyuan/ZIM/internal/model"
)

// burnCollection holds burn-after-reading payloads, physically erased by
// MongoDB TTL monitors (ADR-0043 §2.4.1).
const burnCollection = "burn_messages"

// burnDoc is one burn payload. _id = server_msg_id.
type burnDoc struct {
	ID             string `bson:"_id"`
	ConversationID string `bson:"conversation_id"`
	Seq            int64  `bson:"seq"`
	SendID         int64  `bson:"send_id"`
	Payload        string `bson:"payload"`
	ExpireAt       int64  `bson:"expire_at"`        // TTL index: destroy when reached
	MaxRetentionAt int64  `bson:"max_retention_at"` // TTL index: unread backstop
}

// EnsureBurnIndexes creates the TTL indexes that physically erase burn
// payloads. TTL monitors fire within ~60s of the indexed instant, so the
// burn_messages read path must treat an expired-but-present doc as burned.
func (d *DAO) EnsureBurnIndexes(ctx context.Context) error {
	_, err := d.burn.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "expire_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
		{Keys: bson.D{{Key: "max_retention_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
	})
	return err
}

// StoreBurnPayload writes the burn content into the TTL collection. The main
// block keeps only light metadata (content empty, no search tokens).
func (d *DAO) StoreBurnPayload(ctx context.Context, msg *model.Message) error {
	_, err := d.burn.ReplaceOne(ctx,
		bson.M{"_id": msg.ServerMsgID},
		&burnDoc{
			ID: msg.ServerMsgID, ConversationID: msg.ConversationID, Seq: msg.Seq,
			SendID: msg.SendID, Payload: msg.Content,
			ExpireAt: msg.BurnExpireAt, MaxRetentionAt: msg.MaxRetentionAt,
		},
		options.Replace().SetUpsert(true),
	)
	return err
}

// PeekBurnPayload returns the raw payload of a burn message even after its
// deadline passed. The sweep needs it to extract object keys before physical
// erasure (ADR-0043 §2.4.3).
func (d *DAO) PeekBurnPayload(ctx context.Context, serverMsgID string) (string, error) {
	doc := &burnDoc{}
	err := d.burn.FindOne(ctx, bson.M{"_id": serverMsgID}).Decode(doc)
	if err != nil {
		return "", err
	}
	return doc.Payload, nil
}

// Burn erases one burn message: it deletes the TTL payload and flags the main
// block copy burned so reads return the tombstone immediately (ADR-0043
// §2.4.2), keeping seq continuity intact.
func (d *DAO) Burn(ctx context.Context, serverMsgID string) error {
	if _, err := d.burn.DeleteOne(ctx, bson.M{"_id": serverMsgID}); err != nil {
		return err
	}
	_, err := d.blocks.UpdateOne(ctx,
		bson.M{"msgs.msg.server_msg_id": serverMsgID},
		bson.M{"$set": bson.M{
			"msgs.$.msg.is_burned":     true,
			"msgs.$.msg.content":       "",
			"msgs.$.msg.search_tokens": bson.A{},
		}},
	)
	return err
}

// StartBurnCountdown sets the single-chat countdown deadline after
// BurnReadAck: burn_expire_at = server_now + ttl, in both the TTL payload and
// the light metadata.
func (d *DAO) StartBurnCountdown(ctx context.Context, serverMsgID string, expireAt int64) error {
	if _, err := d.burn.UpdateOne(ctx, bson.M{"_id": serverMsgID},
		bson.M{"$set": bson.M{"expire_at": expireAt}}); err != nil {
		return err
	}
	_, err := d.blocks.UpdateOne(ctx,
		bson.M{"msgs.msg.server_msg_id": serverMsgID},
		bson.M{"$set": bson.M{"msgs.$.msg.burn_expire_at": expireAt}},
	)
	return err
}

// FindBurnExpired returns burn messages whose countdown deadline or unread
// retention passed. The main index stores the light metadata, so the sweep
// only needs the block scan (ADR-0043 §2.4.3, G-01).
func (d *DAO) FindBurnExpired(ctx context.Context, now int64, limit int) ([]*model.Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filter := bson.M{
		"msgs.msg.burn_ttl":  bson.M{"$gt": 0},
		"msgs.msg.is_burned": bson.M{"$ne": true},
		"$or": bson.A{
			bson.M{"msgs.msg.burn_expire_at": bson.M{"$gt": 0, "$lte": now}},
			bson.M{"msgs.msg.max_retention_at": bson.M{"$gt": 0, "$lte": now}},
		},
	}
	cursor, err := d.blocks.Find(ctx, filter, options.Find().SetLimit(int64(limit*2)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	result := make([]*model.Message, 0, limit)
scan:
	for cursor.Next(ctx) {
		block := &blockDoc{}
		if err = cursor.Decode(block); err != nil {
			return nil, err
		}
		for _, item := range block.Msgs {
			doc := item.Msg
			if doc.BurnTTL <= 0 || doc.IsBurned {
				continue
			}
			if (doc.BurnExpireAt > 0 && now >= doc.BurnExpireAt) ||
				(doc.MaxRetentionAt > 0 && now >= doc.MaxRetentionAt) {
				msg := convert(doc)
				msg.ConversationID = block.ConversationID
				result = append(result, msg)
				if len(result) >= limit {
					break scan
				}
			}
		}
	}
	if err = cursor.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// LoadBurnPayloads fills Content for burn messages whose payload still exists.
// Messages whose payload is already TTL-erased or expired come back with
// IsBurned = true and empty Content, so Pull/SyncMissing return tombstones
// without breaking seq continuity (ADR-0043 §2.4.2).
func (d *DAO) LoadBurnPayloads(ctx context.Context, now int64, messages []*model.Message) error {
	ids := make([]string, 0, 4)
	index := make(map[string]*model.Message, 4)
	for _, msg := range messages {
		if msg == nil || msg.BurnTTL <= 0 || msg.IsBurned {
			continue
		}
		if msg.BurnExpireAt > 0 && now >= msg.BurnExpireAt {
			msg.IsBurned = true
			msg.Content = ""
			continue
		}
		if msg.MaxRetentionAt > 0 && now >= msg.MaxRetentionAt {
			msg.IsBurned = true
			msg.Content = ""
			continue
		}
		ids = append(ids, msg.ServerMsgID)
		index[msg.ServerMsgID] = msg
	}
	if len(ids) == 0 {
		return nil
	}
	cursor, err := d.burn.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close(ctx) }()
	found := make(map[string]struct{}, len(ids))
	for cursor.Next(ctx) {
		doc := &burnDoc{}
		if err = cursor.Decode(doc); err != nil {
			return err
		}
		found[doc.ID] = struct{}{}
		if msg, ok := index[doc.ID]; ok {
			msg.Content = doc.Payload
		}
	}
	if err = cursor.Err(); err != nil {
		return err
	}
	// Missing payload: the TTL monitor already erased it.
	for _, msg := range index {
		if _, ok := found[msg.ServerMsgID]; !ok {
			msg.IsBurned = true
			msg.Content = ""
		}
	}
	return nil
}
