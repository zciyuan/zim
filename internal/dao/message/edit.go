package message

import (
	"context"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zciyuan/ZIM/internal/model"
)

// searchBlockLimit bounds how many aggregate blocks a keyword search scans.
// 50 blocks ≈ 5000 recent messages per conversation; older history belongs to
// OpenSearch (ZIM-01 §21.1), not to a Mongo regex.
const searchBlockLimit = 50

// UpdateContent rewrites one message body (edit).
func (d *DAO) UpdateContent(ctx context.Context, serverMsgID, content string) error {
	_, err := d.blocks.UpdateOne(ctx,
		bson.M{"msgs.msg.server_msg_id": serverMsgID},
		bson.M{"$set": bson.M{"msgs.$.msg.content": content}},
	)
	return err
}

// MarkDeleted flags one message as deleted for both sides.
func (d *DAO) MarkDeleted(ctx context.Context, serverMsgID string) error {
	_, err := d.blocks.UpdateOne(ctx,
		bson.M{"msgs.msg.server_msg_id": serverMsgID},
		bson.M{"$set": bson.M{"msgs.$.msg.status": model.MessageDeleted}},
	)
	return err
}

// Search returns messages of one conversation whose body contains keyword.
// Results are ordered newest first; the scan is bounded by searchBlockLimit.
func (d *DAO) Search(ctx context.Context, conversationID, keyword string, limit int) ([]*model.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	needle := strings.ToLower(strings.TrimSpace(keyword))
	if needle == "" {
		return []*model.Message{}, nil
	}

	filter := bson.M{"conversation_id": conversationID}
	if tokens := searchTokens(keyword); len(tokens) > 0 {
		filter["msgs.msg.search_tokens"] = bson.M{"$all": tokens}
	}
	cursor, err := d.blocks.Find(ctx, filter,
		options.Find().SetSort(bson.M{"block_idx": -1}).SetLimit(searchBlockLimit),
	)
	if err != nil {
		// Legacy documents have no search_tokens; fall back to the bounded
		// block scan rather than failing the search.
		cursor, err = d.blocks.Find(ctx,
			bson.M{"conversation_id": conversationID},
			options.Find().SetSort(bson.M{"block_idx": -1}).SetLimit(searchBlockLimit),
		)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	matches := make([]*model.Message, 0, limit)
	for cursor.Next(ctx) {
		block := blockDoc{}
		if err = cursor.Decode(&block); err != nil {
			return nil, err
		}
		for i := len(block.Msgs) - 1; i >= 0; i-- {
			msg := block.Msgs[i].Msg
			if !strings.Contains(strings.ToLower(msg.Content), needle) {
				continue
			}
			matches = append(matches, convert(msg))
			if len(matches) >= limit {
				func() { _ = cursor.Close(ctx) }()
				sort.Slice(matches, func(a, b int) bool { return matches[a].Seq > matches[b].Seq })
				return matches, nil
			}
		}
	}
	if err = cursor.Err(); err != nil {
		return nil, err
	}
	sort.Slice(matches, func(a, b int) bool { return matches[a].Seq > matches[b].Seq })
	return matches, nil
}
