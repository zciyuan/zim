// Package notification broadcasts announcements to online users.
package notification

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// UserSource lists target users.
type UserSource interface {
	ListIDsAfter(ctx context.Context, after int64, limit int) ([]int64, error)
}

// KafkaBroadcaster publishes one system message to all users.
type KafkaBroadcaster struct {
	producer pipe.Producer
	users    UserSource
}

// NewKafkaBroadcaster returns a broadcaster.
func NewKafkaBroadcaster(producer pipe.Producer, users UserSource) *KafkaBroadcaster {
	return &KafkaBroadcaster{producer: producer, users: users}
}

// Broadcast publishes an online push event for every user.
func (b *KafkaBroadcaster) Broadcast(ctx context.Context, id int64, title, content string) {
	if b == nil || b.producer == nil {
		return
	}
	cursor := int64(0)
	for {
		uids, err := b.users.ListIDsAfter(ctx, cursor, 5000)
		if err != nil || len(uids) == 0 {
			return
		}
		cursor = uids[len(uids)-1]
		event := &messagesvc.Event{
			ServerMsgID:    "notice-" + strconv.FormatInt(id, 10),
			ClientMsgID:    "notice-" + strconv.FormatInt(id, 10) + "-" + strconv.FormatInt(cursor, 10),
			ConversationID: "n_system",
			Seq:            id,
			MsgType:        1,
			Content:        `{"title":` + quote(title) + `,"content":` + quote(content) + `}`,
			SendTime:       time.Now().UnixMilli(),
			Recipients:     uids,
		}
		value, err := json.Marshal(event)
		if err != nil {
			return
		}
		_ = b.producer.Publish(ctx, pipe.Message{Topic: "zim.pipe.toPush", Key: "n_system", Value: value})
		if len(uids) < 5000 {
			return
		}
	}
}

func quote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
