// Package pushoffline implements offline push delivery (ADR-0024).
package pushoffline

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// Publisher writes offline push events to Kafka.
type Publisher struct {
	producer pipe.Producer
	topic    string
}

// NewPublisher returns a publisher for zim.pipe.toOfflinePush.
func NewPublisher(producer pipe.Producer) *Publisher {
	return &Publisher{producer: producer, topic: define.TopicToOfflinePush}
}

// Publish enqueues one event keyed by uid.
func (p *Publisher) Publish(ctx context.Context, event *Event) error {
	if p == nil || p.producer == nil || event == nil {
		return nil
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	key := event.ConversationID
	if event.UID > 0 {
		key = strconv.FormatInt(event.UID, 10)
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: p.topic, Key: key, Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

// Decode parses one toOfflinePush record.
func Decode(value []byte) (*Event, error) {
	event := &Event{}
	if err := json.Unmarshal(value, event); err != nil {
		return nil, err
	}
	return event, nil
}
