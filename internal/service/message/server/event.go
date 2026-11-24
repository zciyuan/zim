package message

import (
	"context"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// NewKafkaPublisher returns an EventPublisher for one topic.
func NewKafkaPublisher(producer pipe.Producer, topic string) *KafkaPublisher {
	return &KafkaPublisher{producer: producer, topic: topic}
}

// Publish encodes and publishes the event.
func (p *KafkaPublisher) Publish(ctx context.Context, event *Event) error {
	if p == nil || p.producer == nil || event == nil {
		return nil
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: p.topic, Key: event.ConversationID, Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

// DecodeEvent decodes a toPush record.
func DecodeEvent(value []byte) (*Event, error) {
	event := &Event{}
	if err := json.Unmarshal(value, event); err != nil {
		return nil, err
	}
	return event, nil
}

// WithPublisher enables push event publication.
func (s *Service) WithPublisher(publisher EventPublisher) *Service {
	s.pub = publisher
	return s
}

func (s *Service) publish(ctx context.Context, msg *model.Message, recipients []int64) {
	if s.pub == nil {
		return
	}
	event := &Event{
		ServerMsgID: msg.ServerMsgID, ClientMsgID: msg.ClientMsgID,
		ConversationID: msg.ConversationID, Seq: msg.Seq, SendID: msg.SendID,
		RecvID: msg.RecvID, GroupID: msg.GroupID, MsgType: msg.MsgType,
		Content: msg.Content, AtUserIDs: msg.AtUserIDs, SendTime: msg.SendTime,
		Recipients: recipients,
		BurnTTL:    msg.BurnTTL, BurnExpireAt: msg.BurnExpireAt,
		MaxRetentionAt: msg.MaxRetentionAt, SecurityFlags: msg.SecurityFlags,
		ExtType: msg.ExtType, ExtPayload: msg.ExtPayload,
	}
	if err := s.pub.Publish(ctx, event); err != nil {
		log.Warnf("publish message event failed: server_msg_id = %s, err = %v", msg.ServerMsgID, err)
	}
}
