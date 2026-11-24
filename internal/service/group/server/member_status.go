package server

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// kafkaMemberStatusPublisher publishes member status changes to
// zim.pipe.groupMemberStatus so voiceroom can revoke LiveKit publish rights
// (ADR-0043 §3.1.5, G-10).
type kafkaMemberStatusPublisher struct {
	producer pipe.Producer
	topic    string
}

// NewKafkaMemberStatusPublisher returns a Kafka-backed status publisher.
func NewKafkaMemberStatusPublisher(producer pipe.Producer) MemberStatusPublisher {
	return &kafkaMemberStatusPublisher{producer: producer, topic: define.TopicGroupMemberStatus}
}

// memberStatusEvent is one status change record.
type memberStatusEvent struct {
	Type        string `json:"type"` // mute / unmute / kick
	GroupID     int64  `json:"group_id"`
	UID         int64  `json:"uid"`
	MuteFlags   int32  `json:"mute_flags,omitempty"`
	MuteUntil   int64  `json:"mute_until,omitempty"`
	OperatorUID int64  `json:"operator_uid,omitempty"`
	At          int64  `json:"at"`
}

// PublishMute publishes a mute/unmute event.
func (p *kafkaMemberStatusPublisher) PublishMute(ctx context.Context, gid, uid int64, flags int32, until int64, operatorUID int64) error {
	if p == nil || p.producer == nil {
		return nil
	}
	eventType := "mute"
	if flags == 0 && until == 0 {
		eventType = "unmute"
	}
	value, err := json.Marshal(&memberStatusEvent{
		Type: eventType, GroupID: gid, UID: uid, MuteFlags: flags,
		MuteUntil: until, OperatorUID: operatorUID, At: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: p.topic, Key: strconv.FormatInt(uid, 10), Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

// PublishKick publishes a kick event.
func (p *kafkaMemberStatusPublisher) PublishKick(ctx context.Context, gid, uid int64, operatorUID int64) error {
	if p == nil || p.producer == nil {
		return nil
	}
	value, err := json.Marshal(&memberStatusEvent{
		Type: "kick", GroupID: gid, UID: uid, OperatorUID: operatorUID,
		At: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: p.topic, Key: strconv.FormatInt(uid, 10), Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}
