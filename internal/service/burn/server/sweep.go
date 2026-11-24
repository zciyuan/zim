// Package burn implements the burn-after-reading timer sweep (ADR-0043 §2.4.3,
// G-01). zim-crontask runs it under its Redis leader lock.
package burn

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	messagedao "github.com/zciyuan/ZIM/internal/dao/message"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// Repository is the burn persistence surface the sweep needs.
type Repository interface {
	FindBurnExpired(ctx context.Context, now int64, limit int) ([]*burnMessage, error)
	Burn(ctx context.Context, serverMsgID string) error
}

// EventPublisher publishes one event to a Kafka topic.
type EventPublisher interface {
	Publish(ctx context.Context, topic string, key string, value []byte) error
}

// SweepResult reports one sweep run.
type SweepResult struct {
	Erased      int
	Pushed      int
	MediaQueued int
}

// Sweeper erases expired burn messages and queues the BurnDelete push and the
// media deletion. Both side effects are at-least-once: clients dedupe the
// push by server_msg_id and object deletion is idempotent.
type Sweeper struct {
	repo   Repository
	toPush EventPublisher
	media  EventPublisher
	now    func() time.Time
}

// New returns a sweep service. toPush/媒体 publishers may be nil to degrade
// gracefully (physical erase still happens).
func New(repo Repository, toPush, media EventPublisher) *Sweeper {
	return &Sweeper{repo: repo, toPush: toPush, media: media, now: time.Now}
}

// Run erases up to limit expired burn messages.
func (s *Sweeper) Run(ctx context.Context, limit int) (*SweepResult, error) {
	if s.repo == nil || limit <= 0 {
		return &SweepResult{}, nil
	}
	expired, err := s.repo.FindBurnExpired(ctx, s.now().UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	result := &SweepResult{}
	for _, msg := range expired {
		if err = s.repo.Burn(ctx, msg.ServerMsgID); err != nil {
			metrics.Default.Inc("zim_burn_sweep_total", map[string]string{"result": "error"})
			log.Warnf("burn erase failed: server_msg_id = %s, err = %v", msg.ServerMsgID, err)
			continue
		}
		result.Erased++
		metrics.Default.Inc("zim_burn_sweep_total", map[string]string{"result": "ok"})
		if s.toPush != nil {
			if pushErr := s.publishDelete(ctx, msg); pushErr != nil {
				log.Warnf("burn delete push queue failed: server_msg_id = %s, err = %v", msg.ServerMsgID, pushErr)
			} else {
				result.Pushed++
			}
		}
		if s.media != nil && msg.MsgType >= 2 && msg.MsgType <= 5 {
			if mediaErr := s.publishMediaDelete(ctx, msg); mediaErr != nil {
				log.Warnf("burn media delete queue failed: server_msg_id = %s, err = %v", msg.ServerMsgID, mediaErr)
			} else {
				result.MediaQueued++
			}
		}
	}
	return result, nil
}

func (s *Sweeper) publishDelete(ctx context.Context, msg *burnMessage) error {
	event, err := json.Marshal(map[string]any{
		"type":            "burn.delete",
		"server_msg_id":   msg.ServerMsgID,
		"conversation_id": msg.ConversationID,
		"seq":             msg.Seq,
		"send_id":         msg.SendID,
		"recv_id":         msg.RecvID,
		"recipients":      []int64{msg.SendID, msg.RecvID},
	})
	if err != nil {
		return err
	}
	return s.toPush.Publish(ctx, define.TopicToPush, msg.ConversationID, event)
}

func (s *Sweeper) publishMediaDelete(ctx context.Context, msg *burnMessage) error {
	event, err := json.Marshal(map[string]any{
		"server_msg_id":   msg.ServerMsgID,
		"conversation_id": msg.ConversationID,
		"msg_type":        msg.MsgType,
		"object_key":      objectKey(msg.Content),
	})
	if err != nil {
		return err
	}
	return s.media.Publish(ctx, define.TopicBurnMediaDelete, msg.ServerMsgID, event)
}

// KafkaPublisher adapts zimfw/pipe.Producer to EventPublisher.
type KafkaPublisher struct {
	producer pipe.Producer
}

// NewKafkaPublisher returns a Kafka-backed event publisher.
func NewKafkaPublisher(producer pipe.Producer) *KafkaPublisher {
	return &KafkaPublisher{producer: producer}
}

// Publish sends one JSON record to the topic.
func (p *KafkaPublisher) Publish(ctx context.Context, topic, key string, value []byte) error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: topic, Key: key, Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

// MessageRepo adapts the message DAO to the sweep repository. It shadows the
// dao.Message type so this package stays independent of dao internals.
type MessageRepo struct {
	dao *messagedao.DAO
}

// NewMessageRepo returns the DAO-backed repository.
func NewMessageRepo(dao *messagedao.DAO) *MessageRepo {
	return &MessageRepo{dao: dao}
}

// FindBurnExpired delegates to the DAO and converts rows. Media payloads are
// peeked before erasure so the sweep can extract object keys.
func (r *MessageRepo) FindBurnExpired(ctx context.Context, now int64, limit int) ([]*burnMessage, error) {
	messages, err := r.dao.FindBurnExpired(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]*burnMessage, 0, len(messages))
	for _, msg := range messages {
		row := &burnMessage{
			ServerMsgID: msg.ServerMsgID, ConversationID: msg.ConversationID,
			Seq: msg.Seq, SendID: msg.SendID, RecvID: msg.RecvID, MsgType: msg.MsgType,
		}
		if msg.MsgType >= 2 && msg.MsgType <= 5 {
			if payload, peekErr := r.dao.PeekBurnPayload(ctx, msg.ServerMsgID); peekErr == nil {
				row.Content = payload
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Burn delegates to the DAO.
func (r *MessageRepo) Burn(ctx context.Context, serverMsgID string) error {
	return r.dao.Burn(ctx, serverMsgID)
}
