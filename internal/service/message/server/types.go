package message

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	daomessage "github.com/zciyuan/ZIM/internal/dao/message"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/idem"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// AppendResult reports persisted message identity.
type AppendResult struct {
	ServerMsgID string
	Seq         int64
	Duplicate   bool
}

// SendRequest is a normalized send request for single or group chats.
type SendRequest struct {
	ClientMsgID string
	ReceiverID  int64
	GroupID     int64
	MsgType     int32
	Content     string
	AtUserIDs   []string
}

// SendResult is the authoritative send response.
type SendResult struct {
	ServerMsgID string
	Seq         int64
	SendTime    int64
	UnreadCount int64
	Duplicate   bool
	Recipients  []int64
}

// Prepared is a validated send request ready for the Kafka pipeline.
type Prepared struct {
	ConversationID string
	Recipients     []int64
	UnreadCount    int64
}

// ResultQueue exchanges a send result between node-msg and transfer.
type ResultQueue interface {
	Publish(ctx context.Context, uid int64, clientMsgID string, result *SendResult) error
	Wait(ctx context.Context, uid int64, clientMsgID string, timeout time.Duration) (*SendResult, error)
}

// PublishingResultQueue is implemented by low-latency result queues that need
// the waiter registered before the event is published. Registering after the
// publish races with transfer's result and can lose an otherwise successful ack.
type PublishingResultQueue interface {
	WaitWithPublish(ctx context.Context, uid int64, clientMsgID string, timeout time.Duration,
		publish func(context.Context) error) (*SendResult, error)
}

// Pipeline publishes a send event and waits for transfer's authoritative result.
type Pipeline struct {
	service  *Service
	producer pipe.Producer
	results  ResultQueue
	timeout  time.Duration
	now      func() time.Time
}

// RedisResultQueue exchanges one send result per client_msg_id.
type RedisResultQueue struct {
	client redis.UniversalClient
	ttl    time.Duration
}

// resultEvent is the compact Pub/Sub payload used to wake a local waiter. The
// recipients/unread fields are filled by the caller from Prepared and are not
// repeated on the broadcast channel.
type resultEvent struct {
	UID         int64  `json:"uid"`
	ClientMsgID string `json:"client_msg_id"`
	ServerMsgID string `json:"server_msg_id"`
	Seq         int64  `json:"seq"`
	SendTime    int64  `json:"send_time"`
	Duplicate   bool   `json:"duplicate,omitempty"`
}

// PubSubResultQueue delivers results over Redis Pub/Sub and keeps only a local
// waiter map, instead of consuming one Redis connection per in-flight send.
// The durable per-key list remains as a fallback when Pub/Sub is unavailable.
type PubSubResultQueue struct {
	fallback *RedisResultQueue
	mu       sync.Mutex
	waiters  map[string]chan *SendResult
	sub      *redis.PubSub
	cancel   context.CancelFunc
	done     chan struct{}
	close    sync.Once
}

// Event is the toPush pipeline payload.
type Event struct {
	Type           string   `json:"type,omitempty"`
	ServerMsgID    string   `json:"server_msg_id"`
	ClientMsgID    string   `json:"client_msg_id"`
	ConversationID string   `json:"conversation_id"`
	Seq            int64    `json:"seq"`
	SendID         int64    `json:"send_id"`
	RecvID         int64    `json:"recv_id"`
	GroupID        int64    `json:"group_id"`
	MsgType        int32    `json:"msg_type"`
	Content        string   `json:"content"`
	AtUserIDs      []string `json:"at_user_ids,omitempty"`
	SendTime       int64    `json:"send_time"`
	Recipients     []int64  `json:"recipients"`
}

// EventPublisher publishes push events.
type EventPublisher interface {
	Publish(ctx context.Context, event *Event) error
}

// KafkaPublisher publishes events to one Kafka topic.
type KafkaPublisher struct {
	producer pipe.Producer
	topic    string
}

// Membership is the subset of group state needed by the message service.
type Membership struct {
	GroupID    int64
	Role       int16
	GroupMute  int64
	MemberMute int64
	MaxMember  int32
	GroupType  int16
	MemberIDs  []int64
}

// Repository persists messages.
type Repository interface {
	Append(ctx context.Context, msg *model.Message) (*daomessage.AppendResult, error)
	FindBySeq(ctx context.Context, conversationID string, begin, end int64, limit int) ([]*model.Message, error)
	FindOne(ctx context.Context, serverMsgID string) (*model.Message, error)
	Recall(ctx context.Context, serverMsgID string) error
	// UpdateContent rewrites one message body (op:1507).
	UpdateContent(ctx context.Context, serverMsgID, content string) error
	// MarkDeleted flags one message as deleted (op:1508).
	MarkDeleted(ctx context.Context, serverMsgID string) error
	// Search returns conversation matches for a keyword (op:1512).
	Search(ctx context.Context, conversationID, keyword string, limit int) ([]*model.Message, error)
}

// Sequence allocates per-conversation sequences.
type Sequence interface {
	Next(ctx context.Context, conversationID string) (int64, error)
}

// Idempotency reserves client_msg_id keys.
type Idempotency interface {
	Reserve(ctx context.Context, uid int64, clientMsgID string) (*idem.Result, bool, error)
	Complete(ctx context.Context, uid int64, clientMsgID string, result *idem.Result) error
	Release(ctx context.Context, uid int64, clientMsgID string) error
}

// ConversationStore updates conversation previews.
type ConversationStore interface {
	Touch(ctx context.Context, conv *model.Conversation) error
}

// Directory resolves social/group state.
type Directory interface {
	Membership(ctx context.Context, gid, uid int64) (*Membership, error)
	Relation(ctx context.Context, uid, target int64) (friend bool, black bool, err error)
}

// Service implements message send/pull/recall use cases.
type Service struct {
	repo      Repository
	seq       Sequence
	idem      Idempotency
	conv      ConversationStore
	dir       Directory
	ids       *snowflake.Generator
	now       func() time.Time
	pub       EventPublisher
	sensitive SensitiveFilter
	webhook   webhook.Notifier
}

// SensitiveFilter checks message content before persistence.
type SensitiveFilter interface {
	Check(ctx context.Context, content string) (bool, error)
}
