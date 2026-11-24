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
	// ADR-0043: burn-after-reading and business-card extension.
	BurnTTL    int32
	ExtType    int32
	ExtPayload string
}

// SendResult is the authoritative send response.
type SendResult struct {
	ServerMsgID string
	Seq         int64
	SendTime    int64
	UnreadCount int64
	Duplicate   bool
	Recipients  []int64
	// ADR-0043: group burn countdown start (0 for single-chat until read).
	BurnExpireAt int64
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
	UID          int64  `json:"uid"`
	ClientMsgID  string `json:"client_msg_id"`
	ServerMsgID  string `json:"server_msg_id"`
	Seq          int64  `json:"seq"`
	SendTime     int64  `json:"send_time"`
	Duplicate    bool   `json:"duplicate,omitempty"`
	BurnExpireAt int64  `json:"burn_expire_at,omitempty"`
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
	// ADR-0043: burn-after-reading metadata carried through the pipeline.
	BurnTTL        int32  `json:"burn_ttl,omitempty"`
	BurnExpireAt   int64  `json:"burn_expire_at,omitempty"`
	MaxRetentionAt int64  `json:"max_retention_at,omitempty"`
	SecurityFlags  uint32 `json:"security_flags,omitempty"`
	ExtType        int32  `json:"ext_type,omitempty"`
	ExtPayload     string `json:"ext_payload,omitempty"`
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
	// ADR-0043: group security state.
	MuteAll             bool
	DefaultMuteFlags    int32
	ScheduledMuteActive bool
	IsWhitelist         bool
	MemberMuteFlags     int32
	MemberMuteUntil     int64
	NewMemberUntil      int64
	AntiSpamEnable      bool
	AntiSpamWindowSec   int32
	AntiSpamMaxMsgs     int32
	SensitiveAutoMuteS  int32
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
	// ADR-0043: burn operations.
	Burn(ctx context.Context, serverMsgID string) error
	StartBurnCountdown(ctx context.Context, serverMsgID string, expireAt int64) error
	FindBurnExpired(ctx context.Context, now int64, limit int) ([]*model.Message, error)
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
	policy    GroupPolicy
	webhook   webhook.Notifier
}

// SensitiveFilter checks message content before persistence.
type SensitiveFilter interface {
	Check(ctx context.Context, content string) (bool, error)
}

// GroupPolicy enforces the ADR-0043 group-level anti-spam window and tiered
// auto-mute (§3.1.4): the sliding window is kept in Redis, and the offense
// counter maps 1st→5min, 2nd→1h, 3rd+→24h mute. It also carries the
// sensitive-word auto-mute.
type GroupPolicy interface {
	// CheckSpam records one send in the sliding window and returns the offense
	// tier when the threshold is exceeded (0 = allowed, 1..3 = tier).
	CheckSpam(ctx context.Context, gid, uid int64, windowSec, maxMsgs int32) (int, error)
	// AutoMute mutes uid for seconds with system operator 0.
	AutoMute(ctx context.Context, gid, uid int64, seconds int64) error
}
