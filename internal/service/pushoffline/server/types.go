package pushoffline

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
)

// Event is one offline push request, published on zim.pipe.toOfflinePush.
type Event struct {
	UID            int64    `json:"uid"`
	Uids           []int64  `json:"uids,omitempty"`
	ServerMsgID    string   `json:"server_msg_id"`
	ConversationID string   `json:"conversation_id"`
	Seq            int64    `json:"seq"`
	SendID         int64    `json:"send_id"`
	MsgType        int32    `json:"msg_type"`
	Content        string   `json:"content,omitempty"`
	SendTime       int64    `json:"send_time"`
	AtUserIDs      []string `json:"at_user_ids,omitempty"`
	// BurnTTL > 0 masks the push body (ADR-0043 §2.1.6).
	BurnTTL int32 `json:"burn_ttl,omitempty"`
}

// Notification is one provider payload.
type Notification struct {
	UID      int64
	Platform int16
	Provider string
	Token    string
	Title    string
	Body     string
	Badge    int32
	Extra    map[string]string
	VoIP     bool
	Priority int
}

// Result is the per-token provider outcome.
type Result struct {
	Token   string
	OK      bool
	Invalid bool
	Err     error
}

// Provider sends notifications through one vendor channel.
type Provider interface {
	Name() string
	Send(ctx context.Context, notifications []Notification) ([]Result, error)
}

// DeviceSource loads the devices that should receive an offline push.
type DeviceSource interface {
	Targets(ctx context.Context, ownerIDs []int64) ([]*model.Device, error)
}

// Limiter enforces the per-user offline push rate (DL-091).
type Limiter interface {
	Allow(ctx context.Context, uid int64) (bool, error)
}

// Dispatcher turns an Event into provider calls.
type Dispatcher struct {
	devices  DeviceSource
	provider Provider
	limiter  Limiter
	now      func() time.Time
}

// RedisLimiter is a fixed-window limiter backed by Redis.
type RedisLimiter struct {
	client redis.UniversalClient
	limit  int64
	window time.Duration
	script *redis.Script
}
