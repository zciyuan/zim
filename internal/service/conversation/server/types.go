package conversation

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/service/changelog/server"
)

// Item is one conversation returned to clients.
type Item struct {
	ConversationID string `json:"conversation_id"`
	Type           int16  `json:"type"`
	PeerID         int64  `json:"peer_id"`
	GroupID        int64  `json:"group_id"`
	RecvOpt        int16  `json:"recv_opt"`
	IsPinned       int16  `json:"is_pinned"`
	LastMsgID      string `json:"last_msg_id"`
	LastMsgSeq     int64  `json:"last_msg_seq"`
	LastMsgAt      int64  `json:"last_msg_at"`
	LastMsgBrief   string `json:"last_msg_brief"`
	ReadSeq        int64  `json:"read_seq"`
	MaxSeq         int64  `json:"max_seq"`
	Unread         int64  `json:"unread"`
	UpdatedAt      int64  `json:"updated_at"`
}

// ReadCache is the hot read_seq cache. A miss falls back to PostgreSQL.
type ReadCache interface {
	Get(ctx context.Context, conversationID string, uid int64) (int64, bool, error)
	GetMany(ctx context.Context, conversationIDs []string, uid int64) (map[string]int64, error)
	Set(ctx context.Context, conversationID string, uid, seq int64) error
}

// RedisReadCache is the Redis-backed read_seq cache.
type RedisReadCache struct {
	client redis.UniversalClient
	ttl    time.Duration
}

// Service implements conversation use cases.
type Service struct {
	repo    Repository
	cache   ReadCache
	changes *changelog.Service
	now     func() time.Time

	pending   sync.Map
	dirty     atomic.Int64
	stop      chan struct{}
	flushCh   chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

const (
	readFlushInterval = 200 * time.Millisecond
	readFlushBatch    = 500
)

type readUpdate struct {
	uid    int64
	convID string
	seq    int64
}
