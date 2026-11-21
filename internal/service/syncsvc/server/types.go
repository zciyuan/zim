package syncsvc

import (
	"context"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/conversation/server"
)

// Conversations is the conversation read surface used by sync.
type Conversations interface {
	// List returns conversations ordered by pinned/time desc.
	List(ctx context.Context, ownerID int64, offset, limit int) ([]*conversation.Item, error)
	// VisibleRange returns the inclusive seq window uid may read in convID.
	VisibleRange(ctx context.Context, uid int64, convID string) (int64, int64, error)
}

// History is the message read surface used by 1514.
type History interface {
	Pull(ctx context.Context, conversationID string, begin, end int64, limit int) ([]*model.Message, bool, error)
}

// Versions reads the current per-user change version.
type Versions interface {
	Current(ctx context.Context, uid int64) (int64, error)
}

// State is the SyncState returned by login.
type State struct {
	ConversationVersion int64
	FriendVersion       int64
	MaxSeqs             map[string]int64
	Truncated           bool
}

// Request is the 1601 input.
type Request struct {
	ConversationVersion int64
	FriendVersion       int64
	MaxSeqs             map[string]int64
}

// Diff is one conversation's missing seq window (inclusive).
type Diff struct {
	ConversationID string
	FromSeq        int64
	ToSeq          int64
}

// CheckResult is the 1601 output.
type CheckResult struct {
	ConversationVersion int64
	FriendVersion       int64
	FriendChanged       bool
	FullSyncRequired    bool
	Truncated           bool
	Diffs               []Diff
	ServerTime          int64
}

// Service implements login SyncState, 1601 SyncCheck and 1514 SyncMissing.
type Service struct {
	conv     Conversations
	history  History
	versions Versions
	now      func() time.Time

	maxConversations int
	maxDiffs         int
	maxMissing       int
	pageSize         int
}
