// Package maintenance implements periodic cleanup use cases.
package maintenance

import (
	"context"
	"time"

	"github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// FriendCleaner removes stale pending friend requests.
type FriendCleaner interface {
	DeleteExpiredRequests(ctx context.Context, before int64) (int64, error)
}

// MessageCleaner removes expired message documents.
type MessageCleaner interface {
	DeleteBefore(ctx context.Context, cutoff int64) (int64, error)
}

// ChangeCompactor folds append-only change logs into snapshots.
type ChangeCompactor interface {
	CompactAll(ctx context.Context, entities []string) error
}

// CallTimeouter marks stale ringing calls as timeout.
type CallTimeouter interface {
	TimeoutCalls(ctx context.Context, cutoff int64) ([]*rtc.Call, error)
}

// Stats reports one maintenance run.
type Stats struct {
	FriendRequests int64
	MessageBlocks  int64
	CallsTimedOut  int
}

// New returns a maintenance service.
func New(friends FriendCleaner, messages MessageCleaner, retention time.Duration,
	compactors ...ChangeCompactor) *Service {
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	service := &Service{friends: friends, messages: messages, retention: retention, now: time.Now}
	if len(compactors) > 0 {
		service.changes = compactors[0]
	}
	return service
}

// Run executes all maintenance tasks.
func (s *Service) Run(ctx context.Context) (*Stats, error) {
	now := s.now()
	stats := &Stats{}
	if s.friends != nil {
		count, err := s.friends.DeleteExpiredRequests(ctx, now.Add(-7*24*time.Hour).UnixMilli())
		if err != nil {
			return stats, errs.ErrInternal.WithCause(err)
		}
		stats.FriendRequests = count
	}
	if s.messages != nil {
		count, err := s.messages.DeleteBefore(ctx, now.Add(-s.retention).UnixMilli())
		if err != nil {
			return stats, errs.ErrInternal.WithCause(err)
		}
		stats.MessageBlocks = count
	}
	if s.changes != nil {
		if err := s.changes.CompactAll(ctx, []string{
			"conversation", "friend", "group_member",
		}); err != nil {
			return stats, errs.ErrInternal.WithCause(err)
		}
	}
	return stats, nil
}

// SetCallTimeouter enables RTC ringing timeout cleanup.
func (s *Service) SetCallTimeouter(calls CallTimeouter) { s.calls = calls }

// TimeoutCalls expires ringing calls older than 30 seconds.
func (s *Service) TimeoutCalls(ctx context.Context) ([]*rtc.Call, error) {
	if s.calls == nil {
		return nil, nil
	}
	return s.calls.TimeoutCalls(ctx, s.now().Add(-30*time.Second).UnixMilli())
}
