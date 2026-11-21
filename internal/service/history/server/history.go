// Package history exposes read-only message history use cases.
package history

import (
	"context"
	"strings"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Repository reads message blocks.
type Repository interface {
	FindBySeq(ctx context.Context, conversationID string, begin, end int64, limit int) ([]*model.Message, error)
	Search(ctx context.Context, conversationID, keyword string, limit int) ([]*model.Message, error)
}

// Service implements message history reads.

// New returns a history service.
func New(repo Repository) *Service {
	return &Service{repo: repo}
}

// Pull returns messages in [begin, end].
func (s *Service) Pull(ctx context.Context, conversationID string, begin, end int64,
	limit int) ([]*model.Message, bool, error) {
	if conversationID == "" || begin < 0 || end < begin {
		return nil, false, errs.ErrMessageEmpty
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	messages, err := s.repo.FindBySeq(ctx, conversationID, begin, end, limit)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	return messages, len(messages) >= limit, nil
}

// Search returns conversation matches for a keyword.
func (s *Service) Search(ctx context.Context, conversationID, keyword string, limit int) ([]*model.Message, error) {
	if conversationID == "" || strings.TrimSpace(keyword) == "" {
		return nil, errs.ErrMessageEmpty
	}
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	messages, err := s.repo.Search(ctx, conversationID, keyword, limit)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return messages, nil
}
