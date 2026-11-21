// Package notification provides admin announcements.
package notification

import (
	"context"
	"time"
)

// Notification is an admin-published announcement.
type Notification struct {
	ID          int64  `json:"id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	CreatedAt   int64  `json:"created_at"`
	PublishedAt int64  `json:"published_at"`
}

// Repository stores notifications.
type Repository interface {
	ListNotifications(ctx context.Context, since int64, limit int) ([]*Notification, error)
	CreateNotification(ctx context.Context, title, content string, now int64) (int64, error)
	MarkRead(ctx context.Context, uid, id, now int64) error
	ReadIDs(ctx context.Context, uid int64) ([]int64, error)
}

// Broadcaster delivers an announcement after persistence.
type Broadcaster interface {
	Broadcast(ctx context.Context, id int64, title, content string)
}

// Service manages announcements.
type Service struct {
	repo        Repository
	broadcaster Broadcaster
}

// SetBroadcaster enables online broadcast after publishing.
func (s *Service) SetBroadcaster(broadcaster Broadcaster) {
	s.broadcaster = broadcaster
}

// New returns a notification service.
func New(repo Repository) *Service { return &Service{repo: repo} }

// MarkRead records an announcement read.
func (s *Service) MarkRead(ctx context.Context, uid, id int64) error {
	return s.repo.MarkRead(ctx, uid, id, time.Now().UnixMilli())
}

// List returns announcements newer than since.
func (s *Service) List(ctx context.Context, since int64, limit int) ([]*Notification, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	return s.repo.ListNotifications(ctx, since, limit)
}

// Publish creates an announcement.
func (s *Service) Publish(ctx context.Context, title, content string, now int64) (int64, error) {
	id, err := s.repo.CreateNotification(ctx, title, content, now)
	if err == nil && s.broadcaster != nil {
		s.broadcaster.Broadcast(ctx, id, title, content)
	}
	return id, err
}

// ReadIDs returns announcements read by a user.
func (s *Service) ReadIDs(ctx context.Context, uid int64) ([]int64, error) {
	return s.repo.ReadIDs(ctx, uid)
}
