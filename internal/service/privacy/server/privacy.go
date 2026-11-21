package privacy

import "context"

// Repository reads and writes user privacy settings.
type Repository interface {
	GetPrivacy(ctx context.Context, uid int64) (bool, bool, error)
	SetPrivacy(ctx context.Context, uid int64, searchable, addFriend bool) error
}

// Service manages user privacy settings.
type Service struct{ repo Repository }

// New returns a privacy service.
func New(repo Repository) *Service { return &Service{repo: repo} }

// Get returns searchable and add-friend flags.
func (s *Service) Get(ctx context.Context, uid int64) (bool, bool, error) {
	return s.repo.GetPrivacy(ctx, uid)
}

// Set updates privacy flags.
func (s *Service) Set(ctx context.Context, uid int64, searchable, addFriend bool) error {
	return s.repo.SetPrivacy(ctx, uid, searchable, addFriend)
}
