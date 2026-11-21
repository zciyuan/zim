package device

import (
	"context"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
)

// Repository is the device persistence surface.
type Repository interface {
	Upsert(ctx context.Context, device *model.Device) error
	EnsureSlot(ctx context.Context, ownerID int64, platform int16, deviceID string) (int16, error)
	List(ctx context.Context, ownerID int64) ([]*model.Device, error)
	Deactivate(ctx context.Context, ownerID int64, platform int16, now int64) error
	DeactivateSlot(ctx context.Context, ownerID int64, platform, slot int16, now int64) error
	ActiveTargets(ctx context.Context, ownerIDs []int64) ([]*model.Device, error)
	ListKeys(ctx context.Context, ownerIDs []int64) ([]int64, error)
}

// Service implements device registration and lookup.
type Service struct {
	repo Repository
	now  func() time.Time
}
