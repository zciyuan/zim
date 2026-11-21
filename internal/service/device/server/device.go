// Package device implements the per-user device registry (ADR-0024).
package device

import (
	"context"
	"strconv"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// New returns a device service.
func New(repo Repository) *Service {
	return &Service{repo: repo, now: time.Now}
}

// Register inserts or refreshes one device. Empty push fields keep the stored
// token, so a plain login does not erase a previously registered token.
func (s *Service) Register(ctx context.Context, device *model.Device) error {
	if device == nil {
		return errs.ErrInternal
	}
	if device.Slot == 0 {
		slot, slotErr := s.repo.EnsureSlot(ctx, device.OwnerID, device.Platform, device.DeviceID)
		if slotErr != nil {
			return errs.ErrInternal.WithCause(slotErr)
		}
		device.Slot = slot
	}
	if err := multi.Validate(device.OwnerID, int32(device.Platform), int32(device.Slot)); err != nil {
		return err
	}
	now := s.now().UnixMilli()
	device.Status = 0
	device.LastActiveAt = now
	device.UpdatedAt = now
	if device.CreatedAt == 0 {
		device.CreatedAt = now
	}
	if err := s.repo.Upsert(ctx, device); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// EnsureSlot returns the persistent slot for a device, allocating one when new.
func (s *Service) EnsureSlot(ctx context.Context, ownerID int64, platform int16, deviceID string) (int16, error) {
	if deviceID == "" {
		deviceID = "srv-" + strconv.FormatInt(ownerID, 10) + "-" + strconv.Itoa(int(platform))
	}
	if err := multi.Validate(ownerID, int32(platform), 1); err != nil {
		return 0, err
	}
	slot, err := s.repo.EnsureSlot(ctx, ownerID, platform, deviceID)
	if err != nil {
		return 0, errs.ErrInternal.WithCause(err)
	}
	return slot, nil
}

// List returns the caller's devices.
func (s *Service) List(ctx context.Context, ownerID int64) ([]*model.Device, error) {
	devices, err := s.repo.List(ctx, ownerID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return devices, nil
}

// Deactivate marks one device as logged out and clears its push tokens.
func (s *Service) Deactivate(ctx context.Context, ownerID int64, platform int16) error {
	if err := multi.Validate(ownerID, int32(platform), 1); err != nil {
		return err
	}
	if err := s.repo.Deactivate(ctx, ownerID, platform, s.now().UnixMilli()); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// DeactivateSlot marks a single device slot as logged out.
func (s *Service) DeactivateSlot(ctx context.Context, ownerID int64, platform, slot int16) error {
	if err := multi.Validate(ownerID, int32(platform), int32(slot)); err != nil {
		return err
	}
	if err := s.repo.DeactivateSlot(ctx, ownerID, platform, slot, s.now().UnixMilli()); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// Targets returns the devices that can receive an offline push.
func (s *Service) Targets(ctx context.Context, ownerIDs []int64) ([]*model.Device, error) {
	devices, err := s.repo.ActiveTargets(ctx, ownerIDs)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return devices, nil
}

// Keys returns the device keys for the given owners.
func (s *Service) Keys(ctx context.Context, ownerIDs []int64) ([]int64, error) {
	keys, err := s.repo.ListKeys(ctx, ownerIDs)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return keys, nil
}
