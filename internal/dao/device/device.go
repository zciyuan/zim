// Package device is the PostgreSQL DAO for zim_device (ADR-0024).
package device

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// New returns a device DAO.
func New(pool *pgxpool.Pool) *DAO {
	return &DAO{pool: pool}
}

// Upsert inserts or updates one device row. Empty string fields keep the
// existing value, so a login that carries no push token does not erase it.
func (d *DAO) Upsert(ctx context.Context, device *model.Device) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_device
(owner_id, platform, slot, device_id, device_name, push_provider, push_token, voip_token,
 app_version, os_version, status, last_active_at, badge, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (owner_id, device_id) DO UPDATE SET
  slot          = EXCLUDED.slot,
  platform      = EXCLUDED.platform,
  device_name   = CASE WHEN EXCLUDED.device_name <> ''   THEN EXCLUDED.device_name   ELSE zim_device.device_name   END,
  push_provider = CASE WHEN EXCLUDED.push_provider <> '' THEN EXCLUDED.push_provider ELSE zim_device.push_provider END,
  push_token    = CASE WHEN EXCLUDED.push_token <> ''    THEN EXCLUDED.push_token    ELSE zim_device.push_token    END,
  voip_token    = CASE WHEN EXCLUDED.voip_token <> ''    THEN EXCLUDED.voip_token    ELSE zim_device.voip_token    END,
  app_version   = CASE WHEN EXCLUDED.app_version <> ''   THEN EXCLUDED.app_version   ELSE zim_device.app_version   END,
  os_version    = CASE WHEN EXCLUDED.os_version <> ''    THEN EXCLUDED.os_version    ELSE zim_device.os_version    END,
  status        = 0,
  last_active_at = GREATEST(zim_device.last_active_at, EXCLUDED.last_active_at),
  updated_at    = EXCLUDED.updated_at`,
		device.OwnerID, device.Platform, device.Slot, device.DeviceID, device.DeviceName,
		device.PushProvider, device.PushToken, device.VoIPToken,
		device.AppVersion, device.OSVersion, device.Status,
		device.LastActiveAt, device.Badge, device.CreatedAt, device.UpdatedAt)
	return err
}

// List returns a user's devices ordered by platform.
func (d *DAO) List(ctx context.Context, ownerID int64) ([]*model.Device, error) {
	rows, err := d.pool.Query(ctx, `SELECT owner_id, platform, slot, device_id, device_name,
push_provider, push_token, voip_token, app_version, os_version, status,
last_active_at, badge, created_at, updated_at
FROM zim_device WHERE owner_id = $1 ORDER BY platform`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	devices := make([]*model.Device, 0, 5)
	for rows.Next() {
		device := &model.Device{}
		if err = rows.Scan(&device.OwnerID, &device.Platform, &device.Slot, &device.DeviceID, &device.DeviceName,
			&device.PushProvider, &device.PushToken, &device.VoIPToken, &device.AppVersion,
			&device.OSVersion, &device.Status, &device.LastActiveAt, &device.Badge,
			&device.CreatedAt, &device.UpdatedAt); err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, rows.Err()
}

// Deactivate marks one device unusable and clears its push tokens.
func (d *DAO) Deactivate(ctx context.Context, ownerID int64, platform int16, now int64) error {
	_, err := d.pool.Exec(ctx, `UPDATE zim_device
SET status = 1, push_token = '', voip_token = '', updated_at = $3
WHERE owner_id = $1 AND platform = $2`, ownerID, platform, now)
	return err
}

// DeactivateSlot marks a single device slot unusable and clears its tokens.
//
// Logout and disconnect use it so the device table only keeps devices that are
// actually bound, instead of growing one live row per browser profile forever.
func (d *DAO) DeactivateSlot(ctx context.Context, ownerID int64, platform, slot int16, now int64) error {
	_, err := d.pool.Exec(ctx, `UPDATE zim_device
SET status = 1, push_token = '', voip_token = '', updated_at = $4
WHERE owner_id = $1 AND platform = $2 AND slot = $3`, ownerID, platform, slot, now)
	return err
}

// ActiveTargets returns devices that have a push token for the given users.
func (d *DAO) ActiveTargets(ctx context.Context, ownerIDs []int64) ([]*model.Device, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT owner_id, platform, slot, device_id, device_name,
push_provider, push_token, voip_token, app_version, os_version, status,
last_active_at, badge, created_at, updated_at
FROM zim_device
WHERE owner_id = ANY($1) AND status = 0 AND push_token <> ''
ORDER BY owner_id, platform`, ownerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	devices := make([]*model.Device, 0, len(ownerIDs))
	for rows.Next() {
		device := &model.Device{}
		if err = rows.Scan(&device.OwnerID, &device.Platform, &device.Slot, &device.DeviceID, &device.DeviceName,
			&device.PushProvider, &device.PushToken, &device.VoIPToken, &device.AppVersion,
			&device.OSVersion, &device.Status, &device.LastActiveAt, &device.Badge,
			&device.CreatedAt, &device.UpdatedAt); err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, rows.Err()
}

// ListKeys returns the device keys for the given owners.
func (d *DAO) ListKeys(ctx context.Context, ownerIDs []int64) ([]int64, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT owner_id, platform, slot FROM zim_device
WHERE owner_id = ANY($1) AND status = 0 ORDER BY owner_id, platform, slot`, ownerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make([]int64, 0, len(ownerIDs)*2)
	for rows.Next() {
		var ownerID int64
		var platform, slot int16
		if err = rows.Scan(&ownerID, &platform, &slot); err != nil {
			return nil, err
		}
		keys = append(keys, multi.Key(ownerID, int32(platform), int32(slot)))
	}
	return keys, rows.Err()
}
