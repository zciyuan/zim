package device

import (
	"context"
	cryptorand "crypto/rand"
	"math/big"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"
)

const maxSlot = 255

// ErrNoSlot reports that all 255 slots of a user are already allocated.
var ErrNoSlot = dueerrors.New("no device slot available")

// EnsureSlot returns the slot allocated to a device, allocating a random one
// when the device is new. Random allocation keeps due's low-bit session shards
// balanced; a linear scan is the bounded fallback when random probes collide.
func (d *DAO) EnsureSlot(ctx context.Context, ownerID int64, platform int16, deviceID string) (int16, error) {
	if slot, ok, err := d.deviceSlot(ctx, ownerID, deviceID); err != nil || ok {
		return slot, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		candidate, err := randomSlot()
		if err != nil {
			return 0, err
		}
		if slot, ok, err := d.insertSlot(ctx, ownerID, platform, deviceID, candidate); err != nil || ok {
			return slot, err
		}
	}
	used, err := d.usedSlots(ctx, ownerID)
	if err != nil {
		return 0, err
	}
	for candidate := int16(1); candidate <= maxSlot; candidate++ {
		if used[candidate] {
			continue
		}
		if slot, ok, err := d.insertSlot(ctx, ownerID, platform, deviceID, candidate); err != nil || ok {
			return slot, err
		}
	}
	return 0, ErrNoSlot
}

// deviceSlot returns the existing slot of one device.
func (d *DAO) deviceSlot(ctx context.Context, ownerID int64, deviceID string) (int16, bool, error) {
	var slot int16
	err := d.pool.QueryRow(ctx, `SELECT slot FROM zim_device
WHERE owner_id = $1 AND device_id = $2`, ownerID, deviceID).Scan(&slot)
	if err == nil {
		return slot, true, nil
	}
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	return 0, false, err
}

// insertSlot inserts a candidate slot and reports whether this device won it.
func (d *DAO) insertSlot(ctx context.Context, ownerID int64, platform int16, deviceID string, slot int16) (int16, bool, error) {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_device
(owner_id, platform, slot, device_id, status, last_active_at, created_at, updated_at)
VALUES ($1,$2,$3,$4,0,0,0,0)
ON CONFLICT DO NOTHING`, ownerID, platform, slot, deviceID)
	if err != nil {
		return 0, false, err
	}
	return d.deviceSlot(ctx, ownerID, deviceID)
}

// usedSlots returns the allocated slots of one user.
func (d *DAO) usedSlots(ctx context.Context, ownerID int64) (map[int16]bool, error) {
	rows, err := d.pool.Query(ctx, `SELECT slot FROM zim_device WHERE owner_id = $1`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	used := make(map[int16]bool, maxSlot)
	for rows.Next() {
		var slot int16
		if err = rows.Scan(&slot); err != nil {
			return nil, err
		}
		used[slot] = true
	}
	return used, rows.Err()
}

func randomSlot() (int16, error) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(maxSlot))
	if err != nil {
		return 0, err
	}
	return int16(value.Int64() + 1), nil
}
