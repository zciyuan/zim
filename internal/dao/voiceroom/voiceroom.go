// Package voiceroom is the PostgreSQL DAO for zim_voice_room (ADR-0043).
package voiceroom

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
)

// poolIface is the subset of pgxpool.Pool the DAO uses; tests substitute a
// fake.
type poolIface interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// VoiceRoom is a concrete pgx DAO; services keep a narrow interface for tests.
type VoiceRoom struct {
	pool poolIface
}

// New returns a voice room DAO.
func New(pool *pgxpool.Pool) *VoiceRoom {
	return &VoiceRoom{pool: pool}
}

func newWithPool(pool poolIface) *VoiceRoom {
	return &VoiceRoom{pool: pool}
}

const createSQL = `
INSERT INTO zim_voice_room(room_id, room_type, group_id, owner_uid, title, announcement,
  seat_count, join_seat_mode, password_hash, status, created_at, closed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`

const getSQL = `
SELECT room_id, room_type, group_id, owner_uid, title, COALESCE(announcement, ''),
  seat_count, join_seat_mode, password_hash, status, created_at, COALESCE(closed_at, 0)
FROM zim_voice_room WHERE room_id = $1`

const listByGroupSQL = `
SELECT room_id, room_type, group_id, owner_uid, title, COALESCE(announcement, ''),
  seat_count, join_seat_mode, password_hash, status, created_at, COALESCE(closed_at, 0)
FROM zim_voice_room WHERE group_id = $1 ORDER BY room_id DESC LIMIT $2 OFFSET $3`

const listPublicSQL = `
SELECT room_id, room_type, group_id, owner_uid, title, COALESCE(announcement, ''),
  seat_count, join_seat_mode, password_hash, status, created_at, COALESCE(closed_at, 0)
FROM zim_voice_room WHERE room_type = $1 AND status = $2
ORDER BY room_id DESC LIMIT $3 OFFSET $4`

const updateStatusSQL = `
UPDATE zim_voice_room SET status = $2, closed_at = $3 WHERE room_id = $1`

// Create inserts one room row.
func (d *VoiceRoom) Create(ctx context.Context, room *model.VoiceRoom) error {
	_, err := d.pool.Exec(ctx, createSQL, room.RoomID, room.RoomType, room.GroupID,
		room.OwnerUID, room.Title, room.Announcement, room.SeatCount, room.JoinSeatMode,
		room.PasswordHash, room.Status, room.CreatedAt, room.ClosedAt)
	return err
}

// Get returns one room row, or pgx.ErrNoRows when absent.
func (d *VoiceRoom) Get(ctx context.Context, roomID int64) (*model.VoiceRoom, error) {
	row := d.pool.QueryRow(ctx, getSQL, roomID)
	return scanRoom(row)
}

// UpdateStatus sets status and closed_at.
func (d *VoiceRoom) UpdateStatus(ctx context.Context, roomID int64, status int16, closedAt int64) error {
	_, err := d.pool.Exec(ctx, updateStatusSQL, roomID, status, closedAt)
	return err
}

// ListByGroup pages rooms of one group.
func (d *VoiceRoom) ListByGroup(ctx context.Context, groupID int64, limit int, offset int64) ([]*model.VoiceRoom, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := d.pool.Query(ctx, listByGroupSQL, groupID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRooms(rows)
}

// ListPublic pages live PUBLIC rooms.
func (d *VoiceRoom) ListPublic(ctx context.Context, limit int, offset int64) ([]*model.VoiceRoom, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := d.pool.Query(ctx, listPublicSQL, model.VoiceRoomTypePublic, model.VoiceRoomStatusLive, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRooms(rows)
}

func scanRoom(row pgx.Row) (*model.VoiceRoom, error) {
	room := &model.VoiceRoom{}
	err := row.Scan(&room.RoomID, &room.RoomType, &room.GroupID, &room.OwnerUID,
		&room.Title, &room.Announcement, &room.SeatCount, &room.JoinSeatMode,
		&room.PasswordHash, &room.Status, &room.CreatedAt, &room.ClosedAt)
	if err != nil {
		return nil, err
	}
	return room, nil
}

func scanRooms(rows pgx.Rows) ([]*model.VoiceRoom, error) {
	rooms := make([]*model.VoiceRoom, 0, 16)
	for rows.Next() {
		room := &model.VoiceRoom{}
		if err := rows.Scan(&room.RoomID, &room.RoomType, &room.GroupID, &room.OwnerUID,
			&room.Title, &room.Announcement, &room.SeatCount, &room.JoinSeatMode,
			&room.PasswordHash, &room.Status, &room.CreatedAt, &room.ClosedAt); err != nil {
			return nil, err
		}
		rooms = append(rooms, room)
	}
	return rooms, rows.Err()
}
