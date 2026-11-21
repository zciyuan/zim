// Package friend is the PostgreSQL DAO for the social graph.
package friend

import (
	"context"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
)

// New returns a social graph DAO.
func New(pool *pgxpool.Pool) *DAO {
	return &DAO{pool: pool}
}

// CreateRequest inserts or refreshes the single pending request for a pair.
func (d *DAO) CreateRequest(ctx context.Context, req *model.FriendRequest) error {
	return d.pool.QueryRow(ctx, `INSERT INTO zim_friend_request
(id, from_id, to_id, why, status, created_at, updated_at, handled_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (from_id, to_id) WHERE status = 0 DO UPDATE SET
  why = EXCLUDED.why, updated_at = EXCLUDED.updated_at
RETURNING id`,
		req.ID, req.FromID, req.ToID, req.Why, req.Status, req.CreatedAt, req.UpdatedAt, req.HandledAt).Scan(&req.ID)
}

// GetRequest returns a request by ID.
func (d *DAO) GetRequest(ctx context.Context, id int64) (*model.FriendRequest, error) {
	req := &model.FriendRequest{}
	err := d.pool.QueryRow(ctx, `SELECT id, from_id, to_id, COALESCE(why,''), status,
COALESCE(created_at,0), COALESCE(updated_at,0), COALESCE(handled_at,0) FROM zim_friend_request WHERE id = $1`, id).Scan(
		&req.ID, &req.FromID, &req.ToID, &req.Why, &req.Status, &req.CreatedAt, &req.UpdatedAt, &req.HandledAt)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// HandleRequest rejects the inbound pending request fromID -> toID. A
// reciprocal request (toID -> fromID) is closed as well so that an answered
// pair cannot leave a stale request in the other user's inbox. The inbound
// guard matters: a caller must not be able to reject (or accept) a request it
// sent itself just because the pair has a pending row. It returns
// pgx.ErrNoRows when no inbound pending request exists.
func (d *DAO) HandleRequest(ctx context.Context, fromID, toID int64, status int16, now int64) error {
	tag, err := d.pool.Exec(ctx, `WITH inbound AS (
  SELECT 1 FROM zim_friend_request
  WHERE from_id = $1 AND to_id = $2 AND status = 0
)
UPDATE zim_friend_request SET status = $3, handled_at = $4
WHERE status = 0
  AND ((from_id = $1 AND to_id = $2) OR (from_id = $2 AND to_id = $1))
  AND EXISTS (SELECT 1 FROM inbound)`, fromID, toID, status, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// AddFriends creates both directed rows in one transaction.
func (d *DAO) AddFriends(ctx context.Context, a, b int64, now int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err = tx.Exec(ctx, `INSERT INTO zim_friend (uid, friend_id, created_at)
VALUES ($1,$2,$3), ($2,$1,$3) ON CONFLICT DO NOTHING`, a, b, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeleteFriend removes both directions.
func (d *DAO) DeleteFriend(ctx context.Context, a, b int64) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM zim_friend
WHERE (uid = $1 AND friend_id = $2) OR (uid = $2 AND friend_id = $1)`, a, b)
	return err
}

// List returns the caller's friend rows.
func (d *DAO) List(ctx context.Context, uid int64) ([]*model.Friend, error) {
	rows, err := d.pool.Query(ctx, `SELECT uid, friend_id, COALESCE(remark,''), is_pinned, COALESCE(created_at,0)
FROM zim_friend WHERE uid = $1 ORDER BY is_pinned DESC, friend_id`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	friends := make([]*model.Friend, 0)
	for rows.Next() {
		item := &model.Friend{}
		if err = rows.Scan(&item.UID, &item.FriendID, &item.Remark, &item.Pinned, &item.CreatedAt); err != nil {
			return nil, err
		}
		friends = append(friends, item)
	}
	return friends, rows.Err()
}

// AcceptRequest accepts the inbound pending request fromID -> toID and creates
// both friend rows in one transaction. A reciprocal request (toID -> fromID) is
// accepted in the same transaction, so a simultaneous A->B and B->A request
// cannot leave one side with a pending item after the friendship already
// exists. The inbound guard matters: accepting your own outbound request must
// not create a friendship without the peer's consent.
func (d *DAO) AcceptRequest(ctx context.Context, fromID, toID, now int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `WITH inbound AS (
  SELECT 1 FROM zim_friend_request
  WHERE from_id = $1 AND to_id = $2 AND status = 0
)
UPDATE zim_friend_request SET status = $3, handled_at = $4
WHERE status = 0
  AND ((from_id = $1 AND to_id = $2) OR (from_id = $2 AND to_id = $1))
  AND EXISTS (SELECT 1 FROM inbound)`, fromID, toID, model.FriendRequestAccepted, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	if _, err = tx.Exec(ctx, `INSERT INTO zim_friend (uid, friend_id, created_at)
VALUES ($1,$2,$3), ($2,$1,$3) ON CONFLICT DO NOTHING`, fromID, toID, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListRequests returns inbound or outbound requests for a user. Inbound rows
// are filtered to pending in SQL, before the 100-row cap: handled rows must
// never crowd a still-pending request out of the inbox, and the recipient must
// only be offered requests that can still be answered.
func (d *DAO) ListRequests(ctx context.Context, uid int64, inbound bool) ([]*model.FriendRequest, error) {
	column := "from_id"
	statusFilter := ""
	if inbound {
		column = "to_id"
		statusFilter = " AND status = 0"
	}
	rows, err := d.pool.Query(ctx, `SELECT id, from_id, to_id, COALESCE(why,''), status,
COALESCE(created_at,0), COALESCE(updated_at,0), COALESCE(handled_at,0)
FROM zim_friend_request WHERE `+column+` = $1`+statusFilter+`
ORDER BY updated_at DESC, id DESC LIMIT 100`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	requests := make([]*model.FriendRequest, 0)
	for rows.Next() {
		req := &model.FriendRequest{}
		if err = rows.Scan(&req.ID, &req.FromID, &req.ToID, &req.Why, &req.Status,
			&req.CreatedAt, &req.UpdatedAt, &req.HandledAt); err != nil {
			return nil, err
		}
		requests = append(requests, req)
	}
	return requests, rows.Err()
}

// DeleteExpiredRequests deletes pending requests older than before.
func (d *DAO) DeleteExpiredRequests(ctx context.Context, before int64) (int64, error) {
	tag, err := d.pool.Exec(ctx, `DELETE FROM zim_friend_request
WHERE status = 0 AND created_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// UpdateRemark updates a friend remark.
func (d *DAO) UpdateRemark(ctx context.Context, uid, friendID int64, remark string) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_friend SET remark = $3 WHERE uid = $1 AND friend_id = $2`,
		uid, friendID, remark)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// UpdatePin updates the friend pin flag.
func (d *DAO) UpdatePin(ctx context.Context, uid, friendID int64, pinned int16) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_friend SET is_pinned = $3 WHERE uid = $1 AND friend_id = $2`,
		uid, friendID, pinned)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// AddBlack inserts a blacklist entry.
func (d *DAO) AddBlack(ctx context.Context, uid, blackID int64, now int64) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_blacklist (uid, black_id, created_at)
VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, uid, blackID, now)
	return err
}

// RemoveBlack removes a blacklist entry.
func (d *DAO) RemoveBlack(ctx context.Context, uid, blackID int64) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM zim_blacklist WHERE uid = $1 AND black_id = $2`, uid, blackID)
	return err
}

// Blacklist returns the caller's blacklist.
func (d *DAO) Blacklist(ctx context.Context, uid int64) ([]*model.BlacklistEntry, error) {
	rows, err := d.pool.Query(ctx, `SELECT uid, black_id, COALESCE(created_at,0)
FROM zim_blacklist WHERE uid = $1 ORDER BY created_at DESC`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]*model.BlacklistEntry, 0)
	for rows.Next() {
		entry := &model.BlacklistEntry{}
		if err = rows.Scan(&entry.UID, &entry.BlackID, &entry.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// Relation returns both the directed friendship and the reverse block state in
// one query. The message hot path calls this for every send, so two round trips
// (IsFriend + IsBlack) would double the relation RPC's PostgreSQL work.
func (d *DAO) Relation(ctx context.Context, a, b int64) (friend bool, black bool, err error) {
	err = d.pool.QueryRow(ctx, `SELECT
  EXISTS(SELECT 1 FROM zim_friend WHERE uid = $1 AND friend_id = $2),
  EXISTS(SELECT 1 FROM zim_blacklist WHERE uid = $2 AND black_id = $1)`, a, b).Scan(&friend, &black)
	return friend, black, err
}

// IsFriend reports whether a and b are friends.
func (d *DAO) IsFriend(ctx context.Context, a, b int64) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM zim_friend WHERE uid = $1 AND friend_id = $2)`,
		a, b).Scan(&exists)
	return exists, err
}

// IsBlack reports whether uid has blocked target.
func (d *DAO) IsBlack(ctx context.Context, uid, target int64) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM zim_blacklist WHERE uid = $1 AND black_id = $2)`,
		uid, target).Scan(&exists)
	return exists, err
}

// CanAddFriend reports whether the target accepts new friend requests.
func (d *DAO) CanAddFriend(ctx context.Context, uid int64) (bool, error) {
	var allowed int16
	err := d.pool.QueryRow(ctx, `SELECT add_friend FROM zim_user_privacy WHERE uid = $1`, uid).Scan(&allowed)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		return true, err
	}
	return allowed == 1, nil
}
