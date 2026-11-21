// Package user is the PostgreSQL DAO for zim_user.
package user

import (
	"context"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
)

const findByNameSQL = `
SELECT user_id, username, password_hash, COALESCE(nickname, ''), COALESCE(face_url, ''),
       COALESCE(gender, 0), status, COALESCE(created_at, 0), COALESCE(updated_at, 0)
FROM zim_user
WHERE username = $1`

// querier is the subset of pgxpool.Pool used by the DAO.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// New returns a user DAO.
func New(pool *pgxpool.Pool) *User {
	return &User{db: pool, pool: pool}
}

func newWithQuerier(db querier) *User {
	return &User{db: db}
}

// FindByUsername returns the user row, or pgx.ErrNoRows when absent.
func (d *User) FindByUsername(ctx context.Context, username string) (*model.User, error) {
	user := &model.User{}
	err := d.db.QueryRow(ctx, findByNameSQL, username).Scan(
		&user.ID,
		&user.Username,
		&user.PasswordHash,
		&user.Nickname,
		&user.FaceURL,
		&user.Gender,
		&user.Status,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return user, nil
}

// ListUsers returns users ordered by ID for the admin console.
func (d *User) ListUsers(ctx context.Context, limit, offset int) ([]*model.User, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, username, password_hash, COALESCE(nickname, ''),
 COALESCE(face_url, ''), COALESCE(gender, 0), status, COALESCE(created_at, 0), COALESCE(updated_at, 0)
FROM zim_user ORDER BY user_id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]*model.User, 0, limit)
	for rows.Next() {
		user := &model.User{}
		if err = rows.Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Nickname,
			&user.FaceURL, &user.Gender, &user.Status, &user.CreatedAt, &user.UpdatedAt); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// CountUsers returns total users.
func (d *User) CountUsers(ctx context.Context) (int64, error) {
	var count int64
	err := d.pool.QueryRow(ctx, `SELECT count(*) FROM zim_user`).Scan(&count)
	return count, err
}

// ListIDsAfter returns the next page of user IDs after the given cursor.
func (d *User) ListIDsAfter(ctx context.Context, after int64, limit int) ([]int64, error) {
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := d.pool.Query(ctx, `SELECT user_id FROM zim_user WHERE user_id > $1 ORDER BY user_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListAllIDs returns up to limit user IDs.
func (d *User) ListAllIDs(ctx context.Context, limit int) ([]int64, error) {
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	rows, err := d.pool.Query(ctx, `SELECT user_id FROM zim_user ORDER BY user_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// GetPrivacy returns a user's privacy settings.
func (d *User) GetPrivacy(ctx context.Context, uid int64) (bool, bool, error) {
	var searchable, addFriend int16
	err := d.pool.QueryRow(ctx, `SELECT searchable, add_friend FROM zim_user_privacy WHERE uid = $1`, uid).
		Scan(&searchable, &addFriend)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return true, true, nil
		}
		return true, true, err
	}
	return searchable == 1, addFriend == 1, nil
}

// SetPrivacy updates a user's privacy settings.
func (d *User) SetPrivacy(ctx context.Context, uid int64, searchable, addFriend bool) error {
	search, add := int16(0), int16(0)
	if searchable {
		search = 1
	}
	if addFriend {
		add = 1
	}
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_user_privacy(uid, searchable, add_friend, updated_at)
VALUES ($1,$2,$3,$4) ON CONFLICT (uid) DO UPDATE SET searchable=EXCLUDED.searchable,
add_friend=EXCLUDED.add_friend, updated_at=EXCLUDED.updated_at`, uid, search, add, time.Now().UnixMilli())
	return err
}
