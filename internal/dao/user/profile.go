package user

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
)

const userColumns = `user_id, username, password_hash, COALESCE(nickname, ''), COALESCE(face_url, ''),
       COALESCE(gender, 0), status, COALESCE(created_at, 0), COALESCE(updated_at, 0)`

// Create inserts a user.
func (d *User) Create(ctx context.Context, user *model.User) error {
	_, err := d.pool.Exec(ctx, `
INSERT INTO zim_user (user_id, username, password_hash, nickname, face_url, gender, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		user.ID, user.Username, user.PasswordHash, user.Nickname, user.FaceURL,
		user.Gender, user.Status, user.CreatedAt, user.UpdatedAt)
	return err
}

// FindByID returns one user.
func (d *User) FindByID(ctx context.Context, uid int64) (*model.User, error) {
	user := &model.User{}
	err := d.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM zim_user WHERE user_id = $1`, uid).Scan(
		&user.ID, &user.Username, &user.PasswordHash, &user.Nickname, &user.FaceURL,
		&user.Gender, &user.Status, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return user, nil
}

// FindByIDs returns users in the requested order-insensitive set.
func (d *User) FindByIDs(ctx context.Context, uids []int64) ([]*model.User, error) {
	rows, err := d.pool.Query(ctx, `SELECT `+userColumns+` FROM zim_user WHERE user_id = ANY($1)`, uids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]*model.User, 0, len(uids))
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

// Search finds users by username or nickname prefix.
func (d *User) Search(ctx context.Context, keyword string, limit int) ([]*model.User, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := d.pool.Query(ctx, `SELECT `+userColumns+`
FROM zim_user
WHERE status = 0
AND NOT EXISTS (SELECT 1 FROM zim_user_privacy p WHERE p.uid = zim_user.user_id AND p.searchable = 0)
AND (username ILIKE $1 OR nickname ILIKE $1)
ORDER BY user_id LIMIT $2`, keyword+"%", limit)
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

// UpdateProfile updates editable profile fields.
func (d *User) UpdateProfile(ctx context.Context, uid int64, nickname, faceURL string, gender int16, now int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_user
SET nickname = $2, face_url = $3, gender = $4, updated_at = $5
WHERE user_id = $1`, uid, nickname, faceURL, gender, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// UpdatePassword updates the password hash.
func (d *User) UpdatePassword(ctx context.Context, uid int64, hash string, now int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_user SET password_hash = $2, updated_at = $3 WHERE user_id = $1`,
		uid, hash, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetStatus enables or disables an account.
func (d *User) SetStatus(ctx context.Context, uid int64, status int16, now int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_user SET status = $2, updated_at = $3 WHERE user_id = $1`,
		uid, status, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// NextUserID allocates the next sequential account id.
//
// Registration uses this instead of the snowflake generator: device keys are
// encoded as uid<<11|platform<<8|slot (ADR-0035), which fits int64 for
// snowflake uids. Message and group ids keep using snowflake.
func (d *User) NextUserID(ctx context.Context) (int64, error) {
	var id int64
	err := d.pool.QueryRow(ctx, `SELECT nextval('zim_user_id_seq')`).Scan(&id)
	return id, err
}
