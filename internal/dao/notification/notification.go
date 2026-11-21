// Package notification is the PostgreSQL announcement store.
package notification

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	notificationsvc "github.com/zciyuan/ZIM/internal/service/notification/server"
)

// DAO stores notifications.
type DAO struct{ pool *pgxpool.Pool }

// New returns a notification DAO.
func New(pool *pgxpool.Pool) *DAO { return &DAO{pool: pool} }

// ListNotifications returns announcements newer than since.
func (d *DAO) ListNotifications(ctx context.Context, since int64, limit int) ([]*notificationsvc.Notification, error) {
	rows, err := d.pool.Query(ctx, `SELECT id, title, content, created_at, published_at
FROM zim_notification WHERE published_at > $1 ORDER BY published_at DESC, id DESC LIMIT $2`, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*notificationsvc.Notification, 0, limit)
	for rows.Next() {
		item := &notificationsvc.Notification{}
		if err = rows.Scan(&item.ID, &item.Title, &item.Content, &item.CreatedAt, &item.PublishedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CreateNotification inserts an announcement.
func (d *DAO) CreateNotification(ctx context.Context, title, content string, now int64) (int64, error) {
	var id int64
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	err := d.pool.QueryRow(ctx, `INSERT INTO zim_notification(title, content, created_at, published_at)
VALUES ($1,$2,$3,$3) RETURNING id`, title, content, now).Scan(&id)
	return id, err
}

// MarkRead records that a user read one announcement.
func (d *DAO) MarkRead(ctx context.Context, uid, id, now int64) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_notification_read(uid, notification_id, read_at)
VALUES ($1,$2,$3) ON CONFLICT (uid, notification_id) DO UPDATE SET read_at = EXCLUDED.read_at`, uid, id, now)
	return err
}

// ReadIDs returns announcements read by a user.
func (d *DAO) ReadIDs(ctx context.Context, uid int64) ([]int64, error) {
	rows, err := d.pool.Query(ctx, `SELECT notification_id FROM zim_notification_read WHERE uid = $1 ORDER BY notification_id DESC`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
