package admin

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
)

// GetSetting returns one setting, or an empty value when absent.
func (d *DAO) GetSetting(ctx context.Context, key string) (*adminsvc.Setting, error) {
	item := &adminsvc.Setting{Key: key}
	err := d.pool.QueryRow(ctx, `SELECT value, updated_at FROM zim_admin_setting WHERE key = $1`, key).
		Scan(&item.Value, &item.UpdatedAt)
	if err == pgx.ErrNoRows {
		return item, nil
	}
	return item, err
}

// SetSetting writes one setting.
func (d *DAO) SetSetting(ctx context.Context, key, value string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_admin_setting(key, value, updated_at) VALUES ($1,$2,$3)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`,
		key, value, time.Now().UnixMilli())
	return err
}

// ListSensitiveWords returns all blocked words.
func (d *DAO) ListSensitiveWords(ctx context.Context) ([]*adminsvc.SensitiveWord, error) {
	rows, err := d.pool.Query(ctx, `SELECT id, word, action, created_at FROM zim_sensitive_word ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.SensitiveWord, 0)
	for rows.Next() {
		item := &adminsvc.SensitiveWord{}
		if err = rows.Scan(&item.ID, &item.Word, &item.Action, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// AddSensitiveWord inserts one blocked word.
func (d *DAO) AddSensitiveWord(ctx context.Context, word string, action int16) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_sensitive_word(word, action, created_at) VALUES ($1,$2,$3)
ON CONFLICT (word) DO UPDATE SET action = EXCLUDED.action`, word, action, time.Now().UnixMilli())
	return err
}

// DeleteSensitiveWord removes one blocked word.
func (d *DAO) DeleteSensitiveWord(ctx context.Context, id int64) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM zim_sensitive_word WHERE id = $1`, id)
	return err
}

// Report is one abuse report (service DTO).

// ListReports returns reports, optionally filtered by status.
func (d *DAO) ListReports(ctx context.Context, status int16, limit int) ([]*adminsvc.Report, error) {
	query := `SELECT id, reporter_id, target_type, target_id, reason, status, created_at FROM zim_report`
	args := make([]any, 0, 2)
	if status >= 0 {
		query += ` WHERE status = $1`
		args = append(args, status)
	}
	query += ` ORDER BY id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)
	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.Report, 0, limit)
	for rows.Next() {
		item := &adminsvc.Report{}
		if err = rows.Scan(&item.ID, &item.ReporterID, &item.TargetType, &item.TargetID,
			&item.Reason, &item.Status, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CreateReport inserts a report.
func (d *DAO) CreateReport(ctx context.Context, reporter int64, targetType, targetID, reason string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_report(reporter_id, target_type, target_id, reason, status, created_at)
VALUES ($1,$2,$3,$4,0,$5)`, reporter, targetType, targetID, reason, time.Now().UnixMilli())
	return err
}

// HandleReport updates report status.
func (d *DAO) HandleReport(ctx context.Context, id int64, status int16) error {
	_, err := d.pool.Exec(ctx, `UPDATE zim_report SET status = $2, handled_at = $3 WHERE id = $1`,
		id, status, time.Now().UnixMilli())
	return err
}

// PushCert is one push provider certificate.
type PushCert struct {
	Platform   int16 `json:"platform"`
	Configured bool  `json:"configured"`
	UpdatedAt  int64 `json:"updated_at"`
}

// ListPushCerts returns configured platforms without exposing cert material.
func (d *DAO) ListPushCerts(ctx context.Context) ([]*adminsvc.PushCert, error) {
	rows, err := d.pool.Query(ctx, `SELECT platform, cert <> '', updated_at FROM zim_push_cert ORDER BY platform`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.PushCert, 0)
	for rows.Next() {
		item := &adminsvc.PushCert{}
		if err = rows.Scan(&item.Platform, &item.Configured, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// SetPushCert writes one platform certificate.
func (d *DAO) SetPushCert(ctx context.Context, platform int16, cert string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_push_cert(platform, cert, updated_at) VALUES ($1,$2,$3)
ON CONFLICT (platform) DO UPDATE SET cert = EXCLUDED.cert, updated_at = EXCLUDED.updated_at`,
		platform, cert, time.Now().UnixMilli())
	return err
}

// ListSettings returns settings matching a SQL LIKE pattern.
func (d *DAO) ListSettings(ctx context.Context, pattern string) ([]*adminsvc.Setting, error) {
	rows, err := d.pool.Query(ctx, `SELECT key, value, updated_at FROM zim_admin_setting WHERE key LIKE $1 ORDER BY key`, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.Setting, 0)
	for rows.Next() {
		item := &adminsvc.Setting{}
		if err = rows.Scan(&item.Key, &item.Value, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ListWebhookDLQ returns failed webhook events.
func (d *DAO) ListWebhookDLQ(ctx context.Context, limit int) ([]*adminsvc.WebhookDLQ, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := d.pool.Query(ctx, `SELECT id, event_type, payload::text, error, created_at
FROM zim_webhook_dlq ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.WebhookDLQ, 0, limit)
	for rows.Next() {
		item := &adminsvc.WebhookDLQ{}
		if err = rows.Scan(&item.ID, &item.EventType, &item.Payload, &item.Error, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteWebhookDLQ removes one failed event.
func (d *DAO) DeleteWebhookDLQ(ctx context.Context, id int64) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM zim_webhook_dlq WHERE id = $1`, id)
	return err
}
