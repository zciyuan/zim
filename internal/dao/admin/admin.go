// Package admin persists admin audit records.
package admin

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
)

// DAO is the PostgreSQL admin audit store.
type DAO struct {
	pool *pgxpool.Pool
}

// New returns an admin audit DAO.
func New(pool *pgxpool.Pool) *DAO { return &DAO{pool: pool} }

// Add appends one audit record.
func (d *DAO) Add(ctx context.Context, record *adminsvc.AuditRecord) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_admin_audit
(id, operator, action, target, request_id, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
		record.ID, record.Operator, record.Action, record.Target, record.RequestID, record.CreatedAt)
	return err
}

// List returns recent audit records.
func (d *DAO) List(ctx context.Context, operator string, limit int) ([]*adminsvc.AuditRecord, error) {
	query := `SELECT id, operator, action, target, request_id, created_at FROM zim_admin_audit`
	args := make([]any, 0, 2)
	if operator != "" {
		query += ` WHERE operator = $1`
		args = append(args, operator)
	}
	query += ` ORDER BY id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)
	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*adminsvc.AuditRecord, 0, limit)
	for rows.Next() {
		item := &adminsvc.AuditRecord{}
		if err = rows.Scan(&item.ID, &item.Operator, &item.Action, &item.Target, &item.RequestID, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
