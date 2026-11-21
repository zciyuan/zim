// Package webhook is the PostgreSQL webhook DLQ.
package webhook

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"

	webhooksvc "github.com/zciyuan/ZIM/internal/service/webhook/server"
)

// DAO stores failed webhook events.
type DAO struct{ pool *pgxpool.Pool }

// New returns a webhook DLQ DAO.
func New(pool *pgxpool.Pool) *DAO { return &DAO{pool: pool} }

// Add records an event that exhausted retries.
func (d *DAO) Add(ctx context.Context, event webhooksvc.Event, cause error) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `INSERT INTO zim_webhook_dlq(event_type, payload, error, created_at)
VALUES ($1,$2,$3,$4)`, event.Type, payload, cause.Error(), time.Now().UnixMilli())
	return err
}
