package postgres

import (
	"context"

	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewWallet returns the PostgreSQL component bound to the independent wallet
// DSN (etc.zim.wallet.dsn, ADR-0043: 独立 wallet 库/复式账本部署面). A missing
// DSN leaves the pool nil so callers can fail fast with a clear message.
func NewWallet() *Postgres {
	component := &Postgres{dsn: etc.Get("etc.zim.wallet.dsn").String()}
	if component.dsn == "" {
		return component
	}

	pool, err := pgxpool.New(context.Background(), component.dsn)
	if err != nil {
		log.Fatalf("create wallet postgres pool failed: %v", err)
	}
	component.pool = pool
	return component
}
