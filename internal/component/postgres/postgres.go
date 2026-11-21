// Package postgres owns the shared pgx connection pool. Connections must go
// through PgBouncer in production; the DSN is supplied by etc.zim.postgres.dsn.
package postgres

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New returns the PostgreSQL component. A missing DSN leaves the pool nil so
// services that do not need the database can still start.
func New() *Postgres {
	component := &Postgres{dsn: etc.Get("etc.zim.postgres.dsn").String()}
	if component.dsn == "" {
		return component
	}

	pool, err := pgxpool.New(context.Background(), component.dsn)
	if err != nil {
		log.Fatalf("create postgres pool failed: %v", err)
	}
	component.pool = pool
	return component
}

// Name returns the component name.
func (p *Postgres) Name() string {
	return "postgres"
}

// Start verifies connectivity once.
func (p *Postgres) Start() {
	if p.pool == nil {
		log.Warn("postgres disabled: etc.zim.postgres.dsn is empty")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := p.pool.Ping(ctx)
	cancel()
	if err != nil {
		log.Fatalf("postgres ping failed: %v", err)
	}
	log.Info("postgres connected")
}

// Close closes the pool.
func (p *Postgres) Close() {
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
}

// Pool returns the pgx pool, or nil when PostgreSQL is not configured.
func (p *Postgres) Pool() *pgxpool.Pool {
	return p.pool
}
