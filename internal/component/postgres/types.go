package postgres

import (
	"github.com/dobyte/due/v2/component"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the process-wide PostgreSQL component.
type Postgres struct {
	component.Base
	dsn  string
	pool *pgxpool.Pool
}
