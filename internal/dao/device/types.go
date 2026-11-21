package device

import "github.com/jackc/pgx/v5/pgxpool"

// DAO is the PostgreSQL device registry.
type DAO struct {
	pool *pgxpool.Pool
}
