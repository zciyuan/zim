package family

import "github.com/jackc/pgx/v5/pgxpool"

// DAO is the PostgreSQL family policy store.
type DAO struct {
	pool *pgxpool.Pool
}

// New returns a family DAO.
func New(pool *pgxpool.Pool) *DAO { return &DAO{pool: pool} }
