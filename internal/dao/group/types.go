package group

import "github.com/jackc/pgx/v5/pgxpool"

// DAO is the group DAO.
type DAO struct {
	pool *pgxpool.Pool
}
