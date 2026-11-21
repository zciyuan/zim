package friend

import "github.com/jackc/pgx/v5/pgxpool"

// DAO is the social graph DAO.
type DAO struct {
	pool *pgxpool.Pool
}
