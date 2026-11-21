package conversation

import "github.com/jackc/pgx/v5/pgxpool"

// DAO is the conversation DAO.
type DAO struct {
	pool *pgxpool.Pool
}
