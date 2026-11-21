package user

import "github.com/jackc/pgx/v5/pgxpool"

// User is the zim_user DAO.
type User struct {
	db   querier
	pool *pgxpool.Pool
}
