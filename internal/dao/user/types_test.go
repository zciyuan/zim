package user

import "github.com/jackc/pgx/v5"

type fakeRow struct {
	err   error
	apply bool
}

type fakeQuerier struct {
	row pgx.Row
}
