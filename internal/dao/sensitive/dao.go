// Package sensitive reads admin-managed blocked words.
package sensitive

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DAO is the PostgreSQL sensitive-word store.
type DAO struct{ pool *pgxpool.Pool }

// New returns a sensitive-word DAO.
func New(pool *pgxpool.Pool) *DAO { return &DAO{pool: pool} }

// List returns enabled blocked words.
func (d *DAO) List(ctx context.Context) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT word FROM zim_sensitive_word WHERE action = 1 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	words := make([]string, 0)
	for rows.Next() {
		var word string
		if err = rows.Scan(&word); err != nil {
			return nil, err
		}
		words = append(words, word)
	}
	return words, rows.Err()
}
