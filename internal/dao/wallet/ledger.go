package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const insertLedgerSQL = `
INSERT INTO wallet.zim_wallet_ledger
(id, biz_order_id, uid, biz_type, amount, balance_after, frozen_after, remark, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (biz_order_id, uid, biz_type) DO NOTHING
RETURNING id`

const getLedgerSQL = `
SELECT id, biz_order_id, uid, biz_type, amount, balance_after, frozen_after,
       COALESCE(remark, ''), COALESCE(created_at, 0)
FROM wallet.zim_wallet_ledger
WHERE biz_order_id = $1 AND uid = $2 AND biz_type = $3`

const ledgerExistsSQL = `
SELECT EXISTS (
    SELECT 1 FROM wallet.zim_wallet_ledger
    WHERE biz_order_id = $1 AND uid = $2 AND biz_type = $3
)`

const sumLedgerSQL = `
SELECT COALESCE(SUM(amount), 0) FROM wallet.zim_wallet_ledger WHERE biz_type = $1`

const sumSystemBalancesSQL = `
SELECT COALESCE(SUM(available_balance + frozen_balance), 0)
FROM wallet.zim_wallet_account
WHERE uid < 0`

const sumUserBalancesSQL = `
SELECT COALESCE(SUM(available_balance + frozen_balance), 0)
FROM wallet.zim_wallet_account
WHERE uid > 0`

func scanLedger(row pgx.Row) (*Ledger, error) {
	l := &Ledger{}
	err := row.Scan(
		&l.ID, &l.BizOrderID, &l.UID, &l.BizType, &l.Amount,
		&l.BalanceAfter, &l.FrozenAfter, &l.Remark, &l.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// InsertLedger appends an immutable ledger row. The unique key
// (biz_order_id, uid, biz_type) makes money movement idempotent: false means
// the exact row already exists and the caller must treat the operation as an
// idempotent replay.
func (d *DAO) InsertLedger(ctx context.Context, l *Ledger) (bool, error) {
	var id int64
	err := d.q.QueryRow(ctx, insertLedgerSQL,
		l.ID, l.BizOrderID, l.UID, l.BizType, l.Amount,
		l.BalanceAfter, l.FrozenAfter, l.Remark, l.CreatedAt,
	).Scan(&id)
	if err == nil {
		return true, nil
	}
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return false, fmt.Errorf("insert ledger %s: %w", l.BizOrderID, err)
}

// GetLedger returns one ledger row, or pgx.ErrNoRows when absent.
func (d *DAO) GetLedger(ctx context.Context, bizOrderID string, uid int64, bizType int16) (*Ledger, error) {
	return scanLedger(d.q.QueryRow(ctx, getLedgerSQL, bizOrderID, uid, bizType))
}

// LedgerExists reports whether the idempotency key already hit.
func (d *DAO) LedgerExists(ctx context.Context, bizOrderID string, uid int64, bizType int16) (bool, error) {
	var exists bool
	err := d.q.QueryRow(ctx, ledgerExistsSQL, bizOrderID, uid, bizType).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check ledger %s: %w", bizOrderID, err)
	}
	return exists, nil
}

// SumLedger returns the sum of amounts for one business type. Debit rows are
// negative, so withdrawal sums come back negative.
func (d *DAO) SumLedger(ctx context.Context, bizType int16) (int64, error) {
	var sum int64
	err := d.q.QueryRow(ctx, sumLedgerSQL, bizType).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum ledger %d: %w", bizType, err)
	}
	return sum, nil
}

// SumBalances returns Σ(available+frozen) over system accounts (uid < 0) when
// system is true, or over user accounts (uid > 0) otherwise.
func (d *DAO) SumBalances(ctx context.Context, system bool) (int64, error) {
	sql := sumUserBalancesSQL
	if system {
		sql = sumSystemBalancesSQL
	}
	var sum int64
	err := d.q.QueryRow(ctx, sql).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum balances: %w", err)
	}
	return sum, nil
}
