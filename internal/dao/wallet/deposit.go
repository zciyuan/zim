package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const getDepositAddressSQL = `
SELECT id, uid, chain, address, derivation_idx, COALESCE(created_at, 0)
FROM wallet.zim_wallet_deposit_address
WHERE uid = $1 AND chain = $2`

const getDepositAddressByAddressSQL = `
SELECT id, uid, chain, address, derivation_idx, COALESCE(created_at, 0)
FROM wallet.zim_wallet_deposit_address
WHERE address = $1`

const insertDepositAddressSQL = `
INSERT INTO wallet.zim_wallet_deposit_address (id, uid, chain, address, derivation_idx, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING`

const nextDerivationIndexSQL = `
SELECT COALESCE(MAX(derivation_idx), -1) + 1 FROM wallet.zim_wallet_deposit_address`

const hasDepositAddressSQL = `
SELECT EXISTS (SELECT 1 FROM wallet.zim_wallet_deposit_address WHERE address = $1)`

const upsertChainDepositSQL = `
INSERT INTO wallet.zim_chain_deposit
(id, uid, chain, tx_hash, log_index, from_address, to_address, amount, confirmations, status, created_at, credited_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, $10, 0)
ON CONFLICT (chain, tx_hash, log_index) DO UPDATE SET
  confirmations = GREATEST(wallet.zim_chain_deposit.confirmations, EXCLUDED.confirmations)
RETURNING id, uid, chain, tx_hash, log_index, COALESCE(from_address, ''), COALESCE(to_address, ''),
          amount, confirmations, status, COALESCE(created_at, 0), COALESCE(credited_at, 0)`

const getChainDepositSQL = `
SELECT id, uid, chain, tx_hash, log_index, COALESCE(from_address, ''), COALESCE(to_address, ''),
       amount, confirmations, status, COALESCE(created_at, 0), COALESCE(credited_at, 0)
FROM wallet.zim_chain_deposit
WHERE id = $1`

const markDepositStatusSQL = `
UPDATE wallet.zim_chain_deposit
SET status = $3, credited_at = $4
WHERE id = $1 AND status = $2`

func scanDepositAddress(row pgx.Row) (*DepositAddress, error) {
	a := &DepositAddress{}
	err := row.Scan(&a.ID, &a.UID, &a.Chain, &a.Address, &a.DerivationIdx, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func scanChainDeposit(row pgx.Row) (*ChainDeposit, error) {
	d := &ChainDeposit{}
	err := row.Scan(
		&d.ID, &d.UID, &d.Chain, &d.TxHash, &d.LogIndex, &d.FromAddress, &d.ToAddress,
		&d.Amount, &d.Confirmations, &d.Status, &d.CreatedAt, &d.CreditedAt,
	)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// GetDepositAddress returns the user's deposit address for one chain, or
// pgx.ErrNoRows when absent.
func (d *DAO) GetDepositAddress(ctx context.Context, uid int64, chain string) (*DepositAddress, error) {
	return scanDepositAddress(d.q.QueryRow(ctx, getDepositAddressSQL, uid, chain))
}

// GetDepositAddressByAddress resolves the owner of a deposit address.
func (d *DAO) GetDepositAddressByAddress(ctx context.Context, address string) (*DepositAddress, error) {
	return scanDepositAddress(d.q.QueryRow(ctx, getDepositAddressByAddressSQL, address))
}

// InsertDepositAddress inserts the address mapping. false means a unique key
// (uid, chain) or (address) already exists; the caller re-reads.
func (d *DAO) InsertDepositAddress(ctx context.Context, a *DepositAddress) (bool, error) {
	tag, err := d.q.Exec(ctx, insertDepositAddressSQL, a.ID, a.UID, a.Chain, a.Address, a.DerivationIdx, a.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("insert deposit address for %d: %w", a.UID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// NextDerivationIndex returns the next free HD derivation index. The mock
// generator uses MAX+1; a concurrent duplicate is resolved by the unique
// (address) constraint and the caller's retry loop.
func (d *DAO) NextDerivationIndex(ctx context.Context) (int32, error) {
	var idx int32
	err := d.q.QueryRow(ctx, nextDerivationIndexSQL).Scan(&idx)
	if err != nil {
		return 0, fmt.Errorf("next derivation index: %w", err)
	}
	return idx, nil
}

// HasDepositAddress reports whether the address is an internal deposit
// address (withdrawal to it is forbidden, 200011).
func (d *DAO) HasDepositAddress(ctx context.Context, address string) (bool, error) {
	var exists bool
	err := d.q.QueryRow(ctx, hasDepositAddressSQL, address).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check deposit address: %w", err)
	}
	return exists, nil
}

// UpsertChainDeposit inserts the chain event or raises its confirmations to
// the maximum seen. The unique key (chain, tx_hash, log_index) makes the
// callback idempotent. It returns the current row.
func (d *DAO) UpsertChainDeposit(ctx context.Context, dep *ChainDeposit) (*ChainDeposit, error) {
	return scanChainDeposit(d.q.QueryRow(ctx, upsertChainDepositSQL,
		dep.ID, dep.UID, dep.Chain, dep.TxHash, dep.LogIndex, dep.FromAddress,
		dep.ToAddress, dep.Amount, dep.Confirmations, dep.CreatedAt,
	))
}

// GetChainDeposit returns one chain deposit row, or pgx.ErrNoRows.
func (d *DAO) GetChainDeposit(ctx context.Context, id int64) (*ChainDeposit, error) {
	return scanChainDeposit(d.q.QueryRow(ctx, getChainDepositSQL, id))
}

// MarkDepositStatus performs a conditional status transition on the chain
// deposit. false means another worker already moved the row; the caller treats
// the operation as an idempotent replay.
func (d *DAO) MarkDepositStatus(ctx context.Context, id int64, fromStatus, toStatus int16, now int64) (bool, error) {
	tag, err := d.q.Exec(ctx, markDepositStatusSQL, id, fromStatus, toStatus, now)
	if err != nil {
		return false, fmt.Errorf("mark deposit %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}
