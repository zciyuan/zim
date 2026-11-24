package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const accountColumns = `uid, account_type, available_balance, frozen_balance,
       COALESCE(pay_password_hash, ''), COALESCE(totp_secret_enc, ''),
       withdraw_freeze_at, status, version, COALESCE(updated_at, 0)`

const insertAccountSQL = `
INSERT INTO wallet.zim_wallet_account
(uid, account_type, available_balance, frozen_balance, pay_password_hash,
 totp_secret_enc, withdraw_freeze_at, status, version, updated_at)
VALUES ($1, $2, 0, 0, '', '', 0, 1, 0, $3)
ON CONFLICT (uid) DO NOTHING`

const getAccountSQL = `SELECT ` + accountColumns + ` FROM wallet.zim_wallet_account WHERE uid = $1`

const getAccountForUpdateSQL = getAccountSQL + ` FOR UPDATE`

const updateBalancesSQL = `
UPDATE wallet.zim_wallet_account
SET available_balance = available_balance + $2,
    frozen_balance    = frozen_balance + $3,
    version           = version + 1,
    updated_at        = $5
WHERE uid = $1 AND version = $4
  AND available_balance + $2 >= 0
  AND frozen_balance + $3 >= 0
RETURNING ` + accountColumns

const setAccountStatusSQL = `
UPDATE wallet.zim_wallet_account SET status = $2, version = version + 1, updated_at = $3 WHERE uid = $1`

const setPayPasswordHashSQL = `
UPDATE wallet.zim_wallet_account
SET pay_password_hash = $2, withdraw_freeze_at = $3, version = version + 1, updated_at = $4
WHERE uid = $1`

const setTOTPSecretSQL = `
UPDATE wallet.zim_wallet_account
SET totp_secret_enc = $2, withdraw_freeze_at = $3, version = version + 1, updated_at = $4
WHERE uid = $1`

// New returns a pool-backed wallet DAO.
func New(pool *pgxpool.Pool) *DAO {
	return &DAO{q: pool, pool: pool}
}

// NewTx returns a transaction-backed wallet DAO. All statements run on tx; the
// transaction boundary stays with the service (pgx.BeginFunc).
func NewTx(tx pgx.Tx) *DAO {
	return &DAO{q: tx}
}

func scanAccount(row pgx.Row) (*Account, error) {
	acc := &Account{}
	err := row.Scan(
		&acc.UID, &acc.AccountType, &acc.AvailableBalance, &acc.FrozenBalance,
		&acc.PayPasswordHash, &acc.TOTPSecretEnc, &acc.WithdrawFreezeAt,
		&acc.Status, &acc.Version, &acc.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return acc, nil
}

// CreateAccount inserts a zero-balance account. It is idempotent: an existing
// uid leaves the row untouched.
func (d *DAO) CreateAccount(ctx context.Context, uid int64, accountType int16, now int64) error {
	_, err := d.q.Exec(ctx, insertAccountSQL, uid, accountType, now)
	if err != nil {
		return fmt.Errorf("create wallet account %d: %w", uid, err)
	}
	return nil
}

// GetAccount returns the account row, or pgx.ErrNoRows when absent.
func (d *DAO) GetAccount(ctx context.Context, uid int64) (*Account, error) {
	return scanAccount(d.q.QueryRow(ctx, getAccountSQL, uid))
}

// GetAccountForUpdate locks the account row for the rest of the transaction.
func (d *DAO) GetAccountForUpdate(ctx context.Context, uid int64) (*Account, error) {
	return scanAccount(d.q.QueryRow(ctx, getAccountForUpdateSQL, uid))
}

// UpdateBalances applies deltas with an optimistic-lock CAS on version. It
// returns the new account state, or pgx.ErrNoRows when the version moved
// (concurrent writer) or a balance guard would go negative.
func (d *DAO) UpdateBalances(ctx context.Context, uid int64, deltaAvailable, deltaFrozen int64, expectVersion, now int64) (*Account, error) {
	return scanAccount(d.q.QueryRow(ctx, updateBalancesSQL, uid, deltaAvailable, deltaFrozen, expectVersion, now))
}

// SetAccountStatus changes the account status without a version guard.
func (d *DAO) SetAccountStatus(ctx context.Context, uid int64, status int16, now int64) error {
	tag, err := d.q.Exec(ctx, setAccountStatusSQL, uid, status, now)
	if err != nil {
		return fmt.Errorf("set account %d status: %w", uid, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetPayPasswordHash stores the pay password hash and starts the withdrawal
// freeze window. The hash must never be logged.
func (d *DAO) SetPayPasswordHash(ctx context.Context, uid int64, hash string, freezeAt, now int64) error {
	tag, err := d.q.Exec(ctx, setPayPasswordHashSQL, uid, hash, freezeAt, now)
	if err != nil {
		return fmt.Errorf("set pay password for %d: %w", uid, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetTOTPSecret stores the TOTP secret and starts the withdrawal freeze
// window. The secret must never be logged.
func (d *DAO) SetTOTPSecret(ctx context.Context, uid int64, secret string, freezeAt, now int64) error {
	tag, err := d.q.Exec(ctx, setTOTPSecretSQL, uid, secret, freezeAt, now)
	if err != nil {
		return fmt.Errorf("set totp secret for %d: %w", uid, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
