package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const insertWithdrawalSQL = `
INSERT INTO wallet.zim_chain_withdrawal
(id, biz_order_id, uid, chain, to_address, amount, network_fee, actual_amount, tx_hash, status,
 audit_admin_id, audit_remark, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (biz_order_id) DO NOTHING`

const getWithdrawalSQL = `
SELECT id, biz_order_id, uid, chain, to_address, amount, network_fee, actual_amount,
       COALESCE(tx_hash, ''), status, COALESCE(audit_admin_id, 0), COALESCE(audit_remark, ''),
       COALESCE(created_at, 0), COALESCE(updated_at, 0)
FROM wallet.zim_chain_withdrawal
WHERE biz_order_id = $1`

const getWithdrawalForUpdateSQL = getWithdrawalSQL + ` FOR UPDATE`

const updateWithdrawalSQL = `
UPDATE wallet.zim_chain_withdrawal
SET status = $3,
    tx_hash = CASE WHEN $4 <> '' THEN $4 ELSE tx_hash END,
    audit_admin_id = $5,
    audit_remark = $6,
    updated_at = $7
WHERE biz_order_id = $1 AND status = $2`

func scanWithdrawal(row pgx.Row) (*ChainWithdrawal, error) {
	w := &ChainWithdrawal{}
	err := row.Scan(
		&w.ID, &w.BizOrderID, &w.UID, &w.Chain, &w.ToAddress, &w.Amount, &w.NetworkFee,
		&w.ActualAmount, &w.TxHash, &w.Status, &w.AuditAdminID, &w.AuditRemark,
		&w.CreatedAt, &w.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// CreateWithdrawal inserts the withdrawal order. false means the biz_order_id
// already exists; the caller treats the request as an idempotent replay.
func (d *DAO) CreateWithdrawal(ctx context.Context, w *ChainWithdrawal) (bool, error) {
	tag, err := d.q.Exec(ctx, insertWithdrawalSQL,
		w.ID, w.BizOrderID, w.UID, w.Chain, w.ToAddress, w.Amount, w.NetworkFee,
		w.ActualAmount, w.TxHash, w.Status, w.AuditAdminID, w.AuditRemark,
		w.CreatedAt, w.UpdatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert withdrawal %s: %w", w.BizOrderID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetWithdrawal returns the withdrawal order, or pgx.ErrNoRows.
func (d *DAO) GetWithdrawal(ctx context.Context, bizOrderID string) (*ChainWithdrawal, error) {
	return scanWithdrawal(d.q.QueryRow(ctx, getWithdrawalSQL, bizOrderID))
}

// GetWithdrawalForUpdate locks the withdrawal order row.
func (d *DAO) GetWithdrawalForUpdate(ctx context.Context, bizOrderID string) (*ChainWithdrawal, error) {
	return scanWithdrawal(d.q.QueryRow(ctx, getWithdrawalForUpdateSQL, bizOrderID))
}

// UpdateWithdrawal performs a conditional status transition. false means the
// order already moved away from fromStatus; the caller re-reads to decide.
func (d *DAO) UpdateWithdrawal(ctx context.Context, bizOrderID string, fromStatus, toStatus int16, txHash string, auditAdminID int64, auditRemark string, now int64) (bool, error) {
	tag, err := d.q.Exec(ctx, updateWithdrawalSQL, bizOrderID, fromStatus, toStatus, txHash, auditAdminID, auditRemark, now)
	if err != nil {
		return false, fmt.Errorf("update withdrawal %s: %w", bizOrderID, err)
	}
	return tag.RowsAffected() == 1, nil
}
