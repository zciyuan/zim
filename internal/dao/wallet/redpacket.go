package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// 本文件是 M4 红包抢包/打赏的 DAO 预留层：表结构已建好（0024_wallet.up.sql），
// 服务侧（Redis Lua 抢包、过期退款、打赏结算）由 M4 任务实现。方法签名按
// 复式记账的幂等约束设计：(biz_order_id, uid, biz_type) 唯一键与
// (rp_id, claimer_uid) 唯一键保证资金动作可重放。

const insertRedPacketSQL = `
INSERT INTO wallet.zim_red_packet
(rp_id, biz_order_id, sender_uid, conv_id, conv_type, rp_type, target_uid,
 total_amount, total_count, claimed_amount, claimed_count, refund_amount, greeting,
 status, expire_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, 0, 0, $10, $11, $12, $13)
ON CONFLICT (biz_order_id) DO NOTHING`

const getRedPacketSQL = `
SELECT rp_id, biz_order_id, sender_uid, conv_id, conv_type, rp_type, target_uid,
       total_amount, total_count, claimed_amount, claimed_count, refund_amount,
       COALESCE(greeting, ''), status, COALESCE(expire_at, 0), COALESCE(created_at, 0)
FROM wallet.zim_red_packet
WHERE rp_id = $1`

const getRedPacketByBizSQL = `
SELECT rp_id, biz_order_id, sender_uid, conv_id, conv_type, rp_type, target_uid,
       total_amount, total_count, claimed_amount, claimed_count, refund_amount,
       COALESCE(greeting, ''), status, COALESCE(expire_at, 0), COALESCE(created_at, 0)
FROM wallet.zim_red_packet
WHERE biz_order_id = $1`

const updateRedPacketClaimedSQL = `
UPDATE wallet.zim_red_packet
SET claimed_amount = claimed_amount + $2,
    claimed_count  = claimed_count + 1,
    status = CASE WHEN claimed_count + 1 >= total_count THEN $3 ELSE status END
WHERE rp_id = $1 AND status = 1
RETURNING rp_id, biz_order_id, sender_uid, conv_id, conv_type, rp_type, target_uid,
          total_amount, total_count, claimed_amount, claimed_count, refund_amount,
          COALESCE(greeting, ''), status, COALESCE(expire_at, 0), COALESCE(created_at, 0)`

const updateRedPacketStatusSQL = `
UPDATE wallet.zim_red_packet
SET status = $3, refund_amount = $4
WHERE rp_id = $1 AND status = $2`

const insertRedPacketClaimSQL = `
INSERT INTO wallet.zim_red_packet_claim (id, rp_id, claimer_uid, amount, is_best_luck, claimed_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (rp_id, claimer_uid) DO NOTHING`

const getRedPacketClaimSQL = `
SELECT id, rp_id, claimer_uid, amount, is_best_luck, COALESCE(claimed_at, 0)
FROM wallet.zim_red_packet_claim
WHERE rp_id = $1 AND claimer_uid = $2`

func scanRedPacket(row pgx.Row) (*RedPacket, error) {
	rp := &RedPacket{}
	err := row.Scan(
		&rp.RPID, &rp.BizOrderID, &rp.SenderUID, &rp.ConvID, &rp.ConvType, &rp.RpType,
		&rp.TargetUID, &rp.TotalAmount, &rp.TotalCount, &rp.ClaimedAmount, &rp.ClaimedCount,
		&rp.RefundAmount, &rp.Greeting, &rp.Status, &rp.ExpireAt, &rp.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return rp, nil
}

// CreateRedPacket inserts a red packet. false means the biz_order_id exists.
func (d *DAO) CreateRedPacket(ctx context.Context, rp *RedPacket) (bool, error) {
	tag, err := d.q.Exec(ctx, insertRedPacketSQL,
		rp.RPID, rp.BizOrderID, rp.SenderUID, rp.ConvID, rp.ConvType, rp.RpType,
		rp.TargetUID, rp.TotalAmount, rp.TotalCount, rp.Greeting, rp.Status,
		rp.ExpireAt, rp.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert red packet %s: %w", rp.BizOrderID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetRedPacket returns the red packet row, or pgx.ErrNoRows.
func (d *DAO) GetRedPacket(ctx context.Context, rpID int64) (*RedPacket, error) {
	return scanRedPacket(d.q.QueryRow(ctx, getRedPacketSQL, rpID))
}

// GetRedPacketByBiz returns the red packet row by biz_order_id, or
// pgx.ErrNoRows. Used for idempotent send replay.
func (d *DAO) GetRedPacketByBiz(ctx context.Context, bizOrderID string) (*RedPacket, error) {
	return scanRedPacket(d.q.QueryRow(ctx, getRedPacketByBizSQL, bizOrderID))
}

// GetRedPacketForUpdate locks the red packet row.
func (d *DAO) GetRedPacketForUpdate(ctx context.Context, rpID int64) (*RedPacket, error) {
	return scanRedPacket(d.q.QueryRow(ctx, getRedPacketSQL+` FOR UPDATE`, rpID))
}

// UpdateRedPacketClaimed accumulates one claim. It refuses (ErrNoRows) when the
// packet is not ACTIVE; the caller must combine it with the claim insert and
// the claimer's credit in one transaction.
func (d *DAO) UpdateRedPacketClaimed(ctx context.Context, rpID, amount int64, depletedStatus int16) (*RedPacket, error) {
	return scanRedPacket(d.q.QueryRow(ctx, updateRedPacketClaimedSQL, rpID, amount, depletedStatus))
}

// UpdateRedPacketStatus moves the packet state conditionally (M4 过期退款).
func (d *DAO) UpdateRedPacketStatus(ctx context.Context, rpID int64, fromStatus, toStatus int16, refundAmount int64) (bool, error) {
	tag, err := d.q.Exec(ctx, updateRedPacketStatusSQL, rpID, fromStatus, toStatus, refundAmount)
	if err != nil {
		return false, fmt.Errorf("update red packet %d: %w", rpID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// CreateRedPacketClaim inserts a claim; the (rp_id, claimer_uid) key makes
// double-claim impossible. false means already claimed.
func (d *DAO) CreateRedPacketClaim(ctx context.Context, c *RedPacketClaim) (bool, error) {
	tag, err := d.q.Exec(ctx, insertRedPacketClaimSQL, c.ID, c.RPID, c.ClaimerUID, c.Amount, c.IsBestLuck, c.ClaimedAt)
	if err != nil {
		return false, fmt.Errorf("insert red packet claim %d: %w", c.RPID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetRedPacketClaim returns one claim row, or pgx.ErrNoRows.
func (d *DAO) GetRedPacketClaim(ctx context.Context, rpID, claimerUID int64) (*RedPacketClaim, error) {
	c := &RedPacketClaim{}
	err := d.q.QueryRow(ctx, getRedPacketClaimSQL, rpID, claimerUID).Scan(
		&c.ID, &c.RPID, &c.ClaimerUID, &c.Amount, &c.IsBestLuck, &c.ClaimedAt,
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

const listExpiredActiveSQL = `
SELECT rp_id, biz_order_id, sender_uid, conv_id, conv_type, rp_type, target_uid,
       total_amount, total_count, claimed_amount, claimed_count, refund_amount,
       COALESCE(greeting, ''), status, COALESCE(expire_at, 0), COALESCE(created_at, 0)
FROM wallet.zim_red_packet
WHERE status = 1 AND expire_at <= $1
ORDER BY expire_at ASC
LIMIT $2`

// ListExpiredActive returns ACTIVE packets past their deadline, oldest first.
func (d *DAO) ListExpiredActive(ctx context.Context, now int64, limit int) ([]*RedPacket, error) {
	rows, err := d.q.Query(ctx, listExpiredActiveSQL, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired red packets: %w", err)
	}
	defer rows.Close()
	result := make([]*RedPacket, 0, limit)
	for rows.Next() {
		rp := &RedPacket{}
		if err = rows.Scan(
			&rp.RPID, &rp.BizOrderID, &rp.SenderUID, &rp.ConvID, &rp.ConvType, &rp.RpType,
			&rp.TargetUID, &rp.TotalAmount, &rp.TotalCount, &rp.ClaimedAmount, &rp.ClaimedCount,
			&rp.RefundAmount, &rp.Greeting, &rp.Status, &rp.ExpireAt, &rp.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan red packet: %w", err)
		}
		result = append(result, rp)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate red packets: %w", err)
	}
	return result, nil
}

const markBestLuckSQL = `
UPDATE wallet.zim_red_packet_claim c
SET is_best_luck = (c.id = (
  SELECT id FROM wallet.zim_red_packet_claim
  WHERE rp_id = $1
  ORDER BY amount DESC, claimed_at ASC
  LIMIT 1))
WHERE c.rp_id = $1`

// MarkBestLuck flags the luckiest claim of a depleted packet (max amount;
// earliest claimed_at wins ties, ADR-0043 §4.4.3).
func (d *DAO) MarkBestLuck(ctx context.Context, rpID int64) error {
	if _, err := d.q.Exec(ctx, markBestLuckSQL, rpID); err != nil {
		return fmt.Errorf("mark best luck %d: %w", rpID, err)
	}
	return nil
}

const listRedPacketClaimsSQL = `
SELECT id, rp_id, claimer_uid, amount, is_best_luck, COALESCE(claimed_at, 0)
FROM wallet.zim_red_packet_claim
WHERE rp_id = $1
ORDER BY claimed_at ASC
LIMIT $2`

// ListRedPacketClaims returns claim rows for a packet, earliest first.
func (d *DAO) ListRedPacketClaims(ctx context.Context, rpID int64, limit int) ([]*RedPacketClaim, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	rows, err := d.q.Query(ctx, listRedPacketClaimsSQL, rpID, limit)
	if err != nil {
		return nil, fmt.Errorf("list red packet claims: %w", err)
	}
	defer rows.Close()
	result := make([]*RedPacketClaim, 0, limit)
	for rows.Next() {
		c := &RedPacketClaim{}
		if err = rows.Scan(&c.ID, &c.RPID, &c.ClaimerUID, &c.Amount, &c.IsBestLuck, &c.ClaimedAt); err != nil {
			return nil, fmt.Errorf("scan red packet claim: %w", err)
		}
		result = append(result, c)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate red packet claims: %w", err)
	}
	return result, nil
}
