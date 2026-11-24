package wallet

import (
	"context"
	"fmt"
)

// 本文件是 M4 打赏/礼物的 DAO 预留层。打赏结算的三段账（TIP_OUT / TIP_IN /
// PLATFORM_FEE）与红包共用 ledger.go 的复式记账原语，由 M4 服务侧实现。

const insertTipOrderSQL = `
INSERT INTO wallet.zim_tip_order
(tip_id, biz_order_id, scene, scene_id, sender_uid, receiver_uid, gift_id, gift_count,
 combo_id, gross_amount, platform_fee, net_amount, message, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (biz_order_id) DO NOTHING`

const getTipOrderSQL = `
SELECT tip_id, biz_order_id, scene, scene_id, sender_uid, receiver_uid, gift_id, gift_count,
       COALESCE(combo_id, ''), gross_amount, platform_fee, net_amount,
       COALESCE(message, ''), COALESCE(created_at, 0)
FROM wallet.zim_tip_order
WHERE biz_order_id = $1`

// CreateTipOrder inserts a tip order. false means the biz_order_id exists.
func (d *DAO) CreateTipOrder(ctx context.Context, t *TipOrder) (bool, error) {
	tag, err := d.q.Exec(ctx, insertTipOrderSQL,
		t.TipID, t.BizOrderID, t.Scene, t.SceneID, t.SenderUID, t.ReceiverUID,
		t.GiftID, t.GiftCount, t.ComboID, t.GrossAmount, t.PlatformFee, t.NetAmount,
		t.Message, t.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert tip order %s: %w", t.BizOrderID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetTipOrder returns one tip order, or pgx.ErrNoRows.
func (d *DAO) GetTipOrder(ctx context.Context, bizOrderID string) (*TipOrder, error) {
	t := &TipOrder{}
	err := d.q.QueryRow(ctx, getTipOrderSQL, bizOrderID).Scan(
		&t.TipID, &t.BizOrderID, &t.Scene, &t.SceneID, &t.SenderUID, &t.ReceiverUID,
		&t.GiftID, &t.GiftCount, &t.ComboID, &t.GrossAmount, &t.PlatformFee,
		&t.NetAmount, &t.Message, &t.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}
