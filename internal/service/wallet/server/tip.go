package wallet

import (
	"context"

	dueerrors "github.com/dobyte/due/v2/errors"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// doSendTip settles one tip order in a single transaction with three ledger
// legs (ADR-0043 §4.5): the sender pays gross, the receiver gets net, the
// platform fee account gets the difference. The fee is computed with integer
// floor arithmetic: platform_fee = floor(gross * fee_bps / 10000).
func (s *Service) doSendTip(ctx context.Context, a tipArgs) (*tipResult, error) {
	if a.SenderUID <= 0 || a.ReceiverUID <= 0 || a.SenderUID == a.ReceiverUID {
		return nil, errs.ErrTipSelf
	}
	if a.GrossAmount <= 0 || a.BizOrderID == "" {
		return nil, errs.ErrInternal.WithMessage("tip amount/order required")
	}
	if a.GiftID != 0 && a.GiftCount <= 0 {
		a.GiftCount = 1
	}
	if existing, err := s.repo.GetTipOrder(ctx, a.BizOrderID); err == nil && existing != nil {
		return &tipResult{Order: existing, PlatformFee: existing.PlatformFee, NetAmount: existing.NetAmount}, nil
	}
	platformFee := a.GrossAmount * s.cfg.FeeBps / 10_000
	netAmount := a.GrossAmount - platformFee
	if netAmount <= 0 {
		return nil, errs.ErrInternal.WithMessage("fee exceeds amount")
	}
	now := s.now().UnixMilli()
	order := &walletdao.TipOrder{
		TipID: s.ids.Next(), BizOrderID: a.BizOrderID, Scene: a.Scene, SceneID: a.SceneID,
		SenderUID: a.SenderUID, ReceiverUID: a.ReceiverUID, GiftID: a.GiftID,
		GiftCount: a.GiftCount, ComboID: a.ComboID, GrossAmount: a.GrossAmount,
		PlatformFee: platformFee, NetAmount: netAmount, Message: a.Message, CreatedAt: now,
	}

	err := s.begin(ctx, func(tx Tx) error {
		inserted, err := tx.CreateTipOrder(ctx, order)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !inserted {
			return errLedgerDuplicate
		}
		// 1) 打赏者出账 gross（TIP_OUT）。
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: order.BizOrderID, UID: order.SenderUID,
			BizType: walletdao.BizTypeTipOut, Amount: order.GrossAmount,
			Remark: "tip out",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		// 2) 受赏者入账 net（TIP_IN）。
		if _, replayed, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: order.BizOrderID, UID: order.ReceiverUID,
			BizType: walletdao.BizTypeTipIn, Amount: order.NetAmount,
			Remark: "tip in",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		// 3) 平台抽成入账（PLATFORM_FEE）。
		if order.PlatformFee > 0 {
			if _, replayed, err := s.creditLocked(ctx, tx, now, creditArgs{
				BizOrderID: order.BizOrderID, UID: SysPlatformFee,
				BizType: walletdao.BizTypePlatformFee, Amount: order.PlatformFee,
				Remark: "tip platform fee",
			}); err != nil {
				return err
			} else if replayed {
				return errLedgerDuplicate
			}
		}
		return nil
	})
	if dueerrors.Is(err, errLedgerDuplicate) {
		existing, getErr := s.repo.GetTipOrder(ctx, a.BizOrderID)
		if getErr == nil && existing != nil {
			return &tipResult{Order: existing, PlatformFee: existing.PlatformFee, NetAmount: existing.NetAmount}, nil
		}
		return nil, errs.ErrInternal.WithMessage("tip order replay lost")
	}
	if err != nil {
		return nil, err
	}
	// ADR-0043 §1.2.C: tip settled → voiceroom/msg broadcast gift effects.
	s.publishWalletEvent(func() error {
		return s.events.PublishTipSettled(ctx, order)
	}, "tip settled")
	return &tipResult{Order: order, PlatformFee: platformFee, NetAmount: netAmount}, nil
}
