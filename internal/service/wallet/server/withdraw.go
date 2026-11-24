package wallet

import (
	"context"
	"regexp"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

var (
	trc20AddrPattern = regexp.MustCompile(`^T[1-9A-HJ-NP-Za-km-z]{33}$`)
	evmAddrPattern   = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
)

// doWithdraw 提现下单：校验支付密码+2FA → 24h 冷却 → 站内地址 → 最小额 →
// 链与地址合法性 → 冻结资金（available→frozen，WITHDRAW_FREEZE 流水）→
// 大额/异常进 PENDING_AUDIT，小额自动 APPROVED_SENDING 并同步广播结算。
// biz_order_id 幂等：重放返回原订单状态，绝不二次冻结。
func (s *Service) doWithdraw(ctx context.Context, a withdrawArgs) (*withdrawResult, error) {
	now := s.now().UnixMilli()

	if a.BizOrderID == "" || len(a.BizOrderID) > 64 {
		return nil, errs.ErrInternal.WithMessage("biz_order_id required (<=64)")
	}
	if !isChainSupported(a.Chain) {
		return nil, errs.ErrChainNotSupported
	}
	if !isValidWithdrawAddress(a.Chain, a.ToAddress) {
		return nil, errs.ErrWithdrawAddressInvalid
	}
	if a.Amount < s.cfg.MinWithdraw {
		return nil, errs.ErrWithdrawBelowMin
	}

	acc, err := s.repo.GetAccount(ctx, a.UID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrWalletAccountNotExist
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	if acc.Status == walletdao.AccountStatusFrozen {
		return nil, errs.ErrWalletAccountFrozen
	}
	if err := s.doVerifyPayPassword(ctx, a.UID, a.PayPassword); err != nil {
		return nil, err
	}
	if acc.TOTPSecretEnc != "" {
		if a.TOTPCode == "" || !verifyTOTP(acc.TOTPSecretEnc, s.now().Unix(), a.TOTPCode) {
			return nil, errs.ErrTOTPInvalid
		}
	}
	if acc.WithdrawFreezeAt > now {
		return nil, errs.ErrWithdrawFrozen
	}
	internal, err := s.repo.HasDepositAddress(ctx, a.ToAddress)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if internal {
		return nil, errs.ErrWithdrawToInternal
	}

	// 手续费基点整数向下取整（ADR-0043），gross 全部冻结，用户实收 actual。
	fee := a.Amount * s.cfg.FeeBps / 10_000
	actual := a.Amount - fee
	if actual <= 0 {
		return nil, errs.ErrInternal.WithMessage("fee exceeds amount")
	}

	status := walletdao.WithdrawStatusPendingAudit
	if a.Amount <= s.cfg.MaxSmallWithdraw {
		status = walletdao.WithdrawStatusApprovedSend
	}

	result := &withdrawResult{}
	err = s.begin(ctx, func(tx Tx) error {
		// 行锁先行（debitLocked 内部），同 biz_order_id 并发重放在锁上串行化。
		_, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: a.BizOrderID,
			UID:        a.UID,
			BizType:    walletdao.BizTypeWithdrawFreeze,
			Amount:     a.Amount,
			Freeze:     true,
			Remark:     "withdraw freeze",
		})
		if err != nil {
			return err
		}
		if replayed {
			// 幂等重放：不写任何行，返回原订单。
			order, err := tx.GetWithdrawal(ctx, a.BizOrderID)
			if err != nil {
				return errs.ErrInternal.WithCause(err)
			}
			result.Order = order
			result.Replayed = true
			return nil
		}
		order := &walletdao.ChainWithdrawal{
			ID:           s.ids.Next(),
			BizOrderID:   a.BizOrderID,
			UID:          a.UID,
			Chain:        a.Chain,
			ToAddress:    a.ToAddress,
			Amount:       a.Amount,
			NetworkFee:   fee,
			ActualAmount: actual,
			Status:       status,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		inserted, err := tx.CreateWithdrawal(ctx, order)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !inserted {
			return errs.ErrInternal.WithMessage("withdraw order conflict")
		}
		result.Order = order
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 小额自动发送：冻结已提交，广播失败走 FAILED_REFUNDED 原路退款。
	if result.Order.Status == walletdao.WithdrawStatusApprovedSend {
		if settleErr := s.broadcastAndSettle(ctx, result.Order); settleErr != nil {
			return result, settleErr
		}
	}
	return result, nil
}

// doAuditWithdraw 审核：通过 → 广播 → BROADCASTED → 结算 SUCCESS（扣 frozen，
// 记 SYS_HOT_WALLET 与 SYS_WITHDRAW_FEE）；驳回 → REJECTED 原路解冻。全程幂等。
func (s *Service) doAuditWithdraw(ctx context.Context, bizOrderID string, approve bool, adminID int64, remark string) (*withdrawResult, error) {
	order, err := s.repo.GetWithdrawal(ctx, bizOrderID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrInternal.WithMessage("withdraw order not found")
		}
		return nil, errs.ErrInternal.WithCause(err)
	}

	if approve {
		switch order.Status {
		case walletdao.WithdrawStatusSuccess:
			// 已结算：审核重放幂等成功。
			return &withdrawResult{Order: order, Replayed: true}, nil
		case walletdao.WithdrawStatusRejected, walletdao.WithdrawStatusFailedRefund:
			return nil, errs.ErrInternal.WithMessage("withdraw order already closed")
		case walletdao.WithdrawStatusPendingAudit, walletdao.WithdrawStatusApprovedSend, walletdao.WithdrawStatusBroadcasted:
			// 继续广播与结算。
		default:
			return nil, errs.ErrInternal.WithMessage("withdraw order state invalid")
		}
		if err := s.broadcastAndSettle(ctx, order); err != nil {
			return nil, err
		}
		return &withdrawResult{Order: order}, nil
	}

	// 驳回：只允许 PENDING_AUDIT。
	result := &withdrawResult{}
	err = s.begin(ctx, func(tx Tx) error {
		current, err := tx.GetWithdrawalForUpdate(ctx, bizOrderID)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		switch current.Status {
		case walletdao.WithdrawStatusRejected, walletdao.WithdrawStatusFailedRefund:
			// 已退款：审核重放幂等成功。
			result.Order = current
			result.Replayed = true
			return nil
		case walletdao.WithdrawStatusSuccess:
			return errs.ErrInternal.WithMessage("withdraw order already settled")
		case walletdao.WithdrawStatusPendingAudit:
		default:
			return errs.ErrInternal.WithMessage("withdraw order not pending audit")
		}
		moved, err := tx.UpdateWithdrawal(ctx, bizOrderID, walletdao.WithdrawStatusPendingAudit, walletdao.WithdrawStatusRejected, "", adminID, remark, s.now().UnixMilli())
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !moved {
			return pgx.ErrNoRows
		}
		// 原路解冻：frozen → available，写 WITHDRAW_REJECT 正流水。
		if _, _, err := s.creditLocked(ctx, tx, s.now().UnixMilli(), creditArgs{
			BizOrderID: bizOrderID,
			UID:        current.UID,
			BizType:    walletdao.BizTypeWithdrawReject,
			Amount:     current.Amount,
			Unfreeze:   true,
			Remark:     "withdraw rejected",
		}); err != nil {
			return err
		}
		current.Status = walletdao.WithdrawStatusRejected
		result.Order = current
		return nil
	})
	if dueerrors.Is(err, pgx.ErrNoRows) {
		// 状态被并发改变：重读一次返回最新状态。
		current, readErr := s.repo.GetWithdrawal(ctx, bizOrderID)
		if readErr != nil {
			return nil, errs.ErrInternal.WithCause(readErr)
		}
		return &withdrawResult{Order: current, Replayed: true}, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// broadcastAndSettle 广播交易并结算：用户 frozen 烧掉（WITHDRAW_SUCCESS），
// SYS_HOT_WALLET 按 gross 出账（actual 给用户 + fee 计平台收入），
// SYS_WITHDRAW_FEE 记 +fee（PLATFORM_FEE）。广播失败 → FAILED_REFUNDED 原路退款。
func (s *Service) broadcastAndSettle(ctx context.Context, order *walletdao.ChainWithdrawal) error {
	now := s.now().UnixMilli()

	txHash, err := s.signer.Broadcast(ctx, order.Chain, order.ToAddress, order.ActualAmount)
	if err != nil {
		if refundErr := s.refundFailedWithdrawal(ctx, order, now); refundErr != nil {
			return refundErr
		}
		return errs.ErrInternal.WithCause(err)
	}

	err = s.begin(ctx, func(tx Tx) error {
		current, err := tx.GetWithdrawalForUpdate(ctx, order.BizOrderID)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		switch current.Status {
		case walletdao.WithdrawStatusSuccess:
			// 已结算：重放幂等成功。
			order.Status = current.Status
			order.TxHash = current.TxHash
			return nil
		case walletdao.WithdrawStatusRejected, walletdao.WithdrawStatusFailedRefund:
			return errs.ErrInternal.WithMessage("withdraw order already closed")
		case walletdao.WithdrawStatusPendingAudit, walletdao.WithdrawStatusApprovedSend, walletdao.WithdrawStatusBroadcasted:
		default:
			return errs.ErrInternal.WithMessage("withdraw order state invalid")
		}

		moved, err := tx.UpdateWithdrawal(ctx, order.BizOrderID, current.Status, walletdao.WithdrawStatusBroadcasted, txHash, current.AuditAdminID, current.AuditRemark, now)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !moved {
			return pgx.ErrNoRows
		}

		// 1) 用户 frozen 烧掉（WITHDRAW_SUCCESS，负流水）。
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: order.BizOrderID,
			UID:        current.UID,
			BizType:    walletdao.BizTypeWithdrawSuccess,
			Amount:     current.Amount,
			FromFrozen: true,
			Remark:     "withdraw success",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}

		// 2) 热钱包按实际到账出账（WITHDRAW_SUCCESS，负流水）。
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: order.BizOrderID,
			UID:        SysHotWallet,
			BizType:    walletdao.BizTypeWithdrawSuccess,
			Amount:     current.ActualAmount,
			Remark:     "hot wallet payout",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}

		// 3) 热钱包把手续费划给平台（PLATFORM_FEE，负流水）…
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: order.BizOrderID,
			UID:        SysHotWallet,
			BizType:    walletdao.BizTypePlatformFee,
			Amount:     current.NetworkFee,
			Remark:     "withdraw fee",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}

		// 4) …平台收入户入账（PLATFORM_FEE，正流水）。
		if _, replayed, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: order.BizOrderID,
			UID:        SysWithdrawFee,
			BizType:    walletdao.BizTypePlatformFee,
			Amount:     current.NetworkFee,
			Remark:     "withdraw fee income",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}

		moved, err = tx.UpdateWithdrawal(ctx, order.BizOrderID, walletdao.WithdrawStatusBroadcasted, walletdao.WithdrawStatusSuccess, txHash, current.AuditAdminID, current.AuditRemark, now)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !moved {
			return pgx.ErrNoRows
		}
		order.Status = walletdao.WithdrawStatusSuccess
		order.TxHash = txHash
		return nil
	})
	if dueerrors.Is(err, pgx.ErrNoRows) {
		// 状态竞争：重读订单返回。
		current, readErr := s.repo.GetWithdrawal(ctx, order.BizOrderID)
		if readErr != nil {
			return errs.ErrInternal.WithCause(readErr)
		}
		order.Status = current.Status
		order.TxHash = current.TxHash
		return nil
	}
	return err
}

// refundFailedWithdrawal 广播失败时把订单置为 FAILED_REFUNDED 并原路解冻。
func (s *Service) refundFailedWithdrawal(ctx context.Context, order *walletdao.ChainWithdrawal, now int64) error {
	err := s.begin(ctx, func(tx Tx) error {
		current, err := tx.GetWithdrawalForUpdate(ctx, order.BizOrderID)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		switch current.Status {
		case walletdao.WithdrawStatusFailedRefund, walletdao.WithdrawStatusRejected, walletdao.WithdrawStatusSuccess:
			order.Status = current.Status
			return nil
		case walletdao.WithdrawStatusPendingAudit, walletdao.WithdrawStatusApprovedSend, walletdao.WithdrawStatusBroadcasted:
		default:
			return errs.ErrInternal.WithMessage("withdraw order state invalid")
		}
		moved, err := tx.UpdateWithdrawal(ctx, order.BizOrderID, current.Status, walletdao.WithdrawStatusFailedRefund, "", current.AuditAdminID, "broadcast failed", now)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !moved {
			return pgx.ErrNoRows
		}
		if _, _, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: order.BizOrderID,
			UID:        current.UID,
			BizType:    walletdao.BizTypeWithdrawReject,
			Amount:     current.Amount,
			Unfreeze:   true,
			Remark:     "withdraw broadcast failed, refunded",
		}); err != nil {
			return err
		}
		order.Status = walletdao.WithdrawStatusFailedRefund
		return nil
	})
	if dueerrors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}

// isValidWithdrawAddress 校验提现地址格式：TRC20 base58 T 开头 34 位，
// ERC20/BEP20 0x + 40 hex。真实链上校验由 ChainSigner/KMS 对接时加强。
func isValidWithdrawAddress(chain, address string) bool {
	switch chain {
	case walletdao.ChainTRC20:
		return trc20AddrPattern.MatchString(address)
	case walletdao.ChainERC20, walletdao.ChainBEP20:
		return evmAddrPattern.MatchString(address)
	default:
		return false
	}
}
