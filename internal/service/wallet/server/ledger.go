package wallet

import (
	"context"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// errLedgerDuplicate 是事务内控制流信号：目标幂等键已存在，整个事务必须回滚，
// 由重试循环转为幂等重放。业务错误一律走 errs，此哨兵不出事务边界。
var errLedgerDuplicate = dueerrors.New("ledger duplicate")

// doTransfer 在一个事务内原子完成「转出方出账 + 转入方入账 + 两条流水」。
// 幂等键为 (biz_order_id, uid, biz_type)：并发重放会在行锁上串行化，重放时
// 直接返回原始流水结果；乐观锁 version 冲突重试 maxLedgerAttempts 次。
// 两条腿共用顶层 biz_order_id：调用方只传一次，此处向两条腿下传，保证流水
// 幂等键一致（否则两腿会各写一条空单号流水，重放永远查不到）。
func (s *Service) doTransfer(ctx context.Context, t transferArgs) (*transferResult, error) {
	t.Debit.BizOrderID = t.BizOrderID
	t.Credit.BizOrderID = t.BizOrderID
	now := s.now().UnixMilli()
	for attempt := 0; attempt < maxLedgerAttempts; attempt++ {
		result := &transferResult{}
		err := s.begin(ctx, func(tx Tx) error {
			debit, replayed, err := s.debitLocked(ctx, tx, now, t.Debit)
			if err != nil {
				return err
			}
			if replayed {
				// 幂等重放：不做任何写入，返回原始两条流水。
				credit, err := tx.GetLedger(ctx, t.BizOrderID, t.Credit.UID, t.Credit.BizType)
				if err != nil {
					return errs.ErrInternal.WithCause(err)
				}
				result.Replayed = true
				result.Debit, err = tx.GetLedger(ctx, t.BizOrderID, t.Debit.UID, t.Debit.BizType)
				if err != nil {
					return errs.ErrInternal.WithCause(err)
				}
				result.Credit = credit
				return nil
			}
			credit, creditReplayed, err := s.creditLocked(ctx, tx, now, t.Credit)
			if err != nil {
				return err
			}
			if creditReplayed {
				// 半程冲突：对方事务已整体提交，回滚本次出账后重试，下一轮走重放分支。
				return errLedgerDuplicate
			}
			result.Debit = debit
			result.Credit = credit
			return nil
		})
		switch {
		case err == nil:
			return result, nil
		case dueerrors.Is(err, pgx.ErrNoRows), dueerrors.Is(err, errLedgerDuplicate):
			continue
		default:
			return nil, err
		}
	}
	return nil, errs.ErrInternal.WithMessage("ledger contention")
}

// debitLocked 在事务内扣减/冻结转出方并写流水。行锁（FOR UPDATE）先行，
// 再做幂等检查：同 biz_order_id 的并发重放会在锁上串行化，第二个事务拿到锁后
// 命中幂等键，返回 replayed 且不写任何行。version CAS 失败返回 pgx.ErrNoRows
// 供外层重试。账户不存在 200001、冻结 200002、余额不足 200003。
func (s *Service) debitLocked(ctx context.Context, tx Tx, now int64, a debitArgs) (*walletdao.Ledger, bool, error) {
	acc, err := tx.GetAccountForUpdate(ctx, a.UID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, false, errs.ErrWalletAccountNotExist
		}
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	if acc.Status == walletdao.AccountStatusFrozen {
		return nil, false, errs.ErrWalletAccountFrozen
	}
	exists, err := tx.LedgerExists(ctx, a.BizOrderID, a.UID, a.BizType)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	if exists {
		return nil, true, nil
	}

	deltaAvailable, deltaFrozen := int64(0), int64(0)
	switch {
	case a.FromFrozen:
		// 提现成功：烧掉冻结资金，资金真正离开账户。
		if acc.FrozenBalance < a.Amount {
			return nil, false, errs.ErrInsufficientBalance
		}
		deltaFrozen = -a.Amount
	case a.Freeze:
		// 冻结：资金仍在账户内，从可用转入冻结（两边同时变化，净值不变）。
		if acc.AvailableBalance < a.Amount {
			return nil, false, errs.ErrInsufficientBalance
		}
		deltaAvailable = -a.Amount
		deltaFrozen = a.Amount
	default:
		if acc.AvailableBalance < a.Amount {
			return nil, false, errs.ErrInsufficientBalance
		}
		deltaAvailable = -a.Amount
	}

	after, err := tx.UpdateBalances(ctx, a.UID, deltaAvailable, deltaFrozen, acc.Version, now)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			// 版本冲突：并发事务已改行，交由外层重试。
			return nil, false, pgx.ErrNoRows
		}
		return nil, false, errs.ErrInternal.WithCause(err)
	}

	ledgerRow := &walletdao.Ledger{
		ID:           s.ids.Next(),
		BizOrderID:   a.BizOrderID,
		UID:          a.UID,
		BizType:      a.BizType,
		Amount:       -a.Amount, // 出账为负
		BalanceAfter: after.AvailableBalance,
		FrozenAfter:  after.FrozenBalance,
		Remark:       a.Remark,
		CreatedAt:    now,
	}
	inserted, err := tx.InsertLedger(ctx, ledgerRow)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	if !inserted {
		return nil, false, errLedgerDuplicate
	}
	return ledgerRow, false, nil
}

// creditLocked 在事务内入账/解冻转入方并写流水。与 debitLocked 同一幂等与
// 重试语义。入账不受账户冻结限制：冻结只挡出账，链上入金不能丢。
func (s *Service) creditLocked(ctx context.Context, tx Tx, now int64, a creditArgs) (*walletdao.Ledger, bool, error) {
	acc, err := tx.GetAccountForUpdate(ctx, a.UID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, false, errs.ErrWalletAccountNotExist
		}
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	exists, err := tx.LedgerExists(ctx, a.BizOrderID, a.UID, a.BizType)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	if exists {
		return nil, true, nil
	}

	deltaAvailable, deltaFrozen := int64(0), int64(0)
	if a.Unfreeze {
		if acc.FrozenBalance < a.Amount {
			return nil, false, errs.ErrInsufficientBalance
		}
		deltaAvailable = a.Amount
		deltaFrozen = -a.Amount
	} else {
		deltaAvailable = a.Amount
	}

	after, err := tx.UpdateBalances(ctx, a.UID, deltaAvailable, deltaFrozen, acc.Version, now)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, false, pgx.ErrNoRows
		}
		return nil, false, errs.ErrInternal.WithCause(err)
	}

	ledgerRow := &walletdao.Ledger{
		ID:           s.ids.Next(),
		BizOrderID:   a.BizOrderID,
		UID:          a.UID,
		BizType:      a.BizType,
		Amount:       a.Amount, // 入账为正
		BalanceAfter: after.AvailableBalance,
		FrozenAfter:  after.FrozenBalance,
		Remark:       a.Remark,
		CreatedAt:    now,
	}
	inserted, err := tx.InsertLedger(ctx, ledgerRow)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	if !inserted {
		return nil, false, errLedgerDuplicate
	}
	return ledgerRow, false, nil
}
