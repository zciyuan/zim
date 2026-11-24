package wallet

import (
	"context"
	"strconv"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func depositBizID(depositID int64) string {
	return "dep-" + strconv.FormatInt(depositID, 10)
}

// doDeposit 处理链上充值回调。幂等键 (chain, tx_hash, log_index) 唯一：
// 同一事件重复回调不会重复入账。确认数低于阈值只更新 confirmations；
// 金额 < 1 USDT 记 DUST_IGNORED；否则 CREDITED 并给用户与热钱包双记入账
// （DEPOSIT 流水 ×2），恒等式两侧同步增加。
func (s *Service) doDeposit(ctx context.Context, a depositArgs) (*depositResult, error) {
	if !isChainSupported(a.Chain) {
		return nil, errs.ErrChainNotSupported
	}
	if a.Amount <= 0 {
		return nil, errs.ErrInternal.WithMessage("invalid deposit amount")
	}

	owner, err := s.repo.GetDepositAddressByAddress(ctx, a.ToAddress)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			// 未知充币地址：账上无主，必须报错由扫描器重试/告警，不能静默丢弃。
			return nil, errs.ErrInternal.WithMessage("deposit address not owned")
		}
		return nil, errs.ErrInternal.WithCause(err)
	}

	now := s.now().UnixMilli()
	dep := &walletdao.ChainDeposit{
		ID:            s.ids.Next(),
		UID:           owner.UID,
		Chain:         a.Chain,
		TxHash:        a.TxHash,
		LogIndex:      a.LogIndex,
		FromAddress:   a.FromAddress,
		ToAddress:     a.ToAddress,
		Amount:        a.Amount,
		Confirmations: a.Confirmations,
		Status:        walletdao.DepositStatusPending,
		CreatedAt:     now,
	}
	current, err := s.repo.UpsertChainDeposit(ctx, dep)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}

	switch current.Status {
	case walletdao.DepositStatusCredited:
		// 已入账：重复回调幂等成功。
		return &depositResult{Status: walletdao.DepositStatusCredited, CreditedUID: current.UID}, nil
	case walletdao.DepositStatusDustIgnored:
		return &depositResult{Status: walletdao.DepositStatusDustIgnored}, nil
	}

	// 确认数不足：只记录进度，不入账。
	if current.Confirmations < s.confirmationsFor(a.Chain) {
		return &depositResult{Status: walletdao.DepositStatusPending}, nil
	}

	// 粉尘：低于 1 USDT 不记账，标记后回调幂等返回。
	if current.Amount < dustThreshold {
		marked, err := s.repo.MarkDepositStatus(ctx, current.ID, walletdao.DepositStatusPending, walletdao.DepositStatusDustIgnored, now)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		if marked {
			return &depositResult{Status: walletdao.DepositStatusDustIgnored}, nil
		}
		after, err := s.repo.GetChainDeposit(ctx, current.ID)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		return &depositResult{Status: after.Status}, nil
	}

	// 入账事务：条件状态迁移是仲裁器，PENDING→CREDITED 只可能成功一次。
	result := &depositResult{}
	biz := depositBizID(current.ID)
	err = s.begin(ctx, func(tx Tx) error {
		marked, err := tx.MarkDepositStatus(ctx, current.ID, walletdao.DepositStatusPending, walletdao.DepositStatusCredited, now)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !marked {
			// 并发入账已被其他事务完成：幂等成功，读取最终状态。
			after, err := tx.GetChainDeposit(ctx, current.ID)
			if err != nil {
				return errs.ErrInternal.WithCause(err)
			}
			result.Status = after.Status
			result.CreditedUID = after.UID
			return nil
		}

		// 用户账户不存在则先建户：链上入金不能丢。
		if _, err := tx.GetAccountForUpdate(ctx, current.UID); err != nil {
			if dueerrors.Is(err, pgx.ErrNoRows) {
				if err := tx.CreateAccount(ctx, current.UID, walletdao.AccountTypeUser, now); err != nil {
					return errs.ErrInternal.WithCause(err)
				}
			} else {
				return errs.ErrInternal.WithCause(err)
			}
		}

		if _, _, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: biz,
			UID:        current.UID,
			BizType:    walletdao.BizTypeDeposit,
			Amount:     current.Amount,
			Remark:     "chain deposit",
		}); err != nil {
			return err
		}
		if _, _, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: biz,
			UID:        SysHotWallet,
			BizType:    walletdao.BizTypeDeposit,
			Amount:     current.Amount,
			Remark:     "hot wallet mirror",
		}); err != nil {
			return err
		}
		result.Status = walletdao.DepositStatusCredited
		result.CreditedUID = current.UID
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// doGetDepositAddress 返回用户在某条链上的充币地址；不存在则 HD 派生占位：
// derivation_idx 递增 + 地址 mock 生成（真实 KMS 对接走 ChainSigner）。
// 唯一键 (uid, chain) 与 (address) 冲突时重试重读，保证并发建址幂等。
// 建址同时确保钱包账户存在：充值链路不依赖用户先登录开户，避免丢钱。
func (s *Service) doGetDepositAddress(ctx context.Context, uid int64, chain string) (*walletdao.DepositAddress, error) {
	if !isChainSupported(chain) {
		return nil, errs.ErrChainNotSupported
	}
	if uid <= 0 {
		return nil, errs.ErrInternal.WithMessage("invalid uid")
	}
	if err := s.doEnsureAccount(ctx, uid); err != nil {
		return nil, err
	}

	for attempt := 0; attempt < maxLedgerAttempts; attempt++ {
		addr, err := s.repo.GetDepositAddress(ctx, uid, chain)
		if err == nil {
			return addr, nil
		}
		if !dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrInternal.WithCause(err)
		}

		idx, err := s.repo.NextDerivationIndex(ctx)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		derived, err := s.signer.DeriveAddress(ctx, chain, idx)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}

		inserted, err := s.repo.InsertDepositAddress(ctx, &walletdao.DepositAddress{
			ID:            s.ids.Next(),
			UID:           uid,
			Chain:         chain,
			Address:       derived,
			DerivationIdx: idx,
			CreatedAt:     s.now().UnixMilli(),
		})
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		if inserted {
			return s.repo.GetDepositAddress(ctx, uid, chain)
		}
		// 唯一键冲突（同链并发派生）：重读，下一轮命中已有地址。
	}
	return nil, errs.ErrInternal.WithMessage("deposit address contention")
}
