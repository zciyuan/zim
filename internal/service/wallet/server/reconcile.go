package wallet

import (
	"context"
	"fmt"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// doDailyReconcile 校验总账恒等式：
//
//	Σ User(available+frozen) + Σ SysAccounts == TotalDeposits - TotalWithdrawals
//
// 其中 TotalDeposits = Σ DEPOSIT 流水（用户+热钱包双记），TotalWithdrawals =
// Σ |WITHDRAW_SUCCESS| 流水。差值非零必须返回 error 并计数 fail 指标。
// 全部 int64 精确比较，禁止浮点。
func (s *Service) doDailyReconcile(ctx context.Context) (*reconcileResult, error) {
	result := &reconcileResult{}

	userTotal, err := s.repo.SumBalances(ctx, false)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	systemTotal, err := s.repo.SumBalances(ctx, true)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	deposits, err := s.repo.SumLedger(ctx, walletdao.BizTypeDeposit)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	withdrawSum, err := s.repo.SumLedger(ctx, walletdao.BizTypeWithdrawSuccess)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	withdrawals := -withdrawSum // 流水为负，取绝对值求和

	result.UserTotal = userTotal
	result.SystemTotal = systemTotal
	result.Deposits = deposits
	result.Withdrawals = withdrawals
	result.Difference = userTotal + systemTotal - (deposits - withdrawals)

	if result.Difference != 0 {
		s.metric(metricReconcileTotal, map[string]string{labelResult: "fail"})
		return result, errs.ErrInternal.WithMessage(
			fmt.Sprintf("wallet reconcile mismatch: difference = %d", result.Difference),
		)
	}
	s.metric(metricReconcileTotal, map[string]string{labelResult: "ok"})
	return result, nil
}
