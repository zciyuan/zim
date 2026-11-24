package wallet

import (
	"context"
	"testing"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// TestDailyReconcileOK 充值+提现全链路后恒等式差值为 0，指标计 ok。
// 恒等式：Σ User + Σ SysAccounts == TotalDeposits - TotalWithdrawals。
func TestDailyReconcileOK(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, metric := newTestService(f)
	f.seedSystemAccounts()

	// 充值 100 USDT：用户 +100，热钱包 +100（DEPOSIT 双记）。
	seedDeposit(t, f, s, 1, walletdao.ChainTRC20, 100_000_000)

	// 提现 20 USDT（小额自动结算）。
	if err := s.doSetPayPassword(context.Background(), 1, "paypass123"); err != nil {
		t.Fatalf("set pay password: err = %v", err)
	}
	clock.advance(25 * 3600 * 1_000 * 1_000 * 1_000) // 25h 纳秒，越过冷却
	result, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-recon", 20_000_000))
	if err != nil {
		t.Fatalf("withdraw: err = %v", err)
	}
	if result.Order.Status != walletdao.WithdrawStatusSuccess {
		t.Fatalf("withdraw: want SUCCESS, got %d", result.Order.Status)
	}

	reconcile, err := s.doDailyReconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: err = %v", err)
	}
	if reconcile.Difference != 0 {
		t.Fatalf("difference: got %d, want 0", reconcile.Difference)
	}
	if got, want := reconcile.UserTotal, int64(80_000_000); got != want {
		t.Fatalf("user total: got %d, want %d", got, want)
	}
	if got, want := reconcile.Deposits, int64(200_000_000); got != want {
		t.Fatalf("deposits: got %d, want %d", got, want)
	}
	if got, want := reconcile.Withdrawals, int64(39_000_000); got != want {
		t.Fatalf("withdrawals: got %d, want %d", got, want)
	}
	if results := metric.results(metricReconcileTotal); len(results) != 1 || results[0] != "ok" {
		t.Fatalf("reconcile metric: got %v, want [ok]", results)
	}
}

// TestDailyReconcileMismatch 恒等式被破坏（直接篡改账户）时必须返回 error，
// 差值为非零 int64 精确值，且 fail 指标计数。
func TestDailyReconcileMismatch(t *testing.T) {
	f := newFakeRepo()
	s, _, _, metric := newTestService(f)
	f.seedSystemAccounts()
	seedDeposit(t, f, s, 1, walletdao.ChainERC20, 50_000_000)

	// 篡改用户余额模拟账实不符（仅测试替身允许）。
	f.mu.Lock()
	f.accounts[1].AvailableBalance += 1
	f.mu.Unlock()

	reconcile, err := s.doDailyReconcile(context.Background())
	if err == nil {
		t.Fatalf("reconcile: want error for nonzero difference")
	}
	if !errs.IsCode(err, 900003) {
		t.Fatalf("reconcile error: want code 900003, got %v", err)
	}
	if reconcile == nil || reconcile.Difference != 1 {
		t.Fatalf("difference: want exactly 1, got %+v", reconcile)
	}
	if results := metric.results(metricReconcileTotal); len(results) != 1 || results[0] != "fail" {
		t.Fatalf("reconcile metric: got %v, want [fail]", results)
	}
}

// TestReconcileEmptyLedger 空账本恒等式仍成立（差值为 0）。
func TestReconcileEmptyLedger(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedSystemAccounts()

	reconcile, err := s.doDailyReconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: err = %v", err)
	}
	if reconcile.Difference != 0 || reconcile.UserTotal != 0 || reconcile.SystemTotal != 0 {
		t.Fatalf("empty ledger: got %+v", reconcile)
	}
}
