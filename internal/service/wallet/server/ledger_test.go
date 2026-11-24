package wallet

import (
	"context"
	"strconv"
	"sync"
	"testing"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// TestDoTransferConservation 校验三方守恒：转出方与转入方的余额之和不变，
// 两条流水的 balance_after/frozen_after 与账户终态一致，金额全 int64 精确比较。
func TestDoTransferConservation(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 1_000_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	before := balanceOf(t, f, 1) + balanceOf(t, f, 2)
	result, err := s.doTransfer(context.Background(), transferArgs{
		BizOrderID: "transfer-001",
		Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeRpSend, Amount: 400_000},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeRpClaim, Amount: 400_000},
	})
	if err != nil {
		t.Fatalf("doTransfer: err = %v", err)
	}
	if result.Replayed {
		t.Fatalf("doTransfer: want fresh transfer, got replay")
	}
	after := balanceOf(t, f, 1) + balanceOf(t, f, 2)
	if after != before {
		t.Fatalf("conservation broken: before = %d, after = %d", before, after)
	}

	acc1 := mustAccount(t, f, 1)
	acc2 := mustAccount(t, f, 2)
	if got, want := acc1.AvailableBalance, int64(600_000); got != want {
		t.Fatalf("acc1 available: got %d, want %d", got, want)
	}
	if got, want := acc2.AvailableBalance, int64(400_000); got != want {
		t.Fatalf("acc2 available: got %d, want %d", got, want)
	}
	if result.Debit.Amount != -400_000 || result.Credit.Amount != 400_000 {
		t.Fatalf("ledger signs: debit = %d, credit = %d", result.Debit.Amount, result.Credit.Amount)
	}
	if result.Debit.BalanceAfter != 600_000 || result.Debit.FrozenAfter != 0 {
		t.Fatalf("debit ledger snapshot: balance_after = %d, frozen_after = %d",
			result.Debit.BalanceAfter, result.Debit.FrozenAfter)
	}
	if result.Credit.BalanceAfter != 400_000 || result.Credit.FrozenAfter != 0 {
		t.Fatalf("credit ledger snapshot: balance_after = %d, frozen_after = %d",
			result.Credit.BalanceAfter, result.Credit.FrozenAfter)
	}
}

// TestDoTransferIdempotentReplay 幂等重放：同 biz_order_id 重放不二次扣减，
// 返回原始流水结果（ID 相同、快照相同）。
func TestDoTransferIdempotentReplay(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 1_000_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	args := transferArgs{
		BizOrderID: "transfer-replay",
		Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeTipOut, Amount: 300_000},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeTipIn, Amount: 300_000},
	}
	first, err := s.doTransfer(context.Background(), args)
	if err != nil {
		t.Fatalf("first doTransfer: err = %v", err)
	}
	second, err := s.doTransfer(context.Background(), args)
	if err != nil {
		t.Fatalf("replay doTransfer: err = %v", err)
	}
	if !second.Replayed {
		t.Fatalf("replay: want Replayed = true, got false")
	}
	if got, want := balanceOf(t, f, 1), int64(700_000); got != want {
		t.Fatalf("replay double-debited: acc1 = %d, want %d", got, want)
	}
	if got, want := balanceOf(t, f, 2), int64(300_000); got != want {
		t.Fatalf("replay double-credited: acc2 = %d, want %d", got, want)
	}
	if second.Debit == nil || second.Credit == nil {
		t.Fatalf("replay: want original ledger rows, got nil")
	}
	if second.Debit.ID != first.Debit.ID || second.Credit.ID != first.Credit.ID {
		t.Fatalf("replay: want original IDs (%d,%d), got (%d,%d)",
			first.Debit.ID, first.Credit.ID, second.Debit.ID, second.Credit.ID)
	}
	if second.Debit.Amount != first.Debit.Amount || second.Credit.Amount != first.Credit.Amount {
		t.Fatalf("replay: ledger amounts changed")
	}
}

// TestDoTransferInsufficientBalance 余额不足返回 200003 且不改任何余额。
func TestDoTransferInsufficientBalance(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 100_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	_, err := s.doTransfer(context.Background(), transferArgs{
		BizOrderID: "transfer-poor",
		Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeRpSend, Amount: 200_000},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeRpClaim, Amount: 200_000},
	})
	if !errs.IsCode(err, 200003) {
		t.Fatalf("want code 200003, got %v", err)
	}
	if got, want := balanceOf(t, f, 1), int64(100_000); got != want {
		t.Fatalf("balance changed on failure: got %d, want %d", got, want)
	}
	if got, want := balanceOf(t, f, 2), int64(0); got != want {
		t.Fatalf("credit applied on failure: got %d, want %d", got, want)
	}
}

// TestDoTransferAccountNotExist 转出方不存在返回 200001。
func TestDoTransferAccountNotExist(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	_, err := s.doTransfer(context.Background(), transferArgs{
		BizOrderID: "transfer-missing",
		Debit:      debitArgs{UID: 99, BizType: walletdao.BizTypeRpSend, Amount: 100_000},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeRpClaim, Amount: 100_000},
	})
	if !errs.IsCode(err, 200001) {
		t.Fatalf("want code 200001, got %v", err)
	}
}

// TestDoTransferFrozenAccount 冻结账户出账被拒 200002。
func TestDoTransferFrozenAccount(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 1_000_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	if err := s.doFreezeAccount(context.Background(), 1); err != nil {
		t.Fatalf("freeze account: err = %v", err)
	}
	_, err := s.doTransfer(context.Background(), transferArgs{
		BizOrderID: "transfer-frozen",
		Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeRpSend, Amount: 100_000},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeRpClaim, Amount: 100_000},
	})
	if !errs.IsCode(err, 200002) {
		t.Fatalf("want code 200002, got %v", err)
	}
}

// TestDoTransferConcurrentNoOverspend 并发扣减不超扣：150 余额被 20 个并发
// 100 转账争抢，成功次数 ≤ 1，余额始终 ≥ 0 且最终守恒。
func TestDoTransferConcurrentNoOverspend(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 150_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	const workers = 20
	var wg sync.WaitGroup
	successes := make(chan int, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.doTransfer(context.Background(), transferArgs{
				BizOrderID: "transfer-conc-" + strconvItoa(i),
				Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeRpSend, Amount: 100_000},
				Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeRpClaim, Amount: 100_000},
			})
			if err == nil {
				successes <- 1
			}
		}(i)
	}
	wg.Wait()
	close(successes)

	count := 0
	for range successes {
		count++
	}
	if count > 1 {
		t.Fatalf("overspend: %d transfers succeeded from a 150_000 balance", count)
	}
	if count < 1 {
		t.Fatalf("want at least one success, got 0")
	}

	acc1 := mustAccount(t, f, 1)
	acc2 := mustAccount(t, f, 2)
	if acc1.AvailableBalance < 0 || acc2.AvailableBalance < 0 {
		t.Fatalf("negative balance: acc1 = %d, acc2 = %d", acc1.AvailableBalance, acc2.AvailableBalance)
	}
	if got, want := balanceOf(t, f, 1)+balanceOf(t, f, 2), int64(150_000); got != want {
		t.Fatalf("conservation broken under concurrency: got %d, want %d", got, want)
	}
	if got, want := acc2.AvailableBalance, int64(100_000); got != want {
		t.Fatalf("credits sum wrong: got %d, want %d", got, want)
	}
}

// TestDebitLockedShapes 覆盖冻结/烧冻结两种出账形态的余额与流水快照。
func TestDebitLockedShapes(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 1_000_000, 0)
	s, _, _, _ := newTestService(f)

	// 冻结：available → frozen，净值不变，流水为负。
	err := s.begin(context.Background(), func(tx Tx) error {
		_, replayed, err := s.debitLocked(context.Background(), tx, s.now().UnixMilli(), debitArgs{
			BizOrderID: "freeze-001", UID: 1, BizType: walletdao.BizTypeWithdrawFreeze, Amount: 600_000, Freeze: true,
		})
		if err != nil {
			t.Fatalf("freeze: err = %v", err)
		}
		if replayed {
			t.Fatalf("freeze: unexpected replay")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("begin: err = %v", err)
	}
	acc := mustAccount(t, f, 1)
	if acc.AvailableBalance != 400_000 || acc.FrozenBalance != 600_000 {
		t.Fatalf("freeze balances: available = %d, frozen = %d", acc.AvailableBalance, acc.FrozenBalance)
	}

	// 烧冻结：frozen 直接减少，available 不变。
	err = s.begin(context.Background(), func(tx Tx) error {
		_, replayed, err := s.debitLocked(context.Background(), tx, s.now().UnixMilli(), debitArgs{
			BizOrderID: "burn-001", UID: 1, BizType: walletdao.BizTypeWithdrawSuccess, Amount: 600_000, FromFrozen: true,
		})
		if err != nil {
			t.Fatalf("burn: err = %v", err)
		}
		if replayed {
			t.Fatalf("burn: unexpected replay")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("begin: err = %v", err)
	}
	acc = mustAccount(t, f, 1)
	if acc.AvailableBalance != 400_000 || acc.FrozenBalance != 0 {
		t.Fatalf("burn balances: available = %d, frozen = %d", acc.AvailableBalance, acc.FrozenBalance)
	}

	// 冻结不足：200003。
	_, err = s.doTransfer(context.Background(), transferArgs{
		BizOrderID: "freeze-poor",
		Debit:      debitArgs{UID: 1, BizType: walletdao.BizTypeWithdrawFreeze, Amount: 500_000, Freeze: true},
		Credit:     creditArgs{UID: 2, BizType: walletdao.BizTypeDeposit, Amount: 500_000},
	})
	if !errs.IsCode(err, 200003) {
		t.Fatalf("want code 200003, got %v", err)
	}
}

func strconvItoa(v int) string {
	return strconv.Itoa(v)
}

func balanceOf(t *testing.T, f *fakeRepo, uid int64) int64 {
	t.Helper()
	return mustAccount(t, f, uid).AvailableBalance + mustAccount(t, f, uid).FrozenBalance
}

func mustAccount(t *testing.T, f *fakeRepo, uid int64) *walletdao.Account {
	t.Helper()
	acc, err := f.GetAccount(context.Background(), uid)
	if err != nil {
		t.Fatalf("get account %d: err = %v", uid, err)
	}
	return acc
}
