package wallet

import (
	"context"
	"testing"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// TestDepositPendingThenCredited 确认数不足不入账只记进度；达标后入账且回调
// 幂等：重复回调不重复入账。
func TestDepositPendingThenCredited(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedSystemAccounts()

	addr, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainERC20)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}

	args := depositArgs{
		Chain:         walletdao.ChainERC20,
		TxHash:        "0xtx1",
		LogIndex:      0,
		FromAddress:   "external",
		ToAddress:     addr.Address,
		Amount:        10_000_000,
		Confirmations: 3,
	}
	res, err := s.doDeposit(context.Background(), args)
	if err != nil {
		t.Fatalf("deposit pending: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusPending {
		t.Fatalf("want PENDING, got %d", res.Status)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 0 {
		t.Fatalf("credited before confirmations: balance = %d", got)
	}

	args.Confirmations = 12
	res, err = s.doDeposit(context.Background(), args)
	if err != nil {
		t.Fatalf("deposit credited: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusCredited || res.CreditedUID != 1 {
		t.Fatalf("want CREDITED uid 1, got %+v", res)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 10_000_000 {
		t.Fatalf("user balance: got %d, want 10_000_000", got)
	}
	if got := mustAccount(t, f, SysHotWallet).AvailableBalance; got != 10_000_000 {
		t.Fatalf("hot wallet: got %d, want 10_000_000", got)
	}

	// 重复回调幂等：不再入账。
	res, err = s.doDeposit(context.Background(), args)
	if err != nil {
		t.Fatalf("deposit replay: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusCredited {
		t.Fatalf("replay: want CREDITED, got %d", res.Status)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 10_000_000 {
		t.Fatalf("double credit: balance = %d", got)
	}
	if got := mustAccount(t, f, SysHotWallet).AvailableBalance; got != 10_000_000 {
		t.Fatalf("hot wallet double credit: balance = %d", got)
	}
}

// TestDepositDustIgnored 低于 1 USDT 的充值记 DUST_IGNORED，不入账且重复回调幂等。
func TestDepositDustIgnored(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedSystemAccounts()

	addr, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainTRC20)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}

	args := depositArgs{
		Chain: walletdao.ChainTRC20, TxHash: "txdust", LogIndex: 0,
		FromAddress: "external", ToAddress: addr.Address,
		Amount: 500_000, Confirmations: 999,
	}
	res, err := s.doDeposit(context.Background(), args)
	if err != nil {
		t.Fatalf("dust deposit: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusDustIgnored {
		t.Fatalf("want DUST_IGNORED, got %d", res.Status)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 0 {
		t.Fatalf("dust credited: balance = %d", got)
	}

	res, err = s.doDeposit(context.Background(), args)
	if err != nil {
		t.Fatalf("dust replay: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusDustIgnored {
		t.Fatalf("replay: want DUST_IGNORED, got %d", res.Status)
	}
}

// TestDepositUnknownAddress 未知充币地址必须报错，不得静默丢弃。
func TestDepositUnknownAddress(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedSystemAccounts()

	_, err := s.doDeposit(context.Background(), depositArgs{
		Chain: walletdao.ChainTRC20, TxHash: "txunk", LogIndex: 0,
		FromAddress: "external", ToAddress: "Taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Amount: 10_000_000, Confirmations: 999,
	})
	if !errs.IsCode(err, 900003) {
		t.Fatalf("want code 900003, got %v", err)
	}
}

// TestDepositUnsupportedChain 不支持链返回 200013。
func TestDepositUnsupportedChain(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)

	_, err := s.doDeposit(context.Background(), depositArgs{
		Chain: "SOL", TxHash: "txsol", LogIndex: 0,
		FromAddress: "external", ToAddress: "whatever",
		Amount: 10_000_000, Confirmations: 999,
	})
	if !errs.IsCode(err, 200013) {
		t.Fatalf("want code 200013, got %v", err)
	}
}

// TestGetDepositAddressStable 同一用户同链地址稳定复用，不同链各自独立。
func TestGetDepositAddressStable(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)

	first, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainTRC20)
	if err != nil {
		t.Fatalf("get address: err = %v", err)
	}
	second, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainTRC20)
	if err != nil {
		t.Fatalf("get address again: err = %v", err)
	}
	if first.Address != second.Address || first.ID != second.ID {
		t.Fatalf("address not stable: %s vs %s", first.Address, second.Address)
	}
	other, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainERC20)
	if err != nil {
		t.Fatalf("get erc20 address: err = %v", err)
	}
	if other.Address == first.Address {
		t.Fatalf("chains share one address: %s", other.Address)
	}
	if _, err := s.doGetDepositAddress(context.Background(), 1, "SOL"); !errs.IsCode(err, 200013) {
		t.Fatalf("unsupported chain: want 200013, got %v", err)
	}
}

// TestDepositAutoCreatesAccount 未开户用户收到链上充值：先建户再入账，不丢钱。
func TestDepositAutoCreatesAccount(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedSystemAccounts()

	addr, err := s.doGetDepositAddress(context.Background(), 42, walletdao.ChainBEP20)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}
	// 模拟开户前钱包账户被删除的场景。
	f.mu.Lock()
	delete(f.accounts, 42)
	f.mu.Unlock()

	res, err := s.doDeposit(context.Background(), depositArgs{
		Chain: walletdao.ChainBEP20, TxHash: "txauto", LogIndex: 0,
		FromAddress: "external", ToAddress: addr.Address,
		Amount: 10_000_000, Confirmations: 999,
	})
	if err != nil {
		t.Fatalf("auto-create deposit: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusCredited {
		t.Fatalf("want CREDITED, got %d", res.Status)
	}
	if got := mustAccount(t, f, 42).AvailableBalance; got != 10_000_000 {
		t.Fatalf("auto-created balance: got %d, want 10_000_000", got)
	}
}
