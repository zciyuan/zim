package wallet

import (
	"context"
	"testing"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// TestEnsureAccountIdempotent 建户幂等：重复调用不报错不重建。
func TestEnsureAccountIdempotent(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)

	if err := s.doEnsureAccount(context.Background(), 1); err != nil {
		t.Fatalf("ensure account: err = %v", err)
	}
	if err := s.doEnsureAccount(context.Background(), 1); err != nil {
		t.Fatalf("ensure account replay: err = %v", err)
	}
	acc := mustAccount(t, f, 1)
	if acc.AccountType != walletdao.AccountTypeUser || acc.Status != walletdao.AccountStatusNormal {
		t.Fatalf("account shape: %+v", acc)
	}
	if err := s.doEnsureAccount(context.Background(), -5); !errs.IsCode(err, 900003) {
		t.Fatalf("negative uid: want 900003, got %v", err)
	}
}

// TestEnsureSystemAccounts 系统过渡户幂等建立且类型正确。
func TestEnsureSystemAccounts(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)

	if err := s.EnsureSystemAccounts(context.Background()); err != nil {
		t.Fatalf("ensure system accounts: err = %v", err)
	}
	if err := s.EnsureSystemAccounts(context.Background()); err != nil {
		t.Fatalf("ensure system accounts replay: err = %v", err)
	}
	for _, uid := range [...]int64{SysHotWallet, SysRpEscrow, SysPlatformFee, SysWithdrawFee} {
		acc := mustAccount(t, f, uid)
		if acc.AccountType != walletdao.AccountTypeSystem {
			t.Fatalf("account %d: want system type, got %d", uid, acc.AccountType)
		}
	}
}

// TestGetBalanceAndFreeze 查询余额、冻结/解冻、不存在账户错误码。
func TestGetBalanceAndFreeze(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedAccount(1, 1_000_000, 2_000_000)

	acc, err := s.doGetAccount(context.Background(), 1)
	if err != nil {
		t.Fatalf("get account: err = %v", err)
	}
	if acc.AvailableBalance != 1_000_000 || acc.FrozenBalance != 2_000_000 {
		t.Fatalf("balance: %+v", acc)
	}
	if _, err := s.doGetAccount(context.Background(), 99); !errs.IsCode(err, 200001) {
		t.Fatalf("missing account: want 200001, got %v", err)
	}

	if err := s.doFreezeAccount(context.Background(), 1); err != nil {
		t.Fatalf("freeze: err = %v", err)
	}
	if got := mustAccount(t, f, 1).Status; got != walletdao.AccountStatusFrozen {
		t.Fatalf("status: want FROZEN, got %d", got)
	}
	if err := s.doUnfreezeAccount(context.Background(), 1); err != nil {
		t.Fatalf("unfreeze: err = %v", err)
	}
	if got := mustAccount(t, f, 1).Status; got != walletdao.AccountStatusNormal {
		t.Fatalf("status: want NORMAL, got %d", got)
	}
}

// TestPayPasswordFlow 设置/校验支付密码，错误密码 200004，弱密码拒绝。
func TestPayPasswordFlow(t *testing.T) {
	f := newFakeRepo()
	s, _, _, _ := newTestService(f)
	f.seedAccount(1, 0, 0)

	if err := s.doSetPayPassword(context.Background(), 1, "123"); !errs.IsCode(err, 200004) {
		t.Fatalf("weak password: want 200004, got %v", err)
	}
	if err := s.doSetPayPassword(context.Background(), 1, "strong-password-1"); err != nil {
		t.Fatalf("set pay password: err = %v", err)
	}
	if err := s.doVerifyPayPassword(context.Background(), 1, "strong-password-1"); err != nil {
		t.Fatalf("verify pay password: err = %v", err)
	}
	if err := s.doVerifyPayPassword(context.Background(), 1, "wrong"); !errs.IsCode(err, 200004) {
		t.Fatalf("wrong password: want 200004, got %v", err)
	}
	if err := s.doVerifyPayPassword(context.Background(), 99, "x"); !errs.IsCode(err, 200001) {
		t.Fatalf("missing account: want 200001, got %v", err)
	}
}

// TestTOTPFlow 设置/校验 TOTP：正确码通过、错误码 200005、未开通 200005。
func TestTOTPFlow(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	f.seedAccount(1, 0, 0)

	secret, err := s.doSetTOTP(context.Background(), 1)
	if err != nil {
		t.Fatalf("set totp: err = %v", err)
	}
	if secret == "" {
		t.Fatalf("set totp: empty secret")
	}

	acc := mustAccount(t, f, 1)
	code := totpCodeAt(mustTOTPKey(t, acc.TOTPSecretEnc), clock.now().Unix())
	if err := s.doVerifyTOTP(context.Background(), 1, code); err != nil {
		t.Fatalf("verify totp: err = %v", err)
	}
	if err := s.doVerifyTOTP(context.Background(), 1, "000000"); !errs.IsCode(err, 200005) {
		t.Fatalf("wrong code: want 200005, got %v", err)
	}
	if err := s.doVerifyTOTP(context.Background(), 2, "000000"); !errs.IsCode(err, 200005) {
		t.Fatalf("unprovisioned: want 200005, got %v", err)
	}
}

func mustTOTPKey(t *testing.T, secret string) []byte {
	t.Helper()
	key, err := decodeTOTPKey(secret)
	if err != nil {
		t.Fatalf("decode totp key: err = %v", err)
	}
	return key
}
