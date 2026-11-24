package wallet

import (
	"context"
	"testing"
	"time"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// seedDeposit 走真实充值路径给用户与热钱包注资（保证热钱包有余额可出账）。
func seedDeposit(t *testing.T, f *fakeRepo, s *Service, uid int64, chain string, amount int64) *walletdao.DepositAddress {
	t.Helper()
	addr, err := s.doGetDepositAddress(context.Background(), uid, chain)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}
	res, err := s.doDeposit(context.Background(), depositArgs{
		Chain:         chain,
		TxHash:        "tx-" + addr.Address,
		LogIndex:      0,
		FromAddress:   "external",
		ToAddress:     addr.Address,
		Amount:        amount,
		Confirmations: 999,
	})
	if err != nil {
		t.Fatalf("deposit: err = %v", err)
	}
	if res.Status != walletdao.DepositStatusCredited {
		t.Fatalf("deposit: want CREDITED, got %d", res.Status)
	}
	return addr
}

// seededWithdrawUser 建好系统户、通过充值给用户注资、设置支付密码并跳过 24h
// 提币冷却窗口。热钱包随充值镜像注资，后续结算出账不会违反 CHECK >= 0。
func seededWithdrawUser(t *testing.T, f *fakeRepo, s *Service, clock *testClock, uid, available int64) {
	t.Helper()
	f.seedSystemAccounts()
	seedDeposit(t, f, s, uid, walletdao.ChainTRC20, available)
	if err := s.doSetPayPassword(context.Background(), uid, "paypass123"); err != nil {
		t.Fatalf("set pay password: err = %v", err)
	}
	clock.advance(25 * time.Hour) // 越过改密 24h 提币冷却
}

func withdrawRequest(uid int64, biz string, amount int64) withdrawArgs {
	return withdrawArgs{
		BizOrderID:  biz,
		UID:         uid,
		Chain:       walletdao.ChainTRC20,
		ToAddress:   "Taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 34 位 base58 合法 mock
		Amount:      amount,
		PayPassword: "paypass123",
	}
}

// TestWithdrawFreezeWindow 改密 24h 内提现被 200010 拒绝，窗口过后成功。
// 用户余额经真实充值路径注资：热钱包镜像同步有余额，结算出账才能成功。
func TestWithdrawFreezeWindow(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	f.seedSystemAccounts()
	seedDeposit(t, f, s, 1, walletdao.ChainTRC20, 100_000_000)
	if err := s.doSetPayPassword(context.Background(), 1, "paypass123"); err != nil {
		t.Fatalf("set pay password: err = %v", err)
	}

	_, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-001", 50_000_000))
	if !errs.IsCode(err, 200010) {
		t.Fatalf("want code 200010, got %v", err)
	}

	clock.advance(24*time.Hour + time.Second)
	result, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-001", 50_000_000))
	if err != nil {
		t.Fatalf("withdraw after window: err = %v", err)
	}
	if result.Order.Status != walletdao.WithdrawStatusSuccess {
		t.Fatalf("small withdraw: want SUCCESS, got status %d", result.Order.Status)
	}
}

// TestWithdrawToInternalAddress 提现到站内充币地址被 200011 拦截。
func TestWithdrawToInternalAddress(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	addr, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainTRC20)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}

	args := withdrawRequest(1, "wd-internal", 10_000_000)
	args.ToAddress = addr.Address
	_, err = s.doWithdraw(context.Background(), args)
	if !errs.IsCode(err, 200011) {
		t.Fatalf("want code 200011, got %v", err)
	}
}

// TestWithdrawBelowMin 低于最小提现额被 200012 拦截。
func TestWithdrawBelowMin(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	_, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-min", 999_999))
	if !errs.IsCode(err, 200012) {
		t.Fatalf("want code 200012, got %v", err)
	}
}

// TestWithdrawChainUnsupported 不支持的链返回 200013。
func TestWithdrawChainUnsupported(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	args := withdrawRequest(1, "wd-chain", 10_000_000)
	args.Chain = "SOL"
	_, err := s.doWithdraw(context.Background(), args)
	if !errs.IsCode(err, 200013) {
		t.Fatalf("want code 200013, got %v", err)
	}
}

// TestWithdrawBadAddress 非法地址返回 200014。
func TestWithdrawBadAddress(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	args := withdrawRequest(1, "wd-addr", 10_000_000)
	args.ToAddress = "not-an-address"
	_, err := s.doWithdraw(context.Background(), args)
	if !errs.IsCode(err, 200014) {
		t.Fatalf("want code 200014, got %v", err)
	}
}

// TestWithdrawWrongPayPassword 支付密码错误返回 200004。
func TestWithdrawWrongPayPassword(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	args := withdrawRequest(1, "wd-pwd", 10_000_000)
	args.PayPassword = "wrong-password"
	_, err := s.doWithdraw(context.Background(), args)
	if !errs.IsCode(err, 200004) {
		t.Fatalf("want code 200004, got %v", err)
	}
}

// TestWithdrawWrongTOTP 已开通 2FA 时错误码返回 200005。
func TestWithdrawWrongTOTP(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	secret, err := s.doSetTOTP(context.Background(), 1)
	if err != nil {
		t.Fatalf("set totp: err = %v", err)
	}
	if secret == "" {
		t.Fatalf("set totp: empty secret")
	}
	clock.advance(25 * time.Hour) // 再次越过 2FA 后的 24h 冷却

	args := withdrawRequest(1, "wd-totp", 10_000_000)
	args.TOTPCode = "000000"
	_, err = s.doWithdraw(context.Background(), args)
	if !errs.IsCode(err, 200005) {
		t.Fatalf("want code 200005, got %v", err)
	}
}

// TestWithdrawHappyPathSmall 小额自动发送：gross 全冻结后结算，用户 available
// 减 gross、frozen 归零，热钱包减 actual+fee，手续费户 +fee，全程 int64 守恒。
func TestWithdrawHappyPathSmall(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	result, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-small", 20_000_000))
	if err != nil {
		t.Fatalf("withdraw: err = %v", err)
	}
	if result.Order.Status != walletdao.WithdrawStatusSuccess {
		t.Fatalf("want SUCCESS, got status %d", result.Order.Status)
	}

	// 20 USDT gross，feeBps=500 → fee = 1 USDT，actual = 19 USDT。
	const gross = int64(20_000_000)
	fee := gross * 500 / 10_000
	actual := gross - fee

	user := mustAccount(t, f, 1)
	if user.AvailableBalance != 100_000_000-gross || user.FrozenBalance != 0 {
		t.Fatalf("user balances: available = %d, frozen = %d", user.AvailableBalance, user.FrozenBalance)
	}
	// 充值镜像 100 USDT，结算后热钱包剩 80 USDT（actual 19 + fee 1 出账）。
	hot := mustAccount(t, f, SysHotWallet)
	if hot.AvailableBalance != 100_000_000-gross {
		t.Fatalf("hot wallet: got %d, want %d", hot.AvailableBalance, 100_000_000-gross)
	}
	feeAccount := mustAccount(t, f, SysWithdrawFee)
	if feeAccount.AvailableBalance != fee {
		t.Fatalf("fee account: got %d, want %d", feeAccount.AvailableBalance, fee)
	}
	if result.Order.NetworkFee != fee || result.Order.ActualAmount != actual {
		t.Fatalf("fee split: network_fee = %d, actual = %d", result.Order.NetworkFee, result.Order.ActualAmount)
	}
	if result.Order.TxHash == "" {
		t.Fatalf("want broadcasted tx hash")
	}
}

// TestWithdrawReplay 同 biz_order_id 重放不二次冻结，返回原订单状态。
func TestWithdrawReplay(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 100_000_000)

	first, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-replay", 10_000_000))
	if err != nil {
		t.Fatalf("first withdraw: err = %v", err)
	}
	second, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-replay", 10_000_000))
	if err != nil {
		t.Fatalf("replay withdraw: err = %v", err)
	}
	if !second.Replayed {
		t.Fatalf("want Replayed = true")
	}
	if second.Order.Status != first.Order.Status || second.Order.ID != first.Order.ID {
		t.Fatalf("replay: want original order (%d,%d), got (%d,%d)",
			first.Order.Status, first.Order.ID, second.Order.Status, second.Order.ID)
	}
	user := mustAccount(t, f, 1)
	if user.AvailableBalance != 100_000_000-10_000_000 {
		t.Fatalf("double freeze: available = %d", user.AvailableBalance)
	}
}

// TestAuditWithdrawRejectUnfreeze 大额进 PENDING_AUDIT，驳回原路解冻。
func TestAuditWithdrawRejectUnfreeze(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 1_000_000_000)

	result, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-audit", 600_000_000))
	if err != nil {
		t.Fatalf("withdraw: err = %v", err)
	}
	if result.Order.Status != walletdao.WithdrawStatusPendingAudit {
		t.Fatalf("large withdraw: want PENDING_AUDIT, got %d", result.Order.Status)
	}

	rejected, err := s.doAuditWithdraw(context.Background(), "wd-audit", false, 7, "risk control")
	if err != nil {
		t.Fatalf("audit reject: err = %v", err)
	}
	if rejected.Order.Status != walletdao.WithdrawStatusRejected {
		t.Fatalf("want REJECTED, got %d", rejected.Order.Status)
	}

	user := mustAccount(t, f, 1)
	if user.AvailableBalance != 1_000_000_000 || user.FrozenBalance != 0 {
		t.Fatalf("unfreeze broken: available = %d, frozen = %d", user.AvailableBalance, user.FrozenBalance)
	}

	// 驳回重放幂等：不再解冻第二次。
	again, err := s.doAuditWithdraw(context.Background(), "wd-audit", false, 7, "")
	if err != nil {
		t.Fatalf("audit reject replay: err = %v", err)
	}
	if !again.Replayed || again.Order.Status != walletdao.WithdrawStatusRejected {
		t.Fatalf("reject replay: want idempotent REJECTED, got status %d replayed %v",
			again.Order.Status, again.Replayed)
	}
}

// TestAuditWithdrawApproveSettles 大额审核通过：广播 → 结算成功，用户 frozen
// 烧掉，热钱包按 gross 出账，手续费入账。
func TestAuditWithdrawApproveSettles(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 1_000_000_000)

	result, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-approve", 600_000_000))
	if err != nil {
		t.Fatalf("withdraw: err = %v", err)
	}
	if result.Order.Status != walletdao.WithdrawStatusPendingAudit {
		t.Fatalf("want PENDING_AUDIT, got %d", result.Order.Status)
	}

	approved, err := s.doAuditWithdraw(context.Background(), "wd-approve", true, 7, "approved")
	if err != nil {
		t.Fatalf("audit approve: err = %v", err)
	}
	if approved.Order.Status != walletdao.WithdrawStatusSuccess {
		t.Fatalf("want SUCCESS, got %d", approved.Order.Status)
	}

	user := mustAccount(t, f, 1)
	if user.AvailableBalance != 400_000_000 || user.FrozenBalance != 0 {
		t.Fatalf("user balances: available = %d, frozen = %d", user.AvailableBalance, user.FrozenBalance)
	}

	// 审核通过重放幂等成功。
	again, err := s.doAuditWithdraw(context.Background(), "wd-approve", true, 7, "")
	if err != nil {
		t.Fatalf("approve replay: err = %v", err)
	}
	if !again.Replayed || again.Order.Status != walletdao.WithdrawStatusSuccess {
		t.Fatalf("approve replay: want idempotent SUCCESS, got status %d replayed %v",
			again.Order.Status, again.Replayed)
	}
}

// TestAuditBroadcastFailureRefunds 广播失败 → FAILED_REFUNDED 且原路解冻。
func TestAuditBroadcastFailureRefunds(t *testing.T) {
	f := newFakeRepo()
	s, clock, signer, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 1_000_000_000)

	_, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-fail", 600_000_000))
	if err != nil {
		t.Fatalf("withdraw: err = %v", err)
	}

	signer.broadcastErr = context.DeadlineExceeded
	_, err = s.doAuditWithdraw(context.Background(), "wd-fail", true, 7, "")
	if !errs.IsCode(err, 900003) {
		t.Fatalf("want code 900003, got %v", err)
	}

	order, err := f.GetWithdrawal(context.Background(), "wd-fail")
	if err != nil {
		t.Fatalf("get withdrawal: err = %v", err)
	}
	if order.Status != walletdao.WithdrawStatusFailedRefund {
		t.Fatalf("want FAILED_REFUNDED, got %d", order.Status)
	}
	user := mustAccount(t, f, 1)
	if user.AvailableBalance != 1_000_000_000 || user.FrozenBalance != 0 {
		t.Fatalf("refund broken: available = %d, frozen = %d", user.AvailableBalance, user.FrozenBalance)
	}
}

// TestWithdrawInsufficientBalance 余额不足返回 200003。
func TestWithdrawInsufficientBalance(t *testing.T) {
	f := newFakeRepo()
	s, clock, _, _ := newTestService(f)
	seededWithdrawUser(t, f, s, clock, 1, 10_000_000)

	_, err := s.doWithdraw(context.Background(), withdrawRequest(1, "wd-poor", 50_000_000))
	if !errs.IsCode(err, 200003) {
		t.Fatalf("want code 200003, got %v", err)
	}
}
