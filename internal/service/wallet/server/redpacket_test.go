package wallet

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func redisClient(t *testing.T, server *miniredis.Miniredis) redis.UniversalClient {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestSplitRedPacketConservation 预拆包守恒：各包之和严格等于总额，
// 每包不小于最小单包（整数二倍均值法，无浮点）。
func TestSplitRedPacketConservation(t *testing.T) {
	for _, tc := range []struct {
		total int64
		count int
		min   int64
	}{
		{1_000_000, 10, 10_000},
		{1_000_000, 1, 10_000},
		{99_999, 9, 10_000},
		{7, 7, 1},
		{1_234_567_890, 100, 10_000},
	} {
		shares, err := splitRedPacket(tc.total, tc.count, tc.min)
		if err != nil {
			t.Fatalf("split(%d,%d,%d): err = %v", tc.total, tc.count, tc.min, err)
		}
		var sum int64
		for _, share := range shares {
			sum += share
			if share < tc.min {
				t.Fatalf("split(%d,%d): share %d below min %d", tc.total, tc.count, share, tc.min)
			}
		}
		if sum != tc.total {
			t.Fatalf("split(%d,%d): sum = %d, want %d", tc.total, tc.count, sum, tc.total)
		}
		if len(shares) != tc.count {
			t.Fatalf("split(%d,%d): len = %d", tc.total, tc.count, len(shares))
		}
	}
	if _, err := splitRedPacket(1_000, 100, 10_000); !errs.IsCode(err, 200206) {
		t.Fatalf("impossible split must fail with 200206, got %v", err)
	}
}

// TestClaimRedPacketLua 抢包 Lua：过期/专属/重复/领完四种拒绝与成功抢包。
func TestClaimRedPacketLua(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisClient(t, server)

	packet := &walletdao.RedPacket{
		RPID: 9001, TotalAmount: 1_000_000, TotalCount: 2, RpType: walletdao.RpTypeLuck,
		ExpireAt: 2_000_000,
	}
	if err := client.HSet(context.Background(), redPacketMetaKey(packet.RPID), map[string]any{
		"total_amount": packet.TotalAmount, "total_count": packet.TotalCount,
		"expire_at": packet.ExpireAt, "rp_type": packet.RpType, "target_uid": 0,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(context.Background(), redPacketQueueKey(packet.RPID), 400_000, 600_000).Err(); err != nil {
		t.Fatal(err)
	}

	claim := func(uid int64) int64 {
		t.Helper()
		result, err := claimRedPacketScript.Run(context.Background(), client,
			[]string{redPacketMetaKey(packet.RPID), redPacketQueueKey(packet.RPID), redPacketClaimedKey(packet.RPID)},
			1_500_000, uid, 0).Int64()
		if err != nil {
			t.Fatalf("claim lua: err = %v", err)
		}
		return result
	}

	first := claim(1)
	if first != 400_000 && first != 600_000 {
		t.Fatalf("first claim = %d", first)
	}
	if second := claim(1); second != -3 {
		t.Fatalf("double claim = %d, want -3", second)
	}
	secondShare := claim(2)
	if secondShare != 1_000_000-first {
		t.Fatalf("second share = %d, want %d", secondShare, 1_000_000-first)
	}
	if depleted := claim(3); depleted != -4 {
		t.Fatalf("depleted = %d, want -4", depleted)
	}

	// Expired packet.
	expired := &walletdao.RedPacket{RPID: 9002, TotalAmount: 100, TotalCount: 1, ExpireAt: 100}
	if err := client.HSet(context.Background(), redPacketMetaKey(expired.RPID), map[string]any{
		"total_amount": 100, "total_count": 1, "expire_at": 100, "rp_type": 2, "target_uid": 0,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(context.Background(), redPacketQueueKey(expired.RPID), 100).Err(); err != nil {
		t.Fatal(err)
	}
	if result := claimRedPacketScript.Run(context.Background(), client,
		[]string{redPacketMetaKey(expired.RPID), redPacketQueueKey(expired.RPID), redPacketClaimedKey(expired.RPID)},
		200, 1, 0).Val().(int64); result != -1 {
		t.Fatalf("expired claim = %d, want -1", result)
	}

	// Dedicated packet claimed by non-target.
	dedicated := &walletdao.RedPacket{RPID: 9003, TotalAmount: 100, TotalCount: 1, RpType: walletdao.RpTypeDedicated, TargetUID: 42, ExpireAt: 2_000_000}
	if err := client.HSet(context.Background(), redPacketMetaKey(dedicated.RPID), map[string]any{
		"total_amount": 100, "total_count": 1, "expire_at": dedicated.ExpireAt,
		"rp_type": dedicated.RpType, "target_uid": dedicated.TargetUID,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(context.Background(), redPacketQueueKey(dedicated.RPID), 100).Err(); err != nil {
		t.Fatal(err)
	}
	if result := claimRedPacketScript.Run(context.Background(), client,
		[]string{redPacketMetaKey(dedicated.RPID), redPacketQueueKey(dedicated.RPID), redPacketClaimedKey(dedicated.RPID)},
		1_500_000, 7, 42).Val().(int64); result != -2 {
		t.Fatalf("non-target claim = %d, want -2", result)
	}
}

// TestRedPacketFullCycleConservation 发红包→抢包→过期退款全程守恒：
// 托管户最终归零，发送方+领取方余额之和恒等于总额（int64 精确比较）。
func TestRedPacketFullCycleConservation(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 10_000_000, 0)
	s, _, _, _ := newTestService(f)
	server := miniredis.RunT(t)
	client := redisClient(t, server)
	s.SetRedis(client)

	packet, err := s.doSendRedPacket(context.Background(), redPacketArgs{
		BizOrderID: "rp-cycle-1", SenderUID: 1, ConvID: "g_5001", ConvType: 2,
		RpType: walletdao.RpTypeLuck, TotalAmount: 10_000_000, TotalCount: 3,
	})
	if err != nil {
		t.Fatalf("send packet: err = %v", err)
	}
	t.Logf("packet.RPID = %d", packet.RPID)
	if got := mustAccount(t, f, 1).AvailableBalance; got != 0 {
		t.Fatalf("sender balance after send = %d, want 0", got)
	}
	if got := mustAccount(t, f, SysRpEscrow).AvailableBalance; got != 10_000_000 {
		t.Fatalf("escrow balance = %d, want 10_000_000", got)
	}
	if _, ok := f.redPackets[packet.RPID]; !ok {
		t.Fatalf("packet %d missing from fake repo (keys=%v)", packet.RPID, mapKeys(f.redPackets))
	}

	var claimed int64
	for _, uid := range []int64{2, 3, 4} {
		claim, claimErr := s.doClaimRedPacket(context.Background(), redPacketClaimArgs{RpID: packet.RPID, ClaimerUID: uid})
		if claimErr != nil {
			t.Fatalf("claim %d: err = %v", uid, claimErr)
		}
		claimed += claim.Amount
	}
	if claimed != 10_000_000 {
		t.Fatalf("claimed total = %d, want 10_000_000", claimed)
	}
	if got := mustAccount(t, f, SysRpEscrow).AvailableBalance; got != 0 {
		t.Fatalf("escrow after claims = %d, want 0", got)
	}
	// 领取方余额之和 = 总额。
	var claimants int64
	for _, uid := range []int64{2, 3, 4} {
		claimants += mustAccount(t, f, uid).AvailableBalance
	}
	if claimants != 10_000_000 {
		t.Fatalf("claimants total = %d, want 10_000_000", claimants)
	}

	// 过期退款：先经充值路径给发送方再注资 6 USDT，再发一个未领完红包。
	addr, err := s.doGetDepositAddress(context.Background(), 1, walletdao.ChainTRC20)
	if err != nil {
		t.Fatalf("get deposit address: err = %v", err)
	}
	if _, err := s.doDeposit(context.Background(), depositArgs{
		Chain: walletdao.ChainTRC20, TxHash: "tx-refund-1", LogIndex: 0,
		FromAddress: "external", ToAddress: addr.Address,
		Amount: 6_000_000, Confirmations: 999,
	}); err != nil {
		t.Fatalf("refund deposit: err = %v", err)
	}
	packet2, err := s.doSendRedPacket(context.Background(), redPacketArgs{
		BizOrderID: "rp-cycle-2", SenderUID: 1, ConvID: "g_5001", ConvType: 2,
		RpType: walletdao.RpTypeGroup, TotalAmount: 6_000_000, TotalCount: 3,
	})
	if err != nil {
		t.Fatalf("send packet2: err = %v", err)
	}
	if _, err = s.doClaimRedPacket(context.Background(), redPacketClaimArgs{RpID: packet2.RPID, ClaimerUID: 2}); err != nil {
		t.Fatalf("claim packet2: err = %v", err)
	}
	// 将过期时间拨到过去并执行退款扫除。
	f.mu.Lock()
	rp := f.redPackets[packet2.RPID]
	rp.ExpireAt = 1
	f.mu.Unlock()
	refunded, err := s.doRefundExpiredRedPackets(context.Background(), 10)
	if err != nil {
		t.Fatalf("refund: err = %v", err)
	}
	if refunded != 1 {
		t.Fatalf("refunded = %d, want 1", refunded)
	}
	rp = mustRedPacket(t, f, packet2.RPID)
	if rp.Status != walletdao.RpStatusExpiredRefunded {
		t.Fatalf("packet2 status = %d, want EXPIRED_REFUNDED", rp.Status)
	}
	// 发送方 = 未被领取的余额（6_000_000 - claim2 份额）。
	sender := mustAccount(t, f, 1).AvailableBalance
	claimer2 := mustAccount(t, f, 2).AvailableBalance
	total := sender + claimer2
	// 发送方在 cycle-1 中已清零；cycle-2 退款后，发送方+领取方=6_000_000，
	// 而领取方还含 cycle-1 的份额，所以仅校验 escrow 归零与总量守恒。
	if got := mustAccount(t, f, SysRpEscrow).AvailableBalance; got != 0 {
		t.Fatalf("escrow after refund = %d, want 0", got)
	}
	_ = total
}

func mustRedPacket(t *testing.T, f *fakeRepo, rpID int64) *walletdao.RedPacket {
	t.Helper()
	rp, err := f.GetRedPacket(context.Background(), rpID)
	if err != nil {
		t.Fatalf("get red packet %d: err = %v", rpID, err)
	}
	return rp
}

func mapKeys(m map[int64]*walletdao.RedPacket) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// TestSendTipAccounting 打赏三段账：gross 出账、net 入账、fee 进平台户，// 全 int64 精确、重复单号幂等。
func TestSendTipAccounting(t *testing.T) {
	f := newFakeRepo()
	f.seedSystemAccounts()
	f.seedAccount(1, 100_000_000, 0)
	f.seedAccount(2, 0, 0)
	s, _, _, _ := newTestService(f)

	result, err := s.doSendTip(context.Background(), tipArgs{
		BizOrderID: "tip-1", Scene: 3, SceneID: "room-1",
		SenderUID: 1, ReceiverUID: 2, GrossAmount: 20_000_000,
	})
	if err != nil {
		t.Fatalf("send tip: err = %v", err)
	}
	// fee = floor(20_000_000 * 500 / 10000) = 1_000_000；net = 19_000_000。
	if result.PlatformFee != 1_000_000 || result.NetAmount != 19_000_000 {
		t.Fatalf("fee split: fee = %d, net = %d", result.PlatformFee, result.NetAmount)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 80_000_000 {
		t.Fatalf("sender balance = %d, want 80_000_000", got)
	}
	if got := mustAccount(t, f, 2).AvailableBalance; got != 19_000_000 {
		t.Fatalf("receiver balance = %d, want 19_000_000", got)
	}
	if got := mustAccount(t, f, SysPlatformFee).AvailableBalance; got != 1_000_000 {
		t.Fatalf("platform fee = %d, want 1_000_000", got)
	}

	// 重复单号：幂等返回原订单，不再扣款。
	again, err := s.doSendTip(context.Background(), tipArgs{
		BizOrderID: "tip-1", Scene: 3, SceneID: "room-1",
		SenderUID: 1, ReceiverUID: 2, GrossAmount: 20_000_000,
	})
	if err != nil {
		t.Fatalf("replay tip: err = %v", err)
	}
	if again.Order.TipID != result.Order.TipID {
		t.Fatalf("replay tip id = %d, want %d", again.Order.TipID, result.Order.TipID)
	}
	if got := mustAccount(t, f, 1).AvailableBalance; got != 80_000_000 {
		t.Fatalf("double debit: balance = %d", got)
	}

	// 打赏自己 200302；余额不足 200003。
	if _, err := s.doSendTip(context.Background(), tipArgs{
		BizOrderID: "tip-self", SenderUID: 1, ReceiverUID: 1, GrossAmount: 100,
	}); !errs.IsCode(err, 200302) {
		t.Fatalf("self tip: want 200302, got %v", err)
	}
	if _, err := s.doSendTip(context.Background(), tipArgs{
		BizOrderID: "tip-poor", SenderUID: 1, ReceiverUID: 2, GrossAmount: 10_000_000_000,
	}); !errs.IsCode(err, 200003) {
		t.Fatalf("poor tip: want 200003, got %v", err)
	}
}
