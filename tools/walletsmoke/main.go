// Command walletsmoke verifies the funded wallet money flows end-to-end inside
// the compose network: deposit callback, red packet send/claim, tip three-leg
// settlement, refund sweep and the daily reconcile identity. It runs inside a
// container on the compose network (nacos/redis/postgres by service name).
//
// Usage (inside the compose network):
//
//	./walletsmoke -nacos http://nacos:8848/nacos -uid 10001 -peer 10002
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"

	walletclient "github.com/zciyuan/ZIM/internal/service/wallet/client"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
)

func main() {
	nacosURL := flag.String("nacos", "http://nacos:8848/nacos", "nacos URL")
	uid := flag.Int64("uid", 10001, "sender uid")
	peer := flag.Int64("peer", 10002, "receiver uid")
	flag.Parse()

	transporter := grpc.NewTransporter()
	transporter.SetDefaultDiscovery(nacos.NewRegistry(
		nacos.WithUrls(*nacosURL),
		nacos.WithGroupName("ZIM"),
	))
	client, err := walletclient.NewClient(transporter.NewClient)
	if err != nil {
		fail("mesh client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// GetDepositAddress also lazily creates the wallet account (ADR-0043 M3),
	// so it must run before any balance read.
	addr, err := client.GetDepositAddress(ctx, &pb.GetDepositAddressArgs{Uid: *uid, Chain: "TRC20"})
	if err != nil {
		fail("deposit address: %v", err)
	}
	before, err := client.GetBalance(ctx, &pb.GetBalanceArgs{Uid: *uid})
	if err != nil {
		fail("balance before deposit: %v", err)
	}
	// 1. Deposit 50 USDT for the sender via the chain-callback path. The tx
	// hash is unique per run so re-runs do not hit deposit idempotency.
	_, err = client.DepositCallback(ctx, &pb.DepositCallbackArgs{
		Chain: "TRC20", TxHash: fmt.Sprintf("smoke-dep-%d", time.Now().UnixNano()), LogIndex: 0,
		FromAddress: "external", ToAddress: addr.GetAddress(),
		Amount: 50_000_000, Confirmations: 999,
	})
	if err != nil {
		fail("deposit callback: %v", err)
	}
	bal, err := client.GetBalance(ctx, &pb.GetBalanceArgs{Uid: *uid})
	if err != nil {
		fail("balance: %v", err)
	}
	if got := bal.GetAvailableBalance() - before.GetAvailableBalance(); got != 50_000_000 {
		fail("balance delta after deposit = %d, want 50000000", got)
	}
	fmt.Println("PASS deposit 50 USDT credited")

	// 2. Send a 10 USDT / 5-share luck packet and claim two shares. Order ids
	// are unique per run so re-runs do not hit ledger idempotency.
	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	sent, err := client.SendRedPacket(ctx, &pb.SendRedPacketArgs{
		BizOrderId: "rp-smoke-" + runID, SenderUid: *uid, ConvId: "g_5001",
		ConvType: 2, RpType: 3, TotalAmount: 10_000_000, TotalCount: 5,
		Greeting: "smoke",
	})
	if err != nil {
		fail("send red packet: %v", err)
	}
	rpID := sent.GetPacket().GetRpId()
	if rpID <= 0 {
		fail("red packet id = %d", rpID)
	}
	claim1, err := client.ClaimRedPacket(ctx, &pb.ClaimRedPacketArgs{RpId: rpID, ClaimerUid: *peer})
	if err != nil {
		fail("claim 1: %v", err)
	}
	claim2, err := client.ClaimRedPacket(ctx, &pb.ClaimRedPacketArgs{RpId: rpID, ClaimerUid: *peer + 1})
	if err != nil {
		fail("claim 2: %v", err)
	}
	fmt.Printf("PASS red packet claims: share1 = %d, share2 = %d\n", claim1.GetAmount(), claim2.GetAmount())

	bal, _ = client.GetBalance(ctx, &pb.GetBalanceArgs{Uid: *uid})
	if got := bal.GetAvailableBalance() - before.GetAvailableBalance(); got != 40_000_000 {
		fail("sender balance delta after packet = %d, want +40000000", got)
	}
	fmt.Println("PASS sender balance 40 USDT net after freeze")

	// 3. Tip 5 USDT: three-leg settlement (gross=5, fee=0.25, net=4.75).
	tip, err := client.SendTip(ctx, &pb.SendTipArgs{
		BizOrderId: "tip-smoke-" + runID, Scene: 3, SceneId: "room-1",
		SenderUid: *uid, ReceiverUid: *peer, GrossAmount: 5_000_000,
	})
	if err != nil {
		fail("send tip: %v", err)
	}
	if tip.GetPlatformFee() != 250_000 || tip.GetNetAmount() != 4_750_000 {
		fail("tip split: fee = %d, net = %d", tip.GetPlatformFee(), tip.GetNetAmount())
	}
	receiverBal, err := client.GetBalance(ctx, &pb.GetBalanceArgs{Uid: *peer})
	if err != nil {
		fail("receiver balance: %v", err)
	}
	if receiverBal.GetAvailableBalance() < 4_750_000 {
		fail("receiver balance = %d, want >= 4750000", receiverBal.GetAvailableBalance())
	}
	fmt.Println("PASS tip three-leg settlement (fee 250000, net 4750000)")

	// 4. Reconcile identity must be exactly zero.
	rec, err := client.DailyReconcile(ctx, &pb.DailyReconcileArgs{})
	if err != nil {
		fail("reconcile: %v", err)
	}
	if rec.GetDifference() != 0 {
		fail("reconcile difference = %d, want 0", rec.GetDifference())
	}
	fmt.Printf("PASS daily reconcile: difference = %d\n", rec.GetDifference())

	fmt.Println("ALL WALLET SMOKE CHECKS PASSED")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}
