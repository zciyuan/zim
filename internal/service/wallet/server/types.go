// Package wallet implements the USDT double-entry ledger service (ADR-0043,
// M3). All amounts are int64 micro-USDT (1 USDT = 1_000_000); float types are
// forbidden. Money movement is idempotent per (biz_order_id, uid, biz_type).
package wallet

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// System transitional accounts. They are created idempotently by
// EnsureSystemAccounts and must never be frozen or withdrawn from.
const (
	SysHotWallet   int64 = -1 // 热钱包：链上充提的镜像账户
	SysRpEscrow    int64 = -2 // 红包托管户（M4）
	SysPlatformFee int64 = -3 // 平台收入户
	SysWithdrawFee int64 = -4 // 提现手续费收入户
)

// 金额与账务边界常量。
const (
	dustThreshold       int64 = 1_000_000 // 低于 1 USDT 的充值视为粉尘
	maxLedgerAttempts   int   = 5         // 乐观锁冲突重试上限
	defaultConfirmTRC20 int32 = 20        // 充币确认数默认阈值
	defaultConfirmERC20 int32 = 12
	defaultConfirmBEP20 int32 = 15
)

// Config carries the wallet tunables loaded from etc.zim.wallet.*.
type Config struct {
	MinWithdraw      int64            // 单笔最小提现额（微 USDT）
	MaxSmallWithdraw int64            // 免审阈值：不超过则自动发送
	FreezeWindowMs   int64            // 改密/2FA 后提币冻结窗口（毫秒）
	FeeBps           int64            // 提现手续费基点（万分比，floor(gross*bps/10000)）
	Confirmations    map[string]int32 // 链 → 充币确认数阈值
}

// ChainSigner is the seam for the real KMS/chain adapter. The DummySigner in
// dummy.go backs local runs; production swaps in a KMS-backed implementation.
type ChainSigner interface {
	// DeriveAddress derives the deposit address for one HD index.
	DeriveAddress(ctx context.Context, chain string, derivationIdx int32) (string, error)
	// Broadcast sends the withdrawal transaction and returns its tx hash.
	Broadcast(ctx context.Context, chain, toAddress string, amount int64) (string, error)
	// GetConfirmations returns the on-chain confirmation count.
	GetConfirmations(ctx context.Context, chain, txHash string) (int32, error)
}

// Repository is the pool-level persistence surface: idempotent writes and
// reads that do not need a row lock. The concrete pgx DAO in
// internal/dao/wallet implements it; the in-memory fake in types_test.go
// mirrors it for unit tests.
type Repository interface {
	CreateAccount(ctx context.Context, uid int64, accountType int16, now int64) error
	GetAccount(ctx context.Context, uid int64) (*walletdao.Account, error)
	SetAccountStatus(ctx context.Context, uid int64, status int16, now int64) error
	SetPayPasswordHash(ctx context.Context, uid int64, hash string, freezeAt, now int64) error
	SetTOTPSecret(ctx context.Context, uid int64, secret string, freezeAt, now int64) error

	SumLedger(ctx context.Context, bizType int16) (int64, error)
	SumBalances(ctx context.Context, system bool) (int64, error)

	GetDepositAddress(ctx context.Context, uid int64, chain string) (*walletdao.DepositAddress, error)
	GetDepositAddressByAddress(ctx context.Context, address string) (*walletdao.DepositAddress, error)
	InsertDepositAddress(ctx context.Context, a *walletdao.DepositAddress) (bool, error)
	NextDerivationIndex(ctx context.Context) (int32, error)
	HasDepositAddress(ctx context.Context, address string) (bool, error)

	UpsertChainDeposit(ctx context.Context, d *walletdao.ChainDeposit) (*walletdao.ChainDeposit, error)
	GetChainDeposit(ctx context.Context, id int64) (*walletdao.ChainDeposit, error)
	MarkDepositStatus(ctx context.Context, id int64, fromStatus, toStatus int16, now int64) (bool, error)

	GetWithdrawal(ctx context.Context, bizOrderID string) (*walletdao.ChainWithdrawal, error)

	// M4: red packets.
	GetRedPacket(ctx context.Context, rpID int64) (*walletdao.RedPacket, error)
	GetRedPacketByBiz(ctx context.Context, bizOrderID string) (*walletdao.RedPacket, error)
	ListExpiredActive(ctx context.Context, now int64, limit int) ([]*walletdao.RedPacket, error)
	GetRedPacketClaim(ctx context.Context, rpID, claimerUID int64) (*walletdao.RedPacketClaim, error)
	ListRedPacketClaims(ctx context.Context, rpID int64, limit int) ([]*walletdao.RedPacketClaim, error)

	// M4: tip orders.
	CreateTipOrder(ctx context.Context, t *walletdao.TipOrder) (bool, error)
	GetTipOrder(ctx context.Context, bizOrderID string) (*walletdao.TipOrder, error)
}

// Tx is the transaction-scoped surface passed by beginTxFunc. Every method
// runs inside one pgx transaction; a returned error rolls it back.
type Tx interface {
	Repository
	GetAccountForUpdate(ctx context.Context, uid int64) (*walletdao.Account, error)
	UpdateBalances(ctx context.Context, uid int64, deltaAvailable, deltaFrozen int64, expectVersion, now int64) (*walletdao.Account, error)

	InsertLedger(ctx context.Context, l *walletdao.Ledger) (bool, error)
	GetLedger(ctx context.Context, bizOrderID string, uid int64, bizType int16) (*walletdao.Ledger, error)
	LedgerExists(ctx context.Context, bizOrderID string, uid int64, bizType int16) (bool, error)

	CreateWithdrawal(ctx context.Context, w *walletdao.ChainWithdrawal) (bool, error)
	GetWithdrawalForUpdate(ctx context.Context, bizOrderID string) (*walletdao.ChainWithdrawal, error)
	UpdateWithdrawal(ctx context.Context, bizOrderID string, fromStatus, toStatus int16, txHash string, auditAdminID int64, auditRemark string, now int64) (bool, error)

	// M4: red packets inside the money transaction.
	CreateRedPacket(ctx context.Context, rp *walletdao.RedPacket) (bool, error)
	GetRedPacketForUpdate(ctx context.Context, rpID int64) (*walletdao.RedPacket, error)
	UpdateRedPacketClaimed(ctx context.Context, rpID, amount int64, depletedStatus int16) (*walletdao.RedPacket, error)
	UpdateRedPacketStatus(ctx context.Context, rpID int64, fromStatus, toStatus int16, refundAmount int64) (bool, error)
	CreateRedPacketClaim(ctx context.Context, c *walletdao.RedPacketClaim) (bool, error)
	MarkBestLuck(ctx context.Context, rpID int64) error
}

// beginTxFunc opens one transaction and hands the tx-scoped store to fn. The
// production wiring uses pgx.BeginFunc; tests use the fake's snapshot/restore.
type beginTxFunc func(ctx context.Context, fn func(tx Tx) error) error

// metricFunc records one labeled counter. Production increments
// metrics.Default; tests inject a recorder to assert the ok/fail counters.
type metricFunc func(name string, labels map[string]string)

// Service is the wallet mesh service provider.
type Service struct {
	pb.UnimplementedWalletServer

	proxy  *mesh.Proxy
	repo   Repository
	begin  beginTxFunc
	cfg    Config
	signer ChainSigner
	ids    *snowflake.Generator
	now    func() time.Time
	metric metricFunc
	redis  redis.UniversalClient
	events EventPublisher
}

// debitArgs describes one debit leg: money leaves the account's available
// balance (Freeze=false), moves into frozen (Freeze=true), or burns frozen
// directly (FromFrozen=true, e.g. withdrawal success).
type debitArgs struct {
	BizOrderID string
	UID        int64
	BizType    int16
	Amount     int64
	Freeze     bool
	FromFrozen bool
	Remark     string
}

// creditArgs describes one credit leg: money enters available (Unfreeze=false)
// or is released from frozen back into available (Unfreeze=true).
type creditArgs struct {
	BizOrderID string
	UID        int64
	BizType    int16
	Amount     int64
	Unfreeze   bool
	Remark     string
}

// transferArgs couples a debit and a credit leg into one atomic transfer.
type transferArgs struct {
	BizOrderID string
	Debit      debitArgs
	Credit     creditArgs
}

// transferResult reports the ledger rows written (or replayed) by a transfer.
type transferResult struct {
	Replayed bool
	Debit    *walletdao.Ledger
	Credit   *walletdao.Ledger
}

type withdrawArgs struct {
	BizOrderID  string
	UID         int64
	Chain       string
	ToAddress   string
	Amount      int64
	PayPassword string
	TOTPCode    string
}

type depositArgs struct {
	Chain         string
	TxHash        string
	LogIndex      int32
	FromAddress   string
	ToAddress     string
	Amount        int64
	Confirmations int32
}

type withdrawResult struct {
	Order    *walletdao.ChainWithdrawal
	Replayed bool
}

type depositResult struct {
	Status      int16
	CreditedUID int64
}

type reconcileResult struct {
	UserTotal   int64
	SystemTotal int64
	Deposits    int64
	Withdrawals int64
	Difference  int64
}

// redPacketArgs is the send request shape (M4).
type redPacketArgs struct {
	BizOrderID  string
	SenderUID   int64
	ConvID      string
	ConvType    int16
	RpType      int16
	TargetUID   int64
	TotalAmount int64
	TotalCount  int
	Greeting    string
}

// redPacketClaimArgs is the claim request shape (M4).
type redPacketClaimArgs struct {
	RpID       int64
	ClaimerUID int64
}

// tipArgs is the tip settlement request shape (M4).
type tipArgs struct {
	BizOrderID  string
	Scene       int16
	SceneID     string
	SenderUID   int64
	ReceiverUID int64
	GiftID      int32
	GiftCount   int32
	ComboID     string
	GrossAmount int64
	Message     string
}

// tipResult reports the settled tip order.
type tipResult struct {
	Order       *walletdao.TipOrder
	PlatformFee int64
	NetAmount   int64
}
