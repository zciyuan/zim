// Package wallet is the PostgreSQL DAO for the wallet schema (ADR-0043).
// All amounts are int64 micro-USDT (1 USDT = 1_000_000); float types are
// forbidden. Table names are qualified with the wallet schema.
package wallet

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Account types.
const (
	AccountTypeUser   int16 = 1
	AccountTypeSystem int16 = 2
)

// Account statuses.
const (
	AccountStatusNormal int16 = 1
	AccountStatusFrozen int16 = 2
)

// Ledger business types.
const (
	BizTypeDeposit         int16 = 1
	BizTypeWithdrawFreeze  int16 = 2
	BizTypeWithdrawSuccess int16 = 3
	BizTypeWithdrawReject  int16 = 4
	BizTypeRpSend          int16 = 5
	BizTypeRpClaim         int16 = 6
	BizTypeRpRefund        int16 = 7
	BizTypeTipOut          int16 = 8
	BizTypeTipIn           int16 = 9
	BizTypePlatformFee     int16 = 10
)

// Chain identifiers.
const (
	ChainTRC20 = "TRC20"
	ChainERC20 = "ERC20"
	ChainBEP20 = "BEP20"
)

// Chain deposit statuses.
const (
	DepositStatusPending     int16 = 0
	DepositStatusCredited    int16 = 1
	DepositStatusDustIgnored int16 = 2
)

// Chain withdrawal statuses.
const (
	WithdrawStatusPendingAudit int16 = 0
	WithdrawStatusApprovedSend int16 = 1
	WithdrawStatusBroadcasted  int16 = 2
	WithdrawStatusSuccess      int16 = 3
	WithdrawStatusRejected     int16 = 4
	WithdrawStatusFailedRefund int16 = 5
)

// Red packet statuses and types (M4 红包业务预留).
const (
	RpStatusActive          int16 = 1
	RpStatusDepleted        int16 = 2
	RpStatusExpiredRefunded int16 = 3

	RpTypeSingle    int16 = 1
	RpTypeGroup     int16 = 2
	RpTypeLuck      int16 = 3
	RpTypeDedicated int16 = 4
)

// querier is the pgx surface shared by the pool and transaction DAO variants.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DAO is the wallet data access object. The pool variant runs each statement
// on a pooled connection; the tx variant (NewTx) runs inside one transaction.
type DAO struct {
	q    querier
	pool *pgxpool.Pool
}

// Account mirrors wallet.zim_wallet_account. Negative UIDs are system
// transitional accounts.
type Account struct {
	UID              int64
	AccountType      int16
	AvailableBalance int64
	FrozenBalance    int64
	PayPasswordHash  string
	TOTPSecretEnc    string
	WithdrawFreezeAt int64
	Status           int16
	Version          int64
	UpdatedAt        int64
}

// Ledger mirrors wallet.zim_wallet_ledger. Amount is positive for credit and
// negative for debit; BalanceAfter/FrozenAfter snapshot the account state.
type Ledger struct {
	ID           int64
	BizOrderID   string
	UID          int64
	BizType      int16
	Amount       int64
	BalanceAfter int64
	FrozenAfter  int64
	Remark       string
	CreatedAt    int64
}

// DepositAddress mirrors wallet.zim_wallet_deposit_address.
type DepositAddress struct {
	ID            int64
	UID           int64
	Chain         string
	Address       string
	DerivationIdx int32
	CreatedAt     int64
}

// ChainDeposit mirrors wallet.zim_chain_deposit.
type ChainDeposit struct {
	ID            int64
	UID           int64
	Chain         string
	TxHash        string
	LogIndex      int32
	FromAddress   string
	ToAddress     string
	Amount        int64
	Confirmations int32
	Status        int16
	CreatedAt     int64
	CreditedAt    int64
}

// ChainWithdrawal mirrors wallet.zim_chain_withdrawal.
type ChainWithdrawal struct {
	ID           int64
	BizOrderID   string
	UID          int64
	Chain        string
	ToAddress    string
	Amount       int64
	NetworkFee   int64
	ActualAmount int64
	TxHash       string
	Status       int16
	AuditAdminID int64
	AuditRemark  string
	CreatedAt    int64
	UpdatedAt    int64
}

// RedPacket mirrors wallet.zim_red_packet (M4 红包业务预留).
type RedPacket struct {
	RPID          int64
	BizOrderID    string
	SenderUID     int64
	ConvID        string
	ConvType      int16
	RpType        int16
	TargetUID     int64
	TotalAmount   int64
	TotalCount    int32
	ClaimedAmount int64
	ClaimedCount  int32
	RefundAmount  int64
	Greeting      string
	Status        int16
	ExpireAt      int64
	CreatedAt     int64
}

// RedPacketClaim mirrors wallet.zim_red_packet_claim (M4 红包业务预留).
type RedPacketClaim struct {
	ID         int64
	RPID       int64
	ClaimerUID int64
	Amount     int64
	IsBestLuck bool
	ClaimedAt  int64
}

// TipOrder mirrors wallet.zim_tip_order (M4 打赏业务预留).
type TipOrder struct {
	TipID       int64
	BizOrderID  string
	Scene       int16
	SceneID     string
	SenderUID   int64
	ReceiverUID int64
	GiftID      int32
	GiftCount   int32
	ComboID     string
	GrossAmount int64
	PlatformFee int64
	NetAmount   int64
	Message     string
	CreatedAt   int64
}
