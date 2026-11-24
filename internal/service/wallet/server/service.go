package wallet

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/etc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

const (
	metricWalletTotal    = "zim_wallet_total"
	metricReconcileTotal = "zim_wallet_reconcile_total"
	labelResult          = "result"
)

var _ pb.WalletServer = (*Service)(nil)

// NewServer returns the wallet mesh service backed by pool. The transaction
// boundary stays here: every money movement runs inside pgx.BeginFunc and
// reuses the tx-scoped DAO (zim-business-style §五).
func NewServer(proxy *mesh.Proxy, pool *pgxpool.Pool, cfg Config, signer ChainSigner) *Service {
	dao := walletdao.New(pool)
	begin := func(ctx context.Context, fn func(tx Tx) error) error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return fn(walletdao.NewTx(tx))
		})
	}
	return newService(proxy, dao, begin, cfg, signer)
}

func newService(proxy *mesh.Proxy, repo Repository, begin beginTxFunc, cfg Config, signer ChainSigner) *Service {
	if cfg.MinWithdraw <= 0 {
		cfg.MinWithdraw = dustThreshold
	}
	if cfg.FreezeWindowMs <= 0 {
		cfg.FreezeWindowMs = 86400000
	}
	if cfg.FeeBps < 0 {
		cfg.FeeBps = 0
	}
	if cfg.FeeBps > 9000 {
		cfg.FeeBps = 9000
	}
	if cfg.Confirmations == nil {
		cfg.Confirmations = make(map[string]int32)
	}
	if signer == nil {
		signer = DummySigner{}
	}
	return &Service{
		proxy:  proxy,
		repo:   repo,
		begin:  begin,
		cfg:    cfg,
		signer: signer,
		ids:    snowflake.New(),
		now:    time.Now,
		metric: func(name string, labels map[string]string) { metrics.Default.Inc(name, labels) },
	}
}

// Init registers the wallet service provider under the discovery name wallet.
func (s *Service) Init() {
	s.proxy.AddServiceProvider(define.ServiceNameWallet, &pb.Wallet_ServiceDesc, s)
}

// SetRedis wires the Redis client used by the red-packet claim Lua (M4).
// Without it, SendRedPacket/ClaimRedPacket degrade to an error instead of
// silently dropping money.
func (s *Service) SetRedis(client redis.UniversalClient) {
	s.redis = client
}

// SetEvents wires the Kafka side-effect publisher (rpClaim/rpRefund/
// tipSettled, ADR-0043 §1.2.C). Nil disables events.
func (s *Service) SetEvents(publisher EventPublisher) {
	s.events = publisher
}

// EnsureSystemAccounts creates the four system transitional accounts
// idempotently. It must run before the mesh starts serving money movement.
func (s *Service) EnsureSystemAccounts(ctx context.Context) error {
	now := s.now().UnixMilli()
	for _, uid := range [...]int64{SysHotWallet, SysRpEscrow, SysPlatformFee, SysWithdrawFee} {
		if err := s.repo.CreateAccount(ctx, uid, walletdao.AccountTypeSystem, now); err != nil {
			return errs.ErrInternal.WithCause(err)
		}
	}
	return nil
}

// ConfigFromEtc loads the wallet configuration from etc.zim.wallet.*.
func ConfigFromEtc() Config {
	return Config{
		MinWithdraw:      etc.Get("etc.zim.wallet.minWithdraw", int64(1_000_000)).Int64(),
		MaxSmallWithdraw: etc.Get("etc.zim.wallet.maxSmallWithdraw", int64(500_000_000)).Int64(),
		FreezeWindowMs:   etc.Get("etc.zim.wallet.freezeWindowMs", int64(86400000)).Int64(),
		FeeBps:           etc.Get("etc.zim.wallet.feeBps", int64(500)).Int64(),
		Confirmations: map[string]int32{
			walletdao.ChainTRC20: int32(etc.Get("etc.zim.wallet.confirmations.trc20", defaultConfirmTRC20).Int()),
			walletdao.ChainERC20: int32(etc.Get("etc.zim.wallet.confirmations.erc20", defaultConfirmERC20).Int()),
			walletdao.ChainBEP20: int32(etc.Get("etc.zim.wallet.confirmations.bep20", defaultConfirmBEP20).Int()),
		},
	}
}

// count records the ok/fail counter for one public path.
func (s *Service) count(err error) {
	if err != nil {
		s.metric(metricWalletTotal, map[string]string{labelResult: "fail"})
		return
	}
	s.metric(metricWalletTotal, map[string]string{labelResult: "ok"})
}

// confirmationsFor returns the deposit confirmation threshold for one chain.
func (s *Service) confirmationsFor(chain string) int32 {
	switch chain {
	case walletdao.ChainTRC20:
		if v, ok := s.cfg.Confirmations[walletdao.ChainTRC20]; ok {
			return v
		}
		return defaultConfirmTRC20
	case walletdao.ChainERC20:
		if v, ok := s.cfg.Confirmations[walletdao.ChainERC20]; ok {
			return v
		}
		return defaultConfirmERC20
	case walletdao.ChainBEP20:
		if v, ok := s.cfg.Confirmations[walletdao.ChainBEP20]; ok {
			return v
		}
		return defaultConfirmBEP20
	default:
		return 0
	}
}

func isChainSupported(chain string) bool {
	switch chain {
	case walletdao.ChainTRC20, walletdao.ChainERC20, walletdao.ChainBEP20:
		return true
	default:
		return false
	}
}
