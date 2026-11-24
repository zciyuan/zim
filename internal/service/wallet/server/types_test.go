package wallet

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// ledgerKey 是 fake 流水表的主键，镜像 (biz_order_id, uid, biz_type) 唯一索引。
type ledgerKey struct {
	bizOrderID string
	uid        int64
	bizType    int16
}

// fakeRepo 是 Repository/Tx 的内存替身。CAS + 快照回滚模拟事务语义：
// begin 先深拷贝全部表，fn 返回错误时整体还原。
type fakeRepo struct {
	mu sync.Mutex

	accounts         map[int64]*walletdao.Account
	ledgers          map[ledgerKey]*walletdao.Ledger
	depositAddresses map[string]*walletdao.DepositAddress // uid/chain
	addressOwner     map[string]*walletdao.DepositAddress // address
	chainDeposits    map[string]*walletdao.ChainDeposit   // chain|tx|log
	depositsByID     map[int64]*walletdao.ChainDeposit
	withdrawals      map[string]*walletdao.ChainWithdrawal
	redPackets       map[int64]*walletdao.RedPacket
	redPacketByBiz   map[string]*walletdao.RedPacket
	redPacketClaims  map[ledgerKey]*walletdao.RedPacketClaim
	tipOrders        map[string]*walletdao.TipOrder
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		accounts:         make(map[int64]*walletdao.Account),
		ledgers:          make(map[ledgerKey]*walletdao.Ledger),
		depositAddresses: make(map[string]*walletdao.DepositAddress),
		addressOwner:     make(map[string]*walletdao.DepositAddress),
		chainDeposits:    make(map[string]*walletdao.ChainDeposit),
		depositsByID:     make(map[int64]*walletdao.ChainDeposit),
		withdrawals:      make(map[string]*walletdao.ChainWithdrawal),
		redPackets:       make(map[int64]*walletdao.RedPacket),
		redPacketByBiz:   make(map[string]*walletdao.RedPacket),
		redPacketClaims:  make(map[ledgerKey]*walletdao.RedPacketClaim),
		tipOrders:        make(map[string]*walletdao.TipOrder),
	}
}

func (f *fakeRepo) seedAccount(uid, available, frozen int64) {
	f.accounts[uid] = &walletdao.Account{
		UID:              uid,
		AccountType:      walletdao.AccountTypeUser,
		AvailableBalance: available,
		FrozenBalance:    frozen,
		Status:           walletdao.AccountStatusNormal,
	}
}

func (f *fakeRepo) seedSystemAccounts() {
	for _, uid := range [...]int64{SysHotWallet, SysRpEscrow, SysPlatformFee, SysWithdrawFee} {
		f.accounts[uid] = &walletdao.Account{
			UID:              uid,
			AccountType:      walletdao.AccountTypeSystem,
			AvailableBalance: 0,
			FrozenBalance:    0,
			Status:           walletdao.AccountStatusNormal,
		}
	}
}

// begin 以快照-回滚模拟事务：fn 返回错误时还原全部表状态。
func (f *fakeRepo) begin(_ context.Context, fn func(tx Tx) error) error {
	f.mu.Lock()
	snapshot := f.snapshotLocked()
	f.mu.Unlock()

	if err := fn(f); err != nil {
		f.mu.Lock()
		f.restoreLocked(snapshot)
		f.mu.Unlock()
		return err
	}
	return nil
}

type fakeSnapshot struct {
	accounts         map[int64]*walletdao.Account
	ledgers          map[ledgerKey]*walletdao.Ledger
	depositAddresses map[string]*walletdao.DepositAddress
	addressOwner     map[string]*walletdao.DepositAddress
	chainDeposits    map[string]*walletdao.ChainDeposit
	depositsByID     map[int64]*walletdao.ChainDeposit
	withdrawals      map[string]*walletdao.ChainWithdrawal
	redPackets       map[int64]*walletdao.RedPacket
	redPacketByBiz   map[string]*walletdao.RedPacket
	redPacketClaims  map[ledgerKey]*walletdao.RedPacketClaim
	tipOrders        map[string]*walletdao.TipOrder
}

func (f *fakeRepo) snapshotLocked() *fakeSnapshot {
	snapshot := &fakeSnapshot{
		accounts:         make(map[int64]*walletdao.Account, len(f.accounts)),
		ledgers:          make(map[ledgerKey]*walletdao.Ledger, len(f.ledgers)),
		depositAddresses: make(map[string]*walletdao.DepositAddress, len(f.depositAddresses)),
		addressOwner:     make(map[string]*walletdao.DepositAddress, len(f.addressOwner)),
		chainDeposits:    make(map[string]*walletdao.ChainDeposit, len(f.chainDeposits)),
		depositsByID:     make(map[int64]*walletdao.ChainDeposit, len(f.depositsByID)),
		withdrawals:      make(map[string]*walletdao.ChainWithdrawal, len(f.withdrawals)),
		redPackets:       make(map[int64]*walletdao.RedPacket, len(f.redPackets)),
		redPacketByBiz:   make(map[string]*walletdao.RedPacket, len(f.redPacketByBiz)),
		redPacketClaims:  make(map[ledgerKey]*walletdao.RedPacketClaim, len(f.redPacketClaims)),
		tipOrders:        make(map[string]*walletdao.TipOrder, len(f.tipOrders)),
	}
	for k, v := range f.accounts {
		cp := *v
		snapshot.accounts[k] = &cp
	}
	for k, v := range f.ledgers {
		cp := *v
		snapshot.ledgers[k] = &cp
	}
	for k, v := range f.depositAddresses {
		cp := *v
		snapshot.depositAddresses[k] = &cp
	}
	for k, v := range f.addressOwner {
		cp := *v
		snapshot.addressOwner[k] = &cp
	}
	for k, v := range f.chainDeposits {
		cp := *v
		snapshot.chainDeposits[k] = &cp
	}
	for k, v := range f.depositsByID {
		cp := *v
		snapshot.depositsByID[k] = &cp
	}
	for k, v := range f.withdrawals {
		cp := *v
		snapshot.withdrawals[k] = &cp
	}
	for k, v := range f.redPackets {
		cp := *v
		snapshot.redPackets[k] = &cp
	}
	for k, v := range f.redPacketByBiz {
		cp := *v
		snapshot.redPacketByBiz[k] = &cp
	}
	for k, v := range f.redPacketClaims {
		cp := *v
		snapshot.redPacketClaims[k] = &cp
	}
	for k, v := range f.tipOrders {
		cp := *v
		snapshot.tipOrders[k] = &cp
	}
	return snapshot
}

func (f *fakeRepo) restoreLocked(snapshot *fakeSnapshot) {
	f.accounts = snapshot.accounts
	f.ledgers = snapshot.ledgers
	f.depositAddresses = snapshot.depositAddresses
	f.addressOwner = snapshot.addressOwner
	f.chainDeposits = snapshot.chainDeposits
	f.depositsByID = snapshot.depositsByID
	f.withdrawals = snapshot.withdrawals
	f.redPackets = snapshot.redPackets
	f.redPacketByBiz = snapshot.redPacketByBiz
	f.redPacketClaims = snapshot.redPacketClaims
	f.tipOrders = snapshot.tipOrders
}

func (f *fakeRepo) CreateAccount(_ context.Context, uid int64, accountType int16, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.accounts[uid]; ok {
		return nil
	}
	f.accounts[uid] = &walletdao.Account{
		UID: uid, AccountType: accountType, Status: walletdao.AccountStatusNormal, UpdatedAt: now,
	}
	return nil
}

func (f *fakeRepo) GetAccount(_ context.Context, uid int64) (*walletdao.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.accounts[uid]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *acc
	return &cp, nil
}

func (f *fakeRepo) GetAccountForUpdate(_ context.Context, uid int64) (*walletdao.Account, error) {
	return f.GetAccount(context.Background(), uid)
}

func (f *fakeRepo) UpdateBalances(_ context.Context, uid int64, deltaAvailable, deltaFrozen int64, expectVersion, now int64) (*walletdao.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.accounts[uid]
	if !ok || acc.Version != expectVersion {
		return nil, pgx.ErrNoRows
	}
	if acc.AvailableBalance+deltaAvailable < 0 || acc.FrozenBalance+deltaFrozen < 0 {
		return nil, pgx.ErrNoRows
	}
	acc.AvailableBalance += deltaAvailable
	acc.FrozenBalance += deltaFrozen
	acc.Version++
	acc.UpdatedAt = now
	cp := *acc
	return &cp, nil
}

func (f *fakeRepo) SetAccountStatus(_ context.Context, uid int64, status int16, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.accounts[uid]
	if !ok {
		return pgx.ErrNoRows
	}
	acc.Status = status
	acc.Version++
	acc.UpdatedAt = now
	return nil
}

func (f *fakeRepo) SetPayPasswordHash(_ context.Context, uid int64, hash string, freezeAt, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.accounts[uid]
	if !ok {
		return pgx.ErrNoRows
	}
	acc.PayPasswordHash = hash
	acc.WithdrawFreezeAt = freezeAt
	acc.Version++
	acc.UpdatedAt = now
	return nil
}

func (f *fakeRepo) SetTOTPSecret(_ context.Context, uid int64, secret string, freezeAt, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	acc, ok := f.accounts[uid]
	if !ok {
		return pgx.ErrNoRows
	}
	acc.TOTPSecretEnc = secret
	acc.WithdrawFreezeAt = freezeAt
	acc.Version++
	acc.UpdatedAt = now
	return nil
}

func (f *fakeRepo) InsertLedger(_ context.Context, l *walletdao.Ledger) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := ledgerKey{bizOrderID: l.BizOrderID, uid: l.UID, bizType: l.BizType}
	if _, ok := f.ledgers[key]; ok {
		return false, nil
	}
	cp := *l
	f.ledgers[key] = &cp
	return true, nil
}

func (f *fakeRepo) GetLedger(_ context.Context, bizOrderID string, uid int64, bizType int16) (*walletdao.Ledger, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.ledgers[ledgerKey{bizOrderID: bizOrderID, uid: uid, bizType: bizType}]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *l
	return &cp, nil
}

func (f *fakeRepo) LedgerExists(_ context.Context, bizOrderID string, uid int64, bizType int16) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.ledgers[ledgerKey{bizOrderID: bizOrderID, uid: uid, bizType: bizType}]
	return ok, nil
}

func (f *fakeRepo) SumLedger(_ context.Context, bizType int16) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum int64
	for _, l := range f.ledgers {
		if l.BizType == bizType {
			sum += l.Amount
		}
	}
	return sum, nil
}

func (f *fakeRepo) SumBalances(_ context.Context, system bool) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum int64
	for uid, acc := range f.accounts {
		if system && uid < 0 {
			sum += acc.AvailableBalance + acc.FrozenBalance
		}
		if !system && uid > 0 {
			sum += acc.AvailableBalance + acc.FrozenBalance
		}
	}
	return sum, nil
}

func (f *fakeRepo) GetDepositAddress(_ context.Context, uid int64, chain string) (*walletdao.DepositAddress, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.depositAddresses[addrKey(uid, chain)]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *a
	return &cp, nil
}

func (f *fakeRepo) GetDepositAddressByAddress(_ context.Context, address string) (*walletdao.DepositAddress, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.addressOwner[address]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *a
	return &cp, nil
}

func (f *fakeRepo) InsertDepositAddress(_ context.Context, a *walletdao.DepositAddress) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.depositAddresses[addrKey(a.UID, a.Chain)]; ok {
		return false, nil
	}
	if _, ok := f.addressOwner[a.Address]; ok {
		return false, nil
	}
	cp := *a
	f.depositAddresses[addrKey(a.UID, a.Chain)] = &cp
	f.addressOwner[a.Address] = &cp
	return true, nil
}

func (f *fakeRepo) NextDerivationIndex(_ context.Context) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int32 = -1
	for _, a := range f.depositAddresses {
		if a.DerivationIdx > max {
			max = a.DerivationIdx
		}
	}
	return max + 1, nil
}

func (f *fakeRepo) HasDepositAddress(_ context.Context, address string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.addressOwner[address]
	return ok, nil
}

func (f *fakeRepo) UpsertChainDeposit(_ context.Context, d *walletdao.ChainDeposit) (*walletdao.ChainDeposit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := depKey(d.Chain, d.TxHash, d.LogIndex)
	existing, ok := f.chainDeposits[key]
	if !ok {
		cp := *d
		f.chainDeposits[key] = &cp
		f.depositsByID[d.ID] = &cp
		out := cp
		return &out, nil
	}
	if d.Confirmations > existing.Confirmations {
		existing.Confirmations = d.Confirmations
	}
	out := *existing
	return &out, nil
}

func (f *fakeRepo) GetChainDeposit(_ context.Context, id int64) (*walletdao.ChainDeposit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.depositsByID[id]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *d
	return &cp, nil
}

func (f *fakeRepo) MarkDepositStatus(_ context.Context, id int64, fromStatus, toStatus int16, now int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.depositsByID[id]
	if !ok || d.Status != fromStatus {
		return false, nil
	}
	d.Status = toStatus
	d.CreditedAt = now
	return true, nil
}

func (f *fakeRepo) CreateWithdrawal(_ context.Context, w *walletdao.ChainWithdrawal) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.withdrawals[w.BizOrderID]; ok {
		return false, nil
	}
	cp := *w
	f.withdrawals[w.BizOrderID] = &cp
	return true, nil
}

func (f *fakeRepo) GetWithdrawal(_ context.Context, bizOrderID string) (*walletdao.ChainWithdrawal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.withdrawals[bizOrderID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *w
	return &cp, nil
}

func (f *fakeRepo) GetWithdrawalForUpdate(_ context.Context, bizOrderID string) (*walletdao.ChainWithdrawal, error) {
	return f.GetWithdrawal(context.Background(), bizOrderID)
}

func (f *fakeRepo) UpdateWithdrawal(_ context.Context, bizOrderID string, fromStatus, toStatus int16, txHash string, auditAdminID int64, auditRemark string, now int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.withdrawals[bizOrderID]
	if !ok || w.Status != fromStatus {
		return false, nil
	}
	w.Status = toStatus
	if txHash != "" {
		w.TxHash = txHash
	}
	w.AuditAdminID = auditAdminID
	w.AuditRemark = auditRemark
	w.UpdatedAt = now
	return true, nil
}

func (f *fakeRepo) CreateRedPacket(_ context.Context, rp *walletdao.RedPacket) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.redPacketByBiz[rp.BizOrderID]; ok {
		return false, nil
	}
	cp := *rp
	f.redPackets[rp.RPID] = &cp
	f.redPacketByBiz[rp.BizOrderID] = &cp
	return true, nil
}

func (f *fakeRepo) GetRedPacket(_ context.Context, rpID int64) (*walletdao.RedPacket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rp, ok := f.redPackets[rpID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *rp
	return &cp, nil
}

func (f *fakeRepo) GetRedPacketForUpdate(ctx context.Context, rpID int64) (*walletdao.RedPacket, error) {
	return f.GetRedPacket(ctx, rpID)
}

func (f *fakeRepo) GetRedPacketByBiz(_ context.Context, bizOrderID string) (*walletdao.RedPacket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rp, ok := f.redPacketByBiz[bizOrderID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *rp
	return &cp, nil
}

func (f *fakeRepo) ListExpiredActive(_ context.Context, now int64, limit int) ([]*walletdao.RedPacket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*walletdao.RedPacket, 0, limit)
	for _, rp := range f.redPackets {
		if rp.Status == walletdao.RpStatusActive && rp.ExpireAt <= now {
			cp := *rp
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (f *fakeRepo) UpdateRedPacketClaimed(_ context.Context, rpID, amount int64, depletedStatus int16) (*walletdao.RedPacket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rp, ok := f.redPackets[rpID]
	if !ok || rp.Status != walletdao.RpStatusActive {
		return nil, pgx.ErrNoRows
	}
	rp.ClaimedAmount += amount
	rp.ClaimedCount++
	if int64(rp.ClaimedCount) >= int64(rp.TotalCount) {
		rp.Status = depletedStatus
	}
	cp := *rp
	return &cp, nil
}

func (f *fakeRepo) UpdateRedPacketStatus(_ context.Context, rpID int64, fromStatus, toStatus int16, refundAmount int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rp, ok := f.redPackets[rpID]
	if !ok || rp.Status != fromStatus {
		return false, nil
	}
	rp.Status = toStatus
	rp.RefundAmount = refundAmount
	return true, nil
}

func (f *fakeRepo) CreateRedPacketClaim(_ context.Context, c *walletdao.RedPacketClaim) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := ledgerKey{bizOrderID: "", uid: c.ClaimerUID, bizType: int16(c.RPID)}
	if _, ok := f.redPacketClaims[key]; ok {
		return false, nil
	}
	cp := *c
	f.redPacketClaims[key] = &cp
	return true, nil
}

func (f *fakeRepo) GetRedPacketClaim(_ context.Context, rpID, claimerUID int64) (*walletdao.RedPacketClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.redPacketClaims[ledgerKey{uid: claimerUID, bizType: int16(rpID)}]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *c
	return &cp, nil
}

func (f *fakeRepo) ListRedPacketClaims(_ context.Context, rpID int64, limit int) ([]*walletdao.RedPacketClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*walletdao.RedPacketClaim, 0, limit)
	for _, c := range f.redPacketClaims {
		if c.RPID == rpID {
			cp := *c
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (f *fakeRepo) MarkBestLuck(_ context.Context, rpID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var best *walletdao.RedPacketClaim
	for _, c := range f.redPacketClaims {
		if c.RPID != rpID {
			continue
		}
		if best == nil || c.Amount > best.Amount ||
			(c.Amount == best.Amount && c.ClaimedAt < best.ClaimedAt) {
			best = c
		}
	}
	if best != nil {
		best.IsBestLuck = true
	}
	return nil
}

func (f *fakeRepo) CreateTipOrder(_ context.Context, t *walletdao.TipOrder) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.tipOrders[t.BizOrderID]; ok {
		return false, nil
	}
	cp := *t
	f.tipOrders[t.BizOrderID] = &cp
	return true, nil
}

func (f *fakeRepo) GetTipOrder(_ context.Context, bizOrderID string) (*walletdao.TipOrder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tipOrders[bizOrderID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *t
	return &cp, nil
}

func addrKey(uid int64, chain string) string {
	return chain + "|" + strconv.FormatInt(uid, 10)
}

func depKey(chain, txHash string, logIndex int32) string {
	return chain + "|" + txHash + "|" + strconv.FormatInt(int64(logIndex), 10)
}

// fakeSigner 是可编程的 ChainSigner 替身。
type fakeSigner struct {
	mu             sync.Mutex
	broadcastErr   error
	broadcastCount int
	confirmations  int32
}

func (f *fakeSigner) DeriveAddress(_ context.Context, chain string, derivationIdx int32) (string, error) {
	return DummySigner{}.DeriveAddress(context.Background(), chain, derivationIdx)
}

func (f *fakeSigner) Broadcast(_ context.Context, _, _ string, _ int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.broadcastErr != nil {
		return "", f.broadcastErr
	}
	f.broadcastCount++
	return "mocktx-test-" + strconv.Itoa(f.broadcastCount), nil
}

func (f *fakeSigner) GetConfirmations(_ context.Context, _, _ string) (int32, error) {
	return f.confirmations, nil
}

// testClock 是可注入时钟：固定起点，手动推进，禁止 time.Sleep。
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock(t time.Time) *testClock {
	return &testClock{t: t}
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type metricCall struct {
	name   string
	result string
}

type recordingMetric struct {
	mu    sync.Mutex
	calls []metricCall
}

func (m *recordingMetric) record(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, metricCall{name: name, result: labels[labelResult]})
}

func (m *recordingMetric) results(name string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]string, 0, len(m.calls))
	for _, call := range m.calls {
		if call.name == name {
			results = append(results, call.result)
		}
	}
	sort.Strings(results)
	return results
}

// newTestService 组装测试用 Service：fake repo + 可编程 signer + 固定时钟。
func newTestService(f *fakeRepo) (*Service, *testClock, *fakeSigner, *recordingMetric) {
	// 时钟必须在雪花纪元（2024-01-01）之后，否则 Next() 产出负 ID。
	clock := newTestClock(time.Unix(1_750_000_000, 0).UTC())
	signer := &fakeSigner{confirmations: 999}
	metric := &recordingMetric{}
	s := newService(nil, f, f.begin, Config{
		MinWithdraw:      1_000_000,
		MaxSmallWithdraw: 500_000_000,
		FreezeWindowMs:   86400000,
		FeeBps:           500,
		Confirmations: map[string]int32{
			walletdao.ChainTRC20: 20,
			walletdao.ChainERC20: 12,
			walletdao.ChainBEP20: 15,
		},
	}, signer)
	s.now = clock.now
	s.ids = snowflake.NewWithClock(1, clock.now)
	s.metric = metric.record
	return s, clock, signer, metric
}
