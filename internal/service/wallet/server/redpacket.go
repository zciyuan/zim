package wallet

import (
	"context"
	"strconv"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// minRedPacketShare is the smallest claimable share: 0.01 USDT
// (ADR-0043 §4.4.1).
const minRedPacketShare int64 = 10_000

// redPacketTTL is the default claim window: 24 hours.
const redPacketTTL = 24 * 3600 * 1000

// doSendRedPacket validates, freezes the sender's funds into the escrow
// account, writes the packet row and arms the Redis claim queue. The
// biz_order_id keeps the whole operation idempotent: a replay returns the
// original packet without freezing twice.
func (s *Service) doSendRedPacket(ctx context.Context, a redPacketArgs) (*walletdao.RedPacket, error) {
	if a.SenderUID <= 0 || a.TotalAmount <= 0 || a.TotalCount <= 0 || a.BizOrderID == "" {
		return nil, errs.ErrRedPacketAmountInvalid
	}
	if a.TotalAmount < int64(a.TotalCount)*minRedPacketShare {
		return nil, errs.ErrRedPacketAmountInvalid
	}
	// 专属红包必须指定目标用户（ADR-0043：rp_type=4 → target_uid 必填）。
	if a.RpType == walletdao.RpTypeDedicated && a.TargetUID <= 0 {
		return nil, errs.ErrRedPacketNotTarget
	}

	existing, err := s.repo.GetRedPacketByBiz(ctx, a.BizOrderID)
	if err == nil && existing != nil {
		return existing, nil // 幂等重放
	}

	shares, err := splitRedPacket(a.TotalAmount, a.TotalCount, minRedPacketShare)
	if err != nil {
		return nil, err
	}
	now := s.now().UnixMilli()
	expireAt := now + redPacketTTL
	rp := &walletdao.RedPacket{
		RPID: s.ids.Next(), BizOrderID: a.BizOrderID, SenderUID: a.SenderUID,
		ConvID: a.ConvID, ConvType: a.ConvType, RpType: a.RpType, TargetUID: a.TargetUID,
		TotalAmount: a.TotalAmount, TotalCount: int32(a.TotalCount),
		Greeting: a.Greeting, Status: walletdao.RpStatusActive,
		ExpireAt: expireAt, CreatedAt: now,
	}

	// 1) 资金划入托管户：发送方可用余额直接出账，escrow 以可用余额托管
	//    （同一事务写两条流水：RP_SEND 负、托管正）。托管户余额守恒，
	//    领取/退款时从托管户出账。
	err = s.begin(ctx, func(tx Tx) error {
		inserted, err := tx.CreateRedPacket(ctx, rp)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !inserted {
			// 并发重放：读取已有行并整体回滚（外层走幂等重读分支）。
			return errLedgerDuplicate
		}
		// 扣减发送方可用并划入 escrow。
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: rp.BizOrderID, UID: a.SenderUID,
			BizType: walletdao.BizTypeRpSend, Amount: a.TotalAmount,
			Remark: "red packet send",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		if _, replayed, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: rp.BizOrderID, UID: SysRpEscrow,
			BizType: walletdao.BizTypeRpSend, Amount: a.TotalAmount,
			Remark: "red packet escrow",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		return nil
	})
	if dueerrors.Is(err, errLedgerDuplicate) {
		if existing, getErr := s.repo.GetRedPacketByBiz(ctx, a.BizOrderID); getErr == nil && existing != nil {
			return existing, nil
		}
	}
	if err != nil {
		return nil, err
	}

	// 2) 冻结成功后写入 Redis 抢包队列（Redis 失败只告警：Lua 抢包不可用，
	//    过期退款兜底，资金不丢）。
	if s.redis != nil {
		if redisErr := s.storeRedPacketRedis(ctx, rp, shares); redisErr != nil {
			log.Warnf("store red packet redis failed: rp_id = %d, err = %v", rp.RPID, redisErr)
		}
	}
	return rp, nil
}

// doClaimRedPacket atomically pops one share (Redis Lua) and credits it inside
// a DB transaction: escrow 解冻出账 + claimer 入账 + claim 明细 + 统计。Lua
// 成功后若 DB 事务失败，claim 明细缺失由补偿任务按 `zim:rp:claimed` 补发。
func (s *Service) doClaimRedPacket(ctx context.Context, a redPacketClaimArgs) (*walletdao.RedPacketClaim, error) {
	if a.RpID <= 0 || a.ClaimerUID <= 0 {
		return nil, errs.ErrRedPacketNotExist
	}
	if s.redis == nil {
		return nil, errs.ErrInternal.WithMessage("red packet redis unavailable")
	}
	rp, err := s.repo.GetRedPacket(ctx, a.RpID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrRedPacketNotExist
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	if rp.Status != walletdao.RpStatusActive {
		if rp.Status == walletdao.RpStatusExpiredRefunded {
			return nil, errs.ErrRedPacketExpired
		}
		return nil, errs.ErrRedPacketDepleted
	}

	targetUID := rp.TargetUID
	amount, err := claimRedPacketScript.Run(ctx, s.redis,
		[]string{redPacketMetaKey(a.RpID), redPacketQueueKey(a.RpID), redPacketClaimedKey(a.RpID)},
		s.now().UnixMilli(), a.ClaimerUID, targetUID,
	).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if amount < 0 {
		return nil, redPacketClaimErr(amount)
	}

	now := s.now().UnixMilli()
	claim := &walletdao.RedPacketClaim{
		ID: s.ids.Next(), RPID: a.RpID, ClaimerUID: a.ClaimerUID,
		Amount: amount, ClaimedAt: now,
	}
	// 入账事务：escrow 出账 → claimer 入账 + 明细 + 统计（乐观锁重试）。
	err = s.begin(ctx, func(tx Tx) error {
		inserted, err := tx.CreateRedPacketClaim(ctx, claim)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !inserted {
			// 重复抢包：回滚并返回幂等已领取。
			return errLedgerDuplicate
		}
		// 领取方账户不存在则先建户：红包入账不能丢（与充值同语义）。
		if _, err := tx.GetAccountForUpdate(ctx, a.ClaimerUID); err != nil {
			if dueerrors.Is(err, pgx.ErrNoRows) {
				if err := tx.CreateAccount(ctx, a.ClaimerUID, walletdao.AccountTypeUser, now); err != nil {
					return errs.ErrInternal.WithCause(err)
				}
			} else {
				return errs.ErrInternal.WithCause(err)
			}
		}
		if _, replayed, err := s.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: claimBizID(a.RpID, a.ClaimerUID), UID: SysRpEscrow,
			BizType: walletdao.BizTypeRpClaim, Amount: amount,
			Remark: "red packet claim",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		if _, replayed, err := s.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: claimBizID(a.RpID, a.ClaimerUID), UID: a.ClaimerUID,
			BizType: walletdao.BizTypeRpClaim, Amount: amount,
			Remark: "red packet claim credit",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		depleted := walletdao.RpStatusDepleted
		updated, err := tx.UpdateRedPacketClaimed(ctx, a.RpID, amount, int16(depleted))
		if err != nil {
			if dueerrors.Is(err, pgx.ErrNoRows) {
				return errLedgerDuplicate
			}
			return errs.ErrInternal.WithCause(err)
		}
		if updated.Status == int16(depleted) {
			if err := tx.MarkBestLuck(ctx, a.RpID); err != nil {
				return errs.ErrInternal.WithCause(err)
			}
		}
		return nil
	})
	if dueerrors.Is(err, errLedgerDuplicate) {
		return nil, errs.ErrRedPacketClaimed
	}
	if err != nil {
		return nil, err
	}
	// ADR-0043 §1.2.C: claim settled → notify the message/voiceroom plane.
	s.publishWalletEvent(func() error {
		return s.events.PublishRpClaim(ctx, a.RpID, a.ClaimerUID, amount)
	}, "rp claim")
	return claim, nil
}

// doRefundExpiredRedPackets scans expired ACTIVE packets and refunds the
// unclaimed remainder to the sender (ADR-0043 §4.4.4, G-07).
func (s *Service) doRefundExpiredRedPackets(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	expired, err := s.repo.ListExpiredActive(ctx, s.now().UnixMilli(), limit)
	if err != nil {
		return 0, errs.ErrInternal.WithCause(err)
	}
	refunded := 0
	for _, rp := range expired {
		refund := rp.TotalAmount - rp.ClaimedAmount
		err = s.begin(ctx, func(tx Tx) error {
			moved, err := tx.UpdateRedPacketStatus(ctx, rp.RPID,
				walletdao.RpStatusActive, walletdao.RpStatusExpiredRefunded, refund)
			if err != nil {
				return errs.ErrInternal.WithCause(err)
			}
			if !moved {
				return errLedgerDuplicate
			}
			biz := "rp-refund-" + strconv.FormatInt(rp.RPID, 10)
			if refund > 0 {
				if _, replayed, err := s.debitLocked(ctx, tx, s.now().UnixMilli(), debitArgs{
					BizOrderID: biz, UID: SysRpEscrow, BizType: walletdao.BizTypeRpRefund,
					Amount: refund, Remark: "red packet refund",
				}); err != nil {
					return err
				} else if replayed {
					return errLedgerDuplicate
				}
				if _, replayed, err := s.creditLocked(ctx, tx, s.now().UnixMilli(), creditArgs{
					BizOrderID: biz, UID: rp.SenderUID, BizType: walletdao.BizTypeRpRefund,
					Amount: refund, Remark: "red packet refund credit",
				}); err != nil {
					return err
				} else if replayed {
					return errLedgerDuplicate
				}
			}
			return nil
		})
		if dueerrors.Is(err, errLedgerDuplicate) {
			continue
		}
		if err != nil {
			return refunded, err
		}
		refunded++
		s.publishWalletEvent(func() error {
			return s.events.PublishRpRefund(ctx, rp.RPID, rp.SenderUID, refund)
		}, "rp refund")
		if s.redis != nil {
			_ = s.redis.Del(ctx,
				redPacketMetaKey(rp.RPID), redPacketQueueKey(rp.RPID), redPacketClaimedKey(rp.RPID)).Err()
		}
	}
	return refunded, nil
}

func claimBizID(rpID, claimerUID int64) string {
	return "rp-claim-" + strconv.FormatInt(rpID, 10) + "-" + strconv.FormatInt(claimerUID, 10)
}
