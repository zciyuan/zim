package wallet

import (
	"context"
	"strconv"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// SweepResult reports one maintenance run.
type SweepResult struct {
	Refunded int
}

// Sweeper runs the periodic wallet jobs that need a single leader:
// expired red-packet refunds and the red-packet claim compensation
// (Redis claimed vs DB claims, ADR-0043 G-06).
type Sweeper struct {
	service *Service
	repo    Repository
	redis   redis.UniversalClient
	now     func() time.Time
}

// NewSweeper returns the wallet maintenance sweeper.
func NewSweeper(service *Service) *Sweeper {
	return &Sweeper{service: service, repo: service.repo, redis: service.redis, now: service.now}
}

// RefundExpired refunds up to limit expired ACTIVE red packets to their
// senders (ADR-0043 §4.4.4, G-07).
func (s *Sweeper) RefundExpired(ctx context.Context, limit int) (int, error) {
	return s.service.doRefundExpiredRedPackets(ctx, limit)
}

// CompensateClaims re-drives DB crediting for Redis claims that never reached
// the database: every minute the job compares zim:rp:claimed with
// red_packet_claims and re-credits missing rows (ADR-0043 G-06).
//
// The Redis claimed hash key is built from the packet id, so the job scans
// ACTIVE packets with expire_at in the near past/future; for each, the hash
// members are claimers and the values their amounts. A missing DB claim is
// re-credited via the same idempotent transaction doClaimRedPacket uses.
func (s *Sweeper) CompensateClaims(ctx context.Context, limit int) (int, error) {
	if s.redis == nil || limit <= 0 {
		return 0, nil
	}
	// Scan ACTIVE packets whose window may still hold Redis state.
	packets, err := s.repo.ListExpiredActive(ctx, s.now().UnixMilli()+redPacketTTL, limit)
	if err != nil {
		return 0, err
	}
	compensated := 0
	for _, rp := range packets {
		claimed, err := s.redis.HGetAll(ctx, redPacketClaimedKey(rp.RPID)).Result()
		if err != nil {
			log.Warnf("red packet compensation read failed: rp_id = %d, err = %v", rp.RPID, err)
			continue
		}
		for claimerStr, amountStr := range claimed {
			claimerUID, parseErr := strconv.ParseInt(claimerStr, 10, 64)
			if parseErr != nil || claimerUID <= 0 {
				continue
			}
			if _, getErr := s.repo.GetRedPacketClaim(ctx, rp.RPID, claimerUID); getErr == nil {
				continue // already persisted
			}
			amount, parseErr := strconv.ParseInt(amountStr, 10, 64)
			if parseErr != nil || amount <= 0 {
				continue
			}
			if err := s.creditClaim(ctx, rp, claimerUID, amount); err != nil {
				log.Warnf("red packet claim compensation failed: rp_id = %d, uid = %d, err = %v",
					rp.RPID, claimerUID, err)
				continue
			}
			compensated++
		}
	}
	return compensated, nil
}

// creditClaim replays one claim credit inside the idempotent transaction.
func (s *Sweeper) creditClaim(ctx context.Context, rp *walletdao.RedPacket, claimerUID, amount int64) error {
	now := s.now().UnixMilli()
	claim := &walletdao.RedPacketClaim{
		ID: s.service.ids.Next(), RPID: rp.RPID, ClaimerUID: claimerUID,
		Amount: amount, ClaimedAt: now,
	}
	err := s.service.begin(ctx, func(tx Tx) error {
		inserted, err := tx.CreateRedPacketClaim(ctx, claim)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if !inserted {
			return errLedgerDuplicate
		}
		if _, replayed, err := s.service.debitLocked(ctx, tx, now, debitArgs{
			BizOrderID: claimBizID(rp.RPID, claimerUID), UID: SysRpEscrow,
			BizType: walletdao.BizTypeRpClaim, Amount: amount,
			Remark: "red packet claim compensation",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		if _, replayed, err := s.service.creditLocked(ctx, tx, now, creditArgs{
			BizOrderID: claimBizID(rp.RPID, claimerUID), UID: claimerUID,
			BizType: walletdao.BizTypeRpClaim, Amount: amount,
			Remark: "red packet claim compensation",
		}); err != nil {
			return err
		} else if replayed {
			return errLedgerDuplicate
		}
		if _, err := tx.UpdateRedPacketClaimed(ctx, rp.RPID, amount, int16(walletdao.RpStatusDepleted)); err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		return nil
	})
	if err != nil && !dueerrors.Is(err, errLedgerDuplicate) {
		return err
	}
	return nil
}
