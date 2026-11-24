package message

import (
	"context"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// allowedBurnTTLs are the countdown tiers fixed by ADR-0043 §2.1.3.
var allowedBurnTTLs = map[int32]struct{}{
	5: {}, 10: {}, 30: {}, 60: {}, 300: {}, 3600: {}, 86400: {},
}

// maxBurnRetentionMS is the default unread retention for single-chat burn
// messages: 7 days (ADR-0043 §2.1.4, G-01).
const maxBurnRetentionMS = 7 * 86400 * 1000

// validateBurn rejects burn messages that the product rules forbid
// (ADR-0043 §2.1.2): red packet / tip / voice-room cards, merged forwards and
// other custom payloads cannot burn.
func validateBurn(req *SendRequest) error {
	if req.BurnTTL <= 0 {
		return nil
	}
	if _, ok := allowedBurnTTLs[req.BurnTTL]; !ok {
		return errs.ErrBurnTTLInvalid
	}
	// Burn supports text/image/voice/video/file/location only.
	switch req.MsgType {
	case contract.MsgTypeText, contract.MsgTypeImage, contract.MsgTypeVoice,
		contract.MsgTypeVideo, contract.MsgTypeFile, contract.MsgTypeLocation:
	default:
		return errs.ErrBurnTypeNotAllowed
	}
	if req.ExtType != model.ExtTypeNone {
		return errs.ErrBurnTypeNotAllowed
	}
	return nil
}

// BurnStart is the result of a single-chat BurnReadAck: the authoritative
// countdown deadline and the parties that need the BurnTimerStart push.
type BurnStart struct {
	ConversationID string
	ServerMsgID    string
	Seq            int64
	BurnExpireAt   int64
	ServerNowMS    int64
	SenderUID      int64
	ReceiverUID    int64
}

// BurnReadAck starts the single-chat countdown when the receiver reads a burn
// message (ADR-0043 §2.1.4). Only the receiver may trigger it; group burn
// messages count down from creation and reject the ack.
func (s *Service) BurnReadAck(ctx context.Context, receiverUID int64, serverMsgID string) (*BurnStart, error) {
	msg, err := s.repo.FindOne(ctx, serverMsgID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if msg == nil || msg.BurnTTL <= 0 {
		return nil, errs.ErrBurnTypeNotAllowed
	}
	if msg.GroupID > 0 {
		// Group burn counts down from creation; ack is a no-op rejection.
		return nil, errs.ErrBurnTypeNotAllowed
	}
	if msg.RecvID != receiverUID {
		return nil, errs.ErrBurnReadDenied
	}
	if msg.IsBurned {
		return nil, errs.ErrBurnAlreadyBurned
	}
	if msg.BurnExpireAt > 0 {
		// Already counting down: idempotent re-ack returns the same deadline.
		return &BurnStart{
			ConversationID: msg.ConversationID, ServerMsgID: msg.ServerMsgID, Seq: msg.Seq,
			BurnExpireAt: msg.BurnExpireAt, ServerNowMS: s.now().UnixMilli(),
			SenderUID: msg.SendID, ReceiverUID: msg.RecvID,
		}, nil
	}
	now := s.now().UnixMilli()
	expireAt := now + int64(msg.BurnTTL)*1000
	if err = s.repo.StartBurnCountdown(ctx, serverMsgID, expireAt); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &BurnStart{
		ConversationID: msg.ConversationID, ServerMsgID: msg.ServerMsgID, Seq: msg.Seq,
		BurnExpireAt: expireAt, ServerNowMS: now, SenderUID: msg.SendID, ReceiverUID: msg.RecvID,
	}, nil
}

// BurnNow erases one burn message before/at its deadline: physical payload
// deletion plus tombstone flag. It is used by the crontask timer sweep and by
// explicit destroys (ADR-0043 §2.2).
func (s *Service) BurnNow(ctx context.Context, serverMsgID string) error {
	return s.repo.Burn(ctx, serverMsgID)
}

// ExpireCandidates returns burn messages whose countdown or unread retention
// has elapsed. The crontask sweep drives physical erase and BurnDelete pushes
// from this list (ADR-0043 §2.4.3, G-01).
func (s *Service) ExpireCandidates(ctx context.Context, now time.Time, limit int) ([]*model.Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return s.repo.FindBurnExpired(ctx, now.UnixMilli(), limit)
}

// applyBurn derives the burn metadata from the send request. Group messages
// start counting down at creation (burn_expire_at = created_at + ttl); single
// messages wait for BurnReadAck (burn_expire_at = 0) but always carry
// max_retention_at as the storage-leak backstop.
func applyBurn(req *SendRequest, now int64, group bool) (ttl int32, expireAt, retentionAt int64, flags uint32) {
	if req.BurnTTL <= 0 {
		return 0, 0, 0, 0
	}
	ttl = req.BurnTTL
	flags = model.BurnAfterReadingFlags
	retentionAt = now + maxBurnRetentionMS
	if group {
		expireAt = now + int64(ttl)*1000
	}
	return ttl, expireAt, retentionAt, flags
}
