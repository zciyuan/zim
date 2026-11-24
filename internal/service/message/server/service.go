// Package message implements message send, pull and recall use cases.
package message

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/conv"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/idem"
)

// Constructor errors.
var (
	ErrRepositoryMissing   = errors.New("message repository missing")
	ErrSequenceMissing     = errors.New("message sequence missing")
	ErrIdempotencyMissing  = errors.New("message idempotency missing")
	ErrConversationMissing = errors.New("message conversation store missing")
	ErrDirectoryMissing    = errors.New("message directory missing")
)

// New returns a message service.
func New(repo Repository, seq Sequence, idemStore Idempotency,
	convStore ConversationStore, dir Directory) (*Service, error) {
	if repo == nil {
		return nil, ErrRepositoryMissing
	}
	if seq == nil {
		return nil, ErrSequenceMissing
	}
	if idemStore == nil {
		return nil, ErrIdempotencyMissing
	}
	if convStore == nil {
		return nil, ErrConversationMissing
	}
	if dir == nil {
		return nil, ErrDirectoryMissing
	}
	return &Service{
		repo: repo, seq: seq, idem: idemStore, conv: convStore, dir: dir,
		ids: snowflake.New(), now: time.Now,
	}, nil
}

// PrepareSingle validates a direct message and returns its routing data.
func (s *Service) PrepareSingle(ctx context.Context, sender int64, req *SendRequest) (*Prepared, error) {
	if err := validate(sender, req); err != nil {
		return nil, err
	}
	if err := validateBurn(req); err != nil {
		return nil, err
	}
	if req.ReceiverID <= 0 || req.ReceiverID == sender {
		return nil, errs.ErrMessageEmpty
	}
	_, black, err := s.dir.Relation(ctx, sender, req.ReceiverID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if black {
		return nil, errs.ErrBlockedByPeer
	}
	return &Prepared{
		ConversationID: conv.Single(sender, req.ReceiverID),
		Recipients:     []int64{sender, req.ReceiverID},
	}, nil
}

// PrepareGroup validates a group message and returns its routing data.
func (s *Service) PrepareGroup(ctx context.Context, sender int64, req *SendRequest) (*Prepared, error) {
	if err := validate(sender, req); err != nil {
		return nil, err
	}
	if err := validateBurn(req); err != nil {
		return nil, err
	}
	if req.GroupID <= 0 {
		return nil, errs.ErrGroupNotExist
	}
	membership, err := s.dir.Membership(ctx, req.GroupID, sender)
	if err != nil {
		return nil, errs.ErrNotGroupMember.WithCause(err)
	}
	if membership == nil {
		return nil, errs.ErrNotGroupMember
	}
	now := s.now().UnixMilli()
	if err := checkGroupMute(membership, req.MsgType, now); err != nil {
		return nil, err
	}
	if err := s.checkGroupSpam(ctx, sender, req.GroupID, membership); err != nil {
		return nil, err
	}
	return &Prepared{
		ConversationID: conv.Group(req.GroupID),
		Recipients:     membership.MemberIDs,
		UnreadCount:    int64(len(membership.MemberIDs)),
	}, nil
}

// checkGroupSpam enforces the per-group sliding-window rate (ADR-0043
// §3.1.4.2): over-threshold sends hit a tiered auto-mute
// (1st 5min / 2nd 1h / 3rd+ 24h) and fail with 130104.
func (s *Service) checkGroupSpam(ctx context.Context, sender, gid int64, membership *Membership) error {
	if s.policy == nil || membership == nil || !membership.AntiSpamEnable {
		return nil
	}
	window := membership.AntiSpamWindowSec
	if window <= 0 {
		window = 5
	}
	maxMsgs := membership.AntiSpamMaxMsgs
	if maxMsgs <= 0 {
		maxMsgs = 5
	}
	tier, err := s.policy.CheckSpam(ctx, gid, sender, window, maxMsgs)
	if err != nil {
		log.Warnf("group spam check failed: gid = %d, uid = %d, err = %v", gid, sender, err)
		return nil // 限流组件故障放行，避免阻塞主写路径
	}
	if tier <= 0 {
		return nil
	}
	seconds := int64(300)
	switch tier {
	case 2:
		seconds = 3600
	case 3:
		seconds = 86400
	default:
		if tier > 3 {
			seconds = 86400
		}
	}
	if muteErr := s.policy.AutoMute(ctx, gid, sender, seconds); muteErr != nil {
		log.Warnf("group spam auto mute failed: gid = %d, uid = %d, err = %v", gid, sender, muteErr)
	}
	return errs.ErrSpamMuted
}

// autoMuteSensitive mutes a group sender after a blocked sensitive word when
// the group enables sensitive_auto_mute_s (ADR-0043 §3.1.4.1).
func (s *Service) autoMuteSensitive(ctx context.Context, sender, gid int64) error {
	if s.policy == nil || gid <= 0 {
		return nil
	}
	membership, err := s.dir.Membership(ctx, gid, sender)
	if err != nil || membership == nil || membership.SensitiveAutoMuteS <= 0 {
		return nil
	}
	if muteErr := s.policy.AutoMute(ctx, gid, sender, int64(membership.SensitiveAutoMuteS)); muteErr != nil {
		log.Warnf("sensitive auto mute failed: gid = %d, uid = %d, err = %v", gid, sender, muteErr)
	}
	return nil
}

// checkGroupMute enforces the ADR-0043 mute matrix: legacy mute windows,
// mute-all / scheduled mute (owner/admin/whitelist exempt), category flags and
// new-member protection.
func checkGroupMute(membership *Membership, msgType int32, now int64) error {
	legacyMuted := membership.GroupMute > now || membership.MemberMute > now
	if legacyMuted {
		return errs.ErrMemberMuted
	}
	privileged := membership.Role >= 1 || membership.IsWhitelist // admin/owner/whitelist
	if !privileged {
		if membership.MuteAll || membership.ScheduledMuteActive {
			return errs.ErrMuteAllActive
		}
		if membership.NewMemberUntil > 0 && now < membership.NewMemberUntil {
			return errs.ErrNewMemberProtected
		}
		flags := membership.DefaultMuteFlags | membership.MemberMuteFlags
		if flags&model.MuteFlagAll != 0 {
			return errs.ErrCategoryMuted
		}
		if categoryMuted(flags, msgType) {
			return errs.ErrCategoryMuted
		}
	}
	if membership.MemberMuteUntil == -1 || membership.MemberMuteUntil > now {
		return errs.ErrMemberMuted
	}
	return nil
}

// categoryMuted maps a message type to its category mute flag (ADR-0043 §3.1.3).
func categoryMuted(flags int32, msgType int32) bool {
	switch msgType {
	case contract.MsgTypeImage, contract.MsgTypeVideo, contract.MsgTypeVoice, contract.MsgTypeFile:
		return flags&model.MuteFlagImageVideo != 0
	default:
		return false
	}
}

// SendSingle sends a direct message synchronously.
func (s *Service) SendSingle(ctx context.Context, sender int64, req *SendRequest) (*SendResult, error) {
	prepared, err := s.PrepareSingle(ctx, sender, req)
	if err != nil {
		return nil, err
	}
	return s.send(ctx, sender, prepared.ConversationID, req, prepared.Recipients, prepared.UnreadCount)
}

// SendGroup sends a group message synchronously.
func (s *Service) SendGroup(ctx context.Context, sender int64, req *SendRequest) (*SendResult, error) {
	prepared, err := s.PrepareGroup(ctx, sender, req)
	if err != nil {
		return nil, err
	}
	return s.send(ctx, sender, prepared.ConversationID, req, prepared.Recipients, prepared.UnreadCount)
}

func (s *Service) send(ctx context.Context, sender int64, conversationID string, req *SendRequest,
	owners []int64, unread int64) (*SendResult, error) {
	existing, acquired, err := s.idem.Reserve(ctx, sender, req.ClientMsgID)
	if err != nil {
		if errors.Is(err, idem.ErrPending) {
			return nil, errs.ErrServerBusy
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	if !acquired && existing != nil {
		return &SendResult{ServerMsgID: existing.ServerMsgID, Seq: existing.Seq, Duplicate: true}, nil
	}
	if !acquired {
		return nil, errs.ErrServerBusy
	}

	seq, err := s.seq.Next(ctx, conversationID)
	if err != nil {
		_ = s.idem.Release(ctx, sender, req.ClientMsgID)
		return nil, errs.ErrInternal.WithCause(err)
	}
	now := s.now().UnixMilli()
	burnTTL, burnExpireAt, retentionAt, securityFlags := applyBurn(req, now, req.GroupID > 0)
	message := &model.Message{
		ServerMsgID: strconv.FormatInt(s.ids.Next(), 10),
		ClientMsgID: req.ClientMsgID, ConversationID: conversationID, Seq: seq,
		SendID: sender, RecvID: req.ReceiverID, GroupID: req.GroupID, MsgType: req.MsgType,
		Content: req.Content, AtUserIDs: req.AtUserIDs, SendTime: now, Status: model.MessageNormal,
		BurnTTL: burnTTL, BurnExpireAt: burnExpireAt, MaxRetentionAt: retentionAt,
		SecurityFlags: securityFlags, ExtType: req.ExtType, ExtPayload: req.ExtPayload,
	}
	result, err := s.repo.Append(ctx, message)
	if err != nil {
		_ = s.idem.Release(ctx, sender, req.ClientMsgID)
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result.Duplicate {
		return &SendResult{ServerMsgID: result.ServerMsgID, Seq: result.Seq, Duplicate: true}, nil
	}

	for _, owner := range owners {
		touch := &model.Conversation{
			OwnerID: owner, ConvID: conversationID, Type: conversationType(req),
			PeerID: peerID(owner, sender, req), GroupID: req.GroupID, LastMsgID: message.ServerMsgID,
			LastMsgSeq: seq, LastMsgAt: now, LastMsgBrief: brief(req.Content),
			UpdatedAt: now,
		}
		if touchErr := s.conv.Touch(ctx, touch); touchErr != nil {
			log.Warnf("touch conversation failed: uid = %d, conv = %s, err = %v", owner, conversationID, touchErr)
		}
	}
	if err = s.idem.Complete(ctx, sender, req.ClientMsgID, &idem.Result{
		ServerMsgID: message.ServerMsgID, Seq: seq,
	}); err != nil {
		log.Warnf("complete idempotency failed: uid = %d, err = %v", sender, err)
	}
	s.notifyWebhook(message)
	s.publish(ctx, message, owners)
	return &SendResult{
		ServerMsgID: message.ServerMsgID, Seq: seq, SendTime: now,
		UnreadCount: unread, Recipients: owners, BurnExpireAt: burnExpireAt,
	}, nil
}

// GroupMemberCount returns the current member count for a group.
func (s *Service) GroupMemberCount(ctx context.Context, sender, groupID int64) (int, error) {
	membership, err := s.dir.Membership(ctx, groupID, sender)
	if err != nil {
		return 0, errs.ErrNotGroupMember.WithCause(err)
	}
	if membership == nil {
		return 0, errs.ErrNotGroupMember
	}
	return len(membership.MemberIDs), nil
}

// Pull returns messages in a seq range.
func (s *Service) Pull(ctx context.Context, conversationID string, begin, end int64,
	limit int) ([]*model.Message, bool, error) {
	if conversationID == "" || begin < 0 || end < begin {
		return nil, false, errs.ErrMessageEmpty
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	messages, err := s.repo.FindBySeq(ctx, conversationID, begin, end, limit)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	return messages, len(messages) >= limit, nil
}

// Recall revokes a message inside the 120-second window.
func (s *Service) Recall(ctx context.Context, sender int64, serverMsgID string) error {
	message, err := s.repo.FindOne(ctx, serverMsgID)
	if err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	if message.SendID != sender {
		return errs.ErrNotMessageSender
	}
	if s.now().UnixMilli()-message.SendTime > model.RetractWindowMS {
		return errs.ErrRecallTimeout
	}
	if err = s.repo.Recall(ctx, serverMsgID); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

func validate(sender int64, req *SendRequest) error {
	if sender <= 0 {
		return errs.ErrUnauthorized
	}
	if req == nil || strings.TrimSpace(req.ClientMsgID) == "" {
		return errs.ErrMessageEmpty
	}
	if req.Content == "" {
		return errs.ErrMessageEmpty
	}
	if len(req.Content) > model.MaxMessageBytes {
		return errs.ErrMessageTooLong
	}
	if req.MsgType < 1 || req.MsgType > 22 {
		return errs.ErrMessageEmpty
	}
	if req.MsgType > 7 && req.MsgType < 20 {
		return errs.ErrMessageEmpty
	}
	return nil
}

func conversationType(req *SendRequest) int16 {
	if req.GroupID > 0 {
		return model.ConversationGroup
	}
	return model.ConversationSingle
}

func peerID(owner, sender int64, req *SendRequest) int64 {
	if req.GroupID > 0 {
		return 0
	}
	if owner == sender {
		return req.ReceiverID
	}
	return sender
}

func brief(content string) string {
	content = strings.TrimSpace(content)
	runes := []rune(content)
	if len(runes) > 120 {
		return string(runes[:120])
	}
	return content
}

// WithSensitiveFilter enables content moderation.
func (s *Service) WithSensitiveFilter(filter SensitiveFilter) *Service {
	s.sensitive = filter
	return s
}

// WithGroupPolicy enables the group anti-spam window and auto-mute
// (ADR-0043 §3.1.4).
func (s *Service) WithGroupPolicy(policy GroupPolicy) *Service {
	s.policy = policy
	return s
}

func (s *Service) checkSensitive(ctx context.Context, req *SendRequest) error {
	if s.sensitive == nil || req == nil || req.Content == "" {
		return nil
	}
	blocked, err := s.sensitive.Check(ctx, req.Content)
	if err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	if blocked {
		return errs.ErrSensitiveWord
	}
	return nil
}

// WithWebhook enables message event callbacks.
func (s *Service) WithWebhook(notifier webhook.Notifier) *Service {
	s.webhook = notifier
	return s
}

func (s *Service) notifyWebhook(message *model.Message) {
	if s.webhook == nil {
		return
	}
	s.webhook.Notify(webhook.Event{
		Type: "message", ServerMsgID: message.ServerMsgID, ConversationID: message.ConversationID,
		Seq: message.Seq, SendID: message.SendID, RecvID: message.RecvID, GroupID: message.GroupID,
		MsgType: message.MsgType, SendTime: message.SendTime,
	})
}
