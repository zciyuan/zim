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
	if membership.GroupMute > now || membership.MemberMute > now {
		return nil, errs.ErrMemberMuted
	}
	return &Prepared{
		ConversationID: conv.Group(req.GroupID),
		Recipients:     membership.MemberIDs,
		UnreadCount:    int64(len(membership.MemberIDs)),
	}, nil
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
	message := &model.Message{
		ServerMsgID: strconv.FormatInt(s.ids.Next(), 10),
		ClientMsgID: req.ClientMsgID, ConversationID: conversationID, Seq: seq,
		SendID: sender, RecvID: req.ReceiverID, GroupID: req.GroupID, MsgType: req.MsgType,
		Content: req.Content, AtUserIDs: req.AtUserIDs, SendTime: now, Status: model.MessageNormal,
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
		UnreadCount: unread, Recipients: owners,
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
