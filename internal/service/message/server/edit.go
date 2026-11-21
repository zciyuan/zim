package message

import (
	"context"
	"strings"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// editWindowMS is how long a sender may still edit a message.
const editWindowMS int64 = 2 * 60 * 1000

// Edit rewrites an existing message body and returns the updated message.
//
// Only the sender may edit, and only inside the edit window; the caller pushes
// the returned item to the participants so open clients replace the bubble.
func (s *Service) Edit(ctx context.Context, sender int64, serverMsgID, content string) (*model.Message, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, errs.ErrMessageEmpty
	}
	if len(content) > model.MaxMessageBytes {
		return nil, errs.ErrMessageTooLong
	}
	message, err := s.ownedMessage(ctx, sender, serverMsgID)
	if err != nil {
		return nil, err
	}
	if s.now().UnixMilli()-message.SendTime > editWindowMS {
		return nil, errs.ErrRecallTimeout
	}
	if err = s.repo.UpdateContent(ctx, serverMsgID, content); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	message.Content = content
	return message, nil
}

// Delete marks a message deleted for both sides and returns the updated message.
func (s *Service) Delete(ctx context.Context, sender int64, serverMsgID string) (*model.Message, error) {
	message, err := s.ownedMessage(ctx, sender, serverMsgID)
	if err != nil {
		return nil, err
	}
	if err = s.repo.MarkDeleted(ctx, serverMsgID); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	message.Status = model.MessageDeleted
	return message, nil
}

// Search returns conversation matches for a keyword, newest first.
func (s *Service) Search(ctx context.Context, conversationID, keyword string, limit int) ([]*model.Message, error) {
	if strings.TrimSpace(conversationID) == "" {
		return nil, errs.ErrConversationNotExist
	}
	if strings.TrimSpace(keyword) == "" {
		return []*model.Message{}, nil
	}
	messages, err := s.repo.Search(ctx, conversationID, keyword, limit)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return messages, nil
}

// ownedMessage loads a message and verifies the caller is its sender.
func (s *Service) ownedMessage(ctx context.Context, sender int64, serverMsgID string) (*model.Message, error) {
	message, err := s.repo.FindOne(ctx, serverMsgID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if message.SendID != sender {
		return nil, errs.ErrNotMessageSender
	}
	return message, nil
}

// Recipients returns the fanout list for an already stored message.
func (s *Service) Recipients(ctx context.Context, msg *model.Message) []int64 {
	if msg == nil {
		return nil
	}
	if msg.GroupID > 0 {
		membership, err := s.dir.Membership(ctx, msg.GroupID, msg.SendID)
		if err != nil || membership == nil {
			return []int64{msg.SendID}
		}
		return membership.MemberIDs
	}
	targets := []int64{msg.SendID}
	if msg.RecvID > 0 && msg.RecvID != msg.SendID {
		targets = append(targets, msg.RecvID)
	}
	return targets
}

// PublishUpdate pushes the new state of an edited/deleted message to the
// participants. Clients treat a 1901 item whose server_msg_id already exists as
// an in-place update.
func (s *Service) PublishUpdate(ctx context.Context, msg *model.Message, recipients []int64) {
	s.publish(ctx, msg, recipients)
}

// Find loads one stored message by its server_msg_id.
func (s *Service) Find(ctx context.Context, serverMsgID string) (*model.Message, error) {
	message, err := s.repo.FindOne(ctx, serverMsgID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return message, nil
}

// Access verifies uid participates in the message's conversation.
func (s *Service) Access(ctx context.Context, uid int64, message *model.Message) error {
	if message == nil {
		return errs.ErrMessageEmpty
	}
	if message.GroupID > 0 {
		membership, err := s.dir.Membership(ctx, message.GroupID, uid)
		if err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		if membership == nil {
			return errs.ErrNotGroupMember
		}
		return nil
	}
	if uid == message.SendID || uid == message.RecvID {
		return nil
	}
	return errs.ErrConversationNotExist
}

// GroupMembers returns the member ids of one group, verifying uid belongs to it.
func (s *Service) GroupMembers(ctx context.Context, groupID, uid int64) ([]int64, error) {
	membership, err := s.dir.Membership(ctx, groupID, uid)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if membership == nil {
		return nil, errs.ErrNotGroupMember
	}
	return membership.MemberIDs, nil
}
