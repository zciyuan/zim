package logic

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/etc"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/model"
	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/internal/utils/conv"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// typing handles op:1517: forward "typing" to the other participants.
//
// The state is ephemeral and never stored; clients expire it locally.
func (c *core) typing(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpTyping, nil, errs.ErrUnauthorized)
			return
		}
		if c.typingLimiter != nil && !c.typingLimiter.Allow(strconv.FormatInt(uid, 10)) {
			c.respond(ctx, contract.OpTyping, nil, errs.ErrRateLimited)
			return
		}
		req := &msgsvc.TypingReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetConversationId() == "" {
			c.respond(ctx, contract.OpTyping, nil, errs.ErrMessageEmpty)
			return
		}
		targets, err := c.typingTargets(ctx.Context(), uid, req.GetConversationId())
		if err != nil {
			c.respond(ctx, contract.OpTyping, nil, err)
			return
		}
		c.pushTyping(ctx.Context(), targets, req.GetConversationId(), uid, req.GetTyping())
		payload, _ := proto.Marshal(&msgsvc.OperationRes{Ok: true})
		c.respond(ctx, contract.OpTyping, payload, nil)
	})
}

// typingTargets resolves who should receive the typing frame.
//
// A brand-new conversation has no row until the first message is stored, so a
// missing row falls back to the canonical conversation ID: direct chats carry
// both uids, groups are still checked through the membership service.
func (c *core) typingTargets(ctx context.Context, uid int64, conversationID string) ([]int64, error) {
	conversation, err := c.conv.Get(ctx, uid, conversationID)
	if err == nil {
		if conversation.Type == model.ConversationGroup {
			return c.groupTypingTargets(ctx, conversation.GroupID, uid)
		}
		return []int64{conversation.PeerID}, nil
	}
	if a, b, ok := conv.ParseSingle(conversationID); ok {
		switch uid {
		case a:
			return []int64{b}, nil
		case b:
			return []int64{a}, nil
		default:
			return nil, errs.ErrConversationNotExist.WithCause(err)
		}
	}
	if gid, ok := conv.ParseGroup(conversationID); ok {
		return c.groupTypingTargets(ctx, gid, uid)
	}
	return nil, errs.ErrConversationNotExist.WithCause(err)
}

// groupTypingTargets returns the other members of a group.
func (c *core) groupTypingTargets(ctx context.Context, gid, uid int64) ([]int64, error) {
	members, err := c.msg.GroupMembers(ctx, gid, uid)
	if err != nil {
		return nil, errs.ErrNotGroupMember
	}
	// 大群不扇出 typing：每事件全量成员 + 设备解析成本过高。
	if max := etc.Get("etc.zim.msg.typingMaxGroup", 200).Int(); max > 0 && len(members) > max {
		return nil, nil
	}
	targets := make([]int64, 0, len(members))
	for _, member := range members {
		if member != uid {
			targets = append(targets, member)
		}
	}
	return targets, nil
}

// pushTyping fans the typing frame out to the given users.
func (c *core) pushTyping(ctx context.Context, targets []int64, conversationID string, uid int64, typing bool) {
	if len(targets) == 0 {
		return
	}
	payload, err := proto.Marshal(&msgsvc.TypingPush{
		ConversationId: conversationID, Uid: uid, Typing: typing,
	})
	if err != nil {
		return
	}
	if keys := c.deviceKeysBatch(ctx, targets); len(keys) > 0 {
		c.pushDeviceKeys(ctx, keys, contract.OpPushTyping, payload, false)
	}
}
