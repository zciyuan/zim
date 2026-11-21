package logic

import (
	"context"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// addReaction handles op:1515.
func (c *core) addReaction(ctx node.Context) {
	c.mutateReaction(ctx, contract.OpAddReaction, true)
}

// removeReaction handles op:1516.
func (c *core) removeReaction(ctx node.Context) {
	c.mutateReaction(ctx, contract.OpRemoveReaction, false)
}

// mutateReaction validates the target message, then adds or removes one emoji
// for the caller. The response is OperationRes; clients update their chip row
// optimistically and re-read on the next history pull.
func (c *core) mutateReaction(ctx node.Context, opcode int32, add bool) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, opcode, nil, errs.ErrUnauthorized)
			return
		}
		if c.reactions == nil {
			c.respond(ctx, opcode, nil, errs.ErrServerBusy)
			return
		}
		conversationID, serverMsgID, emoji, err := parseReaction(ctx, add)
		if err != nil {
			c.respond(ctx, opcode, nil, err)
			return
		}
		message, err := c.msg.Find(ctx.Context(), serverMsgID)
		if err != nil {
			c.respond(ctx, opcode, nil, err)
			return
		}
		if err = c.msg.Access(ctx.Context(), uid, message); err != nil {
			c.respond(ctx, opcode, nil, err)
			return
		}
		// The stored message owns the conversation id; a client-supplied one is
		// informational only (a thin client may not have it yet).
		_ = conversationID
		if add {
			err = c.reactions.Add(ctx.Context(), message.ConversationID, message.Seq, uid, emoji)
		} else {
			err = c.reactions.Remove(ctx.Context(), message.ConversationID, message.Seq, uid, emoji)
		}
		if err != nil {
			c.respond(ctx, opcode, nil, err)
			return
		}
		payload, _ := proto.Marshal(&msgsvc.OperationRes{Ok: true})
		c.respond(ctx, opcode, payload, nil)
	})
}

// parseReaction decodes the add/remove request into its common fields.
func parseReaction(ctx node.Context, add bool) (conversationID, serverMsgID, emoji string, err error) {
	if add {
		req := &msgsvc.AddReactionReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			return "", "", "", errs.ErrMessageEmpty
		}
		return req.GetConversationId(), req.GetServerMsgId(), req.GetEmoji(), nil
	}
	req := &msgsvc.RemoveReactionReq{}
	if _, err = parseMessage(ctx, req); err != nil {
		return "", "", "", errs.ErrMessageEmpty
	}
	return req.GetConversationId(), req.GetServerMsgId(), req.GetEmoji(), nil
}

// attachReactions fills the reactions of one history/search page.
func (c *core) attachReactions(ctx context.Context, conversationID string, uid int64, items []*msgsvc.MessageItem) {
	if c.reactions == nil || len(items) == 0 {
		return
	}
	seqs := make([]int64, 0, len(items))
	for _, item := range items {
		seqs = append(seqs, item.GetSeq())
	}
	reactions, err := c.reactions.List(ctx, conversationID, seqs, uid)
	if err != nil || len(reactions) == 0 {
		return
	}
	for _, item := range items {
		for _, entry := range reactions[item.GetSeq()] {
			item.Reactions = append(item.Reactions, &msgsvc.Reaction{
				Emoji: entry.Emoji, Count: entry.Count, Mine: entry.Mine,
			})
		}
	}
}
