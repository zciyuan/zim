package logic

import (
	"context"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/etc"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/model"
	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// readReceipt 处理 op:1505。群已读回执默认关闭，但关闭只影响“多少人已读”的展示，
// 不能阻止会话 read_seq 前进，否则群消息会一直显示未读。
func (c *core) readReceipt(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpReadReceipt, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.ReadReceiptReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpReadReceipt, nil, errs.ErrMessageEmpty)
			return
		}
		conversation, err := c.conv.Get(ctx.Context(), uid, req.GetConversationId())
		if err != nil {
			c.respond(ctx, contract.OpReadReceipt, nil, err)
			return
		}
		// The conversation read watermark is authoritative for unread counts
		// on every platform. Group receipt visibility is a separate feature and
		// must never block marking the conversation read.
		seq, err := c.conv.Read(ctx.Context(), uid, req.GetConversationId(), req.GetSeq())
		if err != nil {
			c.respond(ctx, contract.OpReadReceipt, nil, err)
			return
		}
		if conversation.Type == model.ConversationGroup {
			if !etc.Get("etc.zim.msg.groupReceipt", false).Bool() {
				payload, _ := proto.Marshal(&msgsvc.ReadReceiptRes{ReadSeq: seq})
				c.respond(ctx, contract.OpReadReceipt, payload, nil)
				return
			}
			memberCount, countErr := c.msg.GroupMemberCount(ctx.Context(), uid, conversation.GroupID)
			if countErr != nil {
				c.respond(ctx, contract.OpReadReceipt, nil, countErr)
				return
			}
			if memberCount > etc.Get("etc.zim.msg.groupReceiptMax", 500).Int() {
				payload, _ := proto.Marshal(&msgsvc.ReadReceiptRes{ReadSeq: seq})
				c.respond(ctx, contract.OpReadReceipt, payload, nil)
				return
			}
			if c.groupRead != nil {
				c.groupRead.Report(ctx.Context(), conversation.GroupID, uid, seq)
			}
		}
		// Single chat: tell the peer how far this side has read (1906).
		if conversation.Type == model.ConversationSingle && conversation.PeerID > 0 {
			c.pushReadReceipt(ctx.Context(), conversation.PeerID, conversation.ConvID, uid, seq)
		}
		payload, _ := proto.Marshal(&msgsvc.ReadReceiptRes{ReadSeq: seq})
		c.respond(ctx, contract.OpReadReceipt, payload, nil)
	})
}

// pushReadReceipt pushes the caller's read watermark to the peer (1906).
func (c *core) pushReadReceipt(ctx context.Context, peerID int64, conversationID string, uid, seq int64) {
	payload, err := proto.Marshal(&msgsvc.ReadSyncPush{
		ConversationId: conversationID, Uid: uid, ReadSeq: seq,
	})
	if err != nil {
		return
	}
	c.pushDeviceKeys(ctx, c.deviceKeys(ctx, peerID), contract.OpPushReadSync, payload, false)
}
