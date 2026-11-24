package logic

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/model"
	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// sendSingle handles op:1501.
func (c *core) sendSingle(ctx node.Context) {
	started := time.Now()
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSendSingle, nil, errs.ErrUnauthorized)
			return
		}
		if !c.sendLimiter.Allow(strconv.FormatInt(uid, 10)) {
			log.Warnf("send rate limited uid=%d route=1501", uid)
			metrics.Default.Inc("zim_send_limited_total", nil)
			c.respond(ctx, contract.OpSendSingle, nil, errs.ErrRateLimited)
			return
		}
		req := &msgsvc.SendSingleReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSendSingle, nil, errs.ErrMessageEmpty)
			return
		}
		result, err := c.sendSingleMessage(ctx.Context(), uid, &message.SendRequest{
			ClientMsgID: req.GetClientMsgId(), ReceiverID: req.GetRecvId(),
			MsgType: req.GetMsgType(), Content: req.GetContent(),
			BurnTTL: req.GetBurnTtl(), ExtType: req.GetExtType(), ExtPayload: req.GetExtPayload(),
		})
		if err != nil {
			logSend(uid, contract.OpSendSingle, req.GetClientMsgId(), nil, err, started)
			c.respond(ctx, contract.OpSendSingle, nil, err)
			return
		}
		logSend(uid, contract.OpSendSingle, req.GetClientMsgId(), result, nil, started)
		payload, _ := proto.Marshal(&msgsvc.SendSingleRes{
			ServerMsgId: result.ServerMsgID, Seq: result.Seq, SendTime: result.SendTime,
			BurnExpireAt: result.BurnExpireAt,
		})
		c.respond(ctx, contract.OpSendSingle, payload, nil)
	})
}

// sendGroup handles op:1502.
func (c *core) sendGroup(ctx node.Context) {
	started := time.Now()
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSendGroup, nil, errs.ErrUnauthorized)
			return
		}
		if !c.sendLimiter.Allow(strconv.FormatInt(uid, 10)) {
			log.Warnf("send rate limited uid=%d route=1502", uid)
			metrics.Default.Inc("zim_send_limited_total", nil)
			c.respond(ctx, contract.OpSendGroup, nil, errs.ErrRateLimited)
			return
		}
		req := &msgsvc.SendGroupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSendGroup, nil, errs.ErrMessageEmpty)
			return
		}
		result, err := c.sendGroupMessage(ctx.Context(), uid, &message.SendRequest{
			ClientMsgID: req.GetClientMsgId(), GroupID: req.GetGroupId(),
			MsgType: req.GetMsgType(), Content: req.GetContent(), AtUserIDs: req.GetAtUserIds(),
			BurnTTL: req.GetBurnTtl(), ExtType: req.GetExtType(), ExtPayload: req.GetExtPayload(),
		})
		if err != nil {
			logSend(uid, contract.OpSendGroup, req.GetClientMsgId(), nil, err, started)
			c.respond(ctx, contract.OpSendGroup, nil, err)
			return
		}
		logSend(uid, contract.OpSendGroup, req.GetClientMsgId(), result, nil, started)
		payload, _ := proto.Marshal(&msgsvc.SendGroupRes{
			ServerMsgId: result.ServerMsgID, Seq: result.Seq, SendTime: result.SendTime,
			UnreadCount: result.UnreadCount, BurnExpireAt: result.BurnExpireAt,
		})
		c.respond(ctx, contract.OpSendGroup, payload, nil)
	})
}

// pullBySeq handles op:1511.
func (c *core) pullBySeq(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &msgsvc.PullBySeqReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpPullBySeq, nil, errs.ErrMessageEmpty)
			return
		}
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpPullBySeq, nil, errs.ErrUnauthorized)
			return
		}
		min, max, err := c.conv.VisibleRange(ctx.Context(), uid, req.GetConversationId())
		if err != nil {
			c.respond(ctx, contract.OpPullBySeq, nil, err)
			return
		}
		begin, end := req.GetBeginSeq(), req.GetEndSeq()
		if begin < min {
			begin = min
		}
		if end <= 0 || end > max {
			end = max
		}
		if begin > end {
			payload, _ := proto.Marshal(&msgsvc.PullBySeqRes{Messages: nil, HasMore: false})
			c.respond(ctx, contract.OpPullBySeq, payload, nil)
			return
		}
		messages, hasMore, err := c.msg.Pull(ctx.Context(), req.GetConversationId(), begin, end, int(req.GetLimit()))
		if err != nil {
			c.respond(ctx, contract.OpPullBySeq, nil, err)
			return
		}
		items := make([]*msgsvc.MessageItem, 0, len(messages))
		for _, item := range messages {
			items = append(items, &msgsvc.MessageItem{
				ServerMsgId: item.ServerMsgID, ClientMsgId: item.ClientMsgID,
				ConversationId: item.ConversationID, Seq: item.Seq, SendId: item.SendID,
				RecvId: item.RecvID, GroupId: item.GroupID, MsgType: item.MsgType,
				Content: item.Content, AtUserIds: item.AtUserIDs, SendTime: item.SendTime,
				Status: item.Status,
			})
		}
		c.attachReactions(ctx.Context(), req.GetConversationId(), uid, items)
		payload, _ := proto.Marshal(&msgsvc.PullBySeqRes{Messages: items, HasMore: hasMore})
		c.respond(ctx, contract.OpPullBySeq, payload, nil)
	})
}

// recall handles op:1506.
func (c *core) recall(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpRecall, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.RecallReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpRecall, nil, errs.ErrMessageEmpty)
			return
		}
		err = c.msg.Recall(ctx.Context(), uid, req.GetServerMsgId())
		if err != nil {
			c.respond(ctx, contract.OpRecall, nil, err)
			return
		}
		payload, _ := proto.Marshal(&msgsvc.OperationRes{Ok: true})
		c.respond(ctx, contract.OpRecall, payload, nil)
	})
}

func (c *core) sendSingleMessage(ctx context.Context, uid int64, req *message.SendRequest) (*message.SendResult, error) {
	if c.pipeline != nil {
		return c.pipeline.SendSingle(ctx, uid, req)
	}
	return c.msg.SendSingle(ctx, uid, req)
}

func (c *core) sendGroupMessage(ctx context.Context, uid int64, req *message.SendRequest) (*message.SendResult, error) {
	if c.pipeline != nil {
		return c.pipeline.SendGroup(ctx, uid, req)
	}
	return c.msg.SendGroup(ctx, uid, req)
}

func parseMessage(ctx node.Context, req proto.Message) (*contract.Envelope, error) {
	envelope := &contract.Envelope{}
	if err := ctx.Parse(envelope); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(envelope.GetPayload(), req); err != nil {
		return nil, err
	}
	return envelope, nil
}

func logSend(uid int64, route int32, clientMsgID string, result *message.SendResult, err error, started time.Time) {
	latency := time.Since(started).Milliseconds()
	if err != nil {
		business := errs.From(err)
		log.Warnf("message send failed uid=%d route=%d client_msg_id=%s code=%d latency_ms=%d err=%v",
			uid, route, clientMsgID, business.Code().Code(), latency, err)
		metrics.Default.Inc("zim_messages_sent_total", map[string]string{
			"route": routeName(route), "result": "error",
		})
		return
	}
	metrics.Default.Observe("zim_message_send_latency_ms", map[string]string{"route": routeName(route)}, latency)
	log.Debugf("message sent uid=%d route=%d client_msg_id=%s server_msg_id=%s seq=%d latency_ms=%d",
		uid, route, clientMsgID, result.ServerMsgID, result.Seq, latency)
	metrics.Default.Inc("zim_messages_sent_total", map[string]string{
		"route": routeName(route), "result": "ok",
	})
}

func routeName(route int32) string {
	if route == contract.OpSendGroup {
		return "group"
	}
	return "single"
}

// editMessage handles op:1507. Only the sender may rewrite a message inside the
// edit window; participants get the new body as a 1901 update.
func (c *core) editMessage(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpEdit, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.EditMessageReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpEdit, nil, errs.ErrMessageEmpty)
			return
		}
		msg, err := c.msg.Edit(ctx.Context(), uid, req.GetServerMsgId(), req.GetContent())
		if err != nil {
			c.respond(ctx, contract.OpEdit, nil, err)
			return
		}
		c.msg.PublishUpdate(ctx.Context(), msg, c.msg.Recipients(ctx.Context(), msg))
		payload, _ := proto.Marshal(&msgsvc.OperationRes{Ok: true})
		c.respond(ctx, contract.OpEdit, payload, nil)
	})
}

// deleteMessage handles op:1508. The message stays as a "deleted" placeholder so
// both sides keep the seq.
func (c *core) deleteMessage(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpDeleteMsg, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.DeleteMessageReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpDeleteMsg, nil, errs.ErrMessageEmpty)
			return
		}
		msg, err := c.msg.Delete(ctx.Context(), uid, req.GetServerMsgId())
		if err != nil {
			c.respond(ctx, contract.OpDeleteMsg, nil, err)
			return
		}
		c.msg.PublishUpdate(ctx.Context(), msg, c.msg.Recipients(ctx.Context(), msg))
		payload, _ := proto.Marshal(&msgsvc.OperationRes{Ok: true})
		c.respond(ctx, contract.OpDeleteMsg, payload, nil)
	})
}

// searchMessage handles op:1512.
func (c *core) searchMessage(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSearchMsg, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.SearchMessageReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSearchMsg, nil, errs.ErrMessageEmpty)
			return
		}
		messages, err := c.msg.Search(ctx.Context(), req.GetConversationId(), req.GetKeyword(), int(req.GetLimit()))
		if err != nil {
			c.respond(ctx, contract.OpSearchMsg, nil, err)
			return
		}
		res := &msgsvc.SearchMessageRes{Messages: make([]*msgsvc.MessageItem, 0, len(messages))}
		for _, msg := range messages {
			res.Messages = append(res.Messages, toWireMessage(msg))
		}
		c.attachReactions(ctx.Context(), req.GetConversationId(), uid, res.Messages)
		payload, err := proto.Marshal(res)
		if err != nil {
			c.respond(ctx, contract.OpSearchMsg, nil, errs.ErrInternal.WithCause(err))
			return
		}
		c.respond(ctx, contract.OpSearchMsg, payload, nil)
	})
}

// toWireMessage converts a stored message into its wire shape.
func toWireMessage(item *model.Message) *msgsvc.MessageItem {
	return &msgsvc.MessageItem{
		ServerMsgId: item.ServerMsgID, ClientMsgId: item.ClientMsgID,
		ConversationId: item.ConversationID, Seq: item.Seq, SendId: item.SendID,
		RecvId: item.RecvID, GroupId: item.GroupID, MsgType: item.MsgType,
		Content: item.Content, AtUserIds: item.AtUserIDs, SendTime: item.SendTime,
		Status: item.Status,
	}
}

// quoteMessage handles op:1504.
func (c *core) quoteMessage(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpQuote, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.QuoteMessageReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpQuote, nil, errs.ErrMessageEmpty)
			return
		}
		result, err := c.msg.SendQuote(ctx.Context(), uid, &message.QuoteRequest{
			ClientMsgID: req.GetClientMsgId(),
			RecvID:      req.GetRecvId(),
			GroupID:     req.GetGroupId(),
			ServerMsgID: req.GetServerMsgId(),
			Content:     req.GetContent(),
		})
		if err != nil {
			c.respond(ctx, contract.OpQuote, nil, err)
			return
		}
		payload, _ := proto.Marshal(&msgsvc.QuoteMessageRes{
			ServerMsgId: result.ServerMsgID, Seq: result.Seq, SendTime: result.SendTime,
		})
		c.respond(ctx, contract.OpQuote, payload, nil)
	})
}

// mergeMessage handles op:1503.
func (c *core) mergeMessage(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpMerge, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.MergeMessageReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpMerge, nil, errs.ErrMessageEmpty)
			return
		}
		result, err := c.msg.SendMerge(ctx.Context(), uid, &message.MergeRequest{
			ClientMsgID:  req.GetClientMsgId(),
			RecvID:       req.GetRecvId(),
			GroupID:      req.GetGroupId(),
			ServerMsgIDs: req.GetServerMsgIds(),
			Title:        req.GetTitle(),
		})
		if err != nil {
			c.respond(ctx, contract.OpMerge, nil, err)
			return
		}
		payload, _ := proto.Marshal(&msgsvc.MergeMessageRes{
			ServerMsgId: result.ServerMsgID, Seq: result.Seq, SendTime: result.SendTime,
		})
		c.respond(ctx, contract.OpMerge, payload, nil)
	})
}
