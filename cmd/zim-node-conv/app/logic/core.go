// Package logic implements the conversation node routes.
package logic

import (
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	convwire "github.com/zciyuan/ZIM/internal/protocol/conversation"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversation "github.com/zciyuan/ZIM/internal/service/conversation/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// core owns the conversation routes.

// NewCore returns the conversation node core.
func NewCore(proxy *node.Proxy, svc *conversation.Service, changes *changelogsvc.Service) *core {
	return &core{proxy: proxy, svc: svc, changes: changes}
}

// Init registers conversation routes.
func (c *core) Init() {
	c.proxy.Router().AddRouteHandler(contract.OpConversationList, c.list)
	c.proxy.Router().AddRouteHandler(contract.OpPinConversation, c.pin)
	c.proxy.Router().AddRouteHandler(contract.OpDeleteConversation, c.delete)
	c.proxy.Router().AddRouteHandler(contract.OpReadConversation, c.read)
	c.proxy.Router().AddRouteHandler(contract.OpUnread, c.unread)
	c.proxy.Router().AddRouteHandler(contract.OpSyncConversation, c.syncConversation)
	c.proxy.Router().AddRouteHandler(contract.OpRecvOpt, c.recvOpt)
}

func (c *core) list(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationListReq{}
		res := &convwire.ConversationListRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpConversationList, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpConversationList, res, err)
			return
		}
		items, err := c.svc.List(ctx.Context(), uid, int(req.GetOffset()), int(req.GetLimit()))
		if err == nil {
			res.Items = make([]*convwire.ConversationItem, 0, len(items))
			for _, item := range items {
				res.Items = append(res.Items, convert(item))
			}
			res.HasMore = len(items) >= int(req.GetLimit())
		}
		respond(ctx, contract.OpConversationList, res, err)
	})
}

func (c *core) pin(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationPinReq{}
		res := &convwire.OperationRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpPinConversation, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpPinConversation, res, err)
			return
		}
		err = c.svc.Pin(ctx.Context(), uid, req.GetConversationId(), req.GetPinned())
		if err == nil {
			res.Ok = true
		}
		respond(ctx, contract.OpPinConversation, res, err)
	})
}

func (c *core) delete(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationDeleteReq{}
		res := &convwire.OperationRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpDeleteConversation, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpDeleteConversation, res, err)
			return
		}
		err = c.svc.Delete(ctx.Context(), uid, req.GetConversationId())
		if err == nil {
			res.Ok = true
		}
		respond(ctx, contract.OpDeleteConversation, res, err)
	})
}

func (c *core) read(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationReadReq{}
		res := &convwire.ConversationReadRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpReadConversation, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpReadConversation, res, err)
			return
		}
		seq, err := c.svc.Read(ctx.Context(), uid, req.GetConversationId(), req.GetSeq())
		if err == nil {
			res.ReadSeq = seq
		}
		respond(ctx, contract.OpReadConversation, res, err)
	})
}

func (c *core) unread(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationUnreadReq{}
		res := &convwire.ConversationUnreadRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpUnread, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpUnread, res, err)
			return
		}
		unread, err := c.svc.Unread(ctx.Context(), uid, req.GetConversationId())
		if err == nil {
			res.Unread = unread
		}
		respond(ctx, contract.OpUnread, res, err)
	})
}

func (c *core) recvOpt(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &convwire.ConversationRecvOptReq{}
		res := &convwire.OperationRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpRecvOpt, res, err)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpRecvOpt, res, err)
			return
		}
		err = c.svc.SetRecvOpt(ctx.Context(), uid, req.GetConversationId(), int16(req.GetRecvOpt()))
		if err == nil {
			res.Ok = true
		}
		respond(ctx, contract.OpRecvOpt, res, err)
	})
}

func parse(ctx node.Context, req proto.Message) (*contract.Envelope, error) {
	envelope := &contract.Envelope{}
	if err := ctx.Parse(envelope); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(envelope.GetPayload(), req); err != nil {
		return nil, err
	}
	return envelope, nil
}

func respond(ctx node.Context, opcode int32, body proto.Message, err error) {
	payload, marshalErr := proto.Marshal(body)
	if marshalErr != nil {
		log.Errorf("conversation marshal failed: %v", marshalErr)
		return
	}
	envelope := &contract.Envelope{Opcode: opcode, Payload: payload}
	if err != nil {
		business := errs.From(err)
		envelope.Code = int32(business.Code().Code())
		envelope.Message = business.Message()
	}
	if responseErr := ctx.Response(envelope); responseErr != nil {
		log.Errorf("conversation response failed: cid = %d, route = %d, err = %v", ctx.CID(), opcode, responseErr)
	}
}

func convert(item *conversation.Item) *convwire.ConversationItem {
	return &convwire.ConversationItem{
		ConversationId: item.ConversationID, Type: int32(item.Type), PeerId: item.PeerID,
		GroupId: item.GroupID, RecvOpt: int32(item.RecvOpt), IsPinned: int32(item.IsPinned),
		LastMsgId: item.LastMsgID, LastMsgSeq: item.LastMsgSeq, LastMsgAt: item.LastMsgAt,
		LastMsgBrief: item.LastMsgBrief, ReadSeq: item.ReadSeq, MaxSeq: item.MaxSeq,
		Unread: item.Unread, UpdatedAt: item.UpdatedAt,
	}
}

func uidOf(ctx node.Context) (int64, error) {
	uid, _, err := multi.Parse(ctx.UID())
	if err != nil {
		return 0, errs.ErrUnauthorized
	}
	return uid, nil
}
