package logic

import (
	"strconv"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	syncwire "github.com/zciyuan/ZIM/internal/protocol/sync"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// fillSyncState adds the login SyncState (ADR-0023). A failure degrades to an
// empty state instead of failing login: the client can still call 1601 later.
func (c *core) fillSyncState(ctx loginContext, res *authwire.LoginRes) {
	if c.sync == nil || res == nil {
		return
	}
	uid, err := strconv.ParseInt(res.GetUid(), 10, 64)
	if err != nil {
		return
	}
	state, err := c.sync.BuildState(ctx.Context(), uid)
	if err != nil {
		log.Warnf("build sync state failed: uid = %d, err = %v", uid, err)
		return
	}
	res.Sync = &authwire.SyncState{
		ConversationVersion: state.ConversationVersion,
		FriendVersion:       state.FriendVersion,
		MaxSeqs:             state.MaxSeqs,
	}
}

// syncCheck handles op:1601.
func (c *core) syncCheck(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSyncCheck, nil, errs.ErrUnauthorized)
			return
		}
		if c.sync == nil {
			c.respond(ctx, contract.OpSyncCheck, nil, errs.ErrInternal)
			return
		}
		req := &syncwire.SyncCheckReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSyncCheck, nil, errs.ErrMessageEmpty)
			return
		}
		result, err := c.sync.Check(ctx.Context(), uid, syncsvc.Request{
			ConversationVersion: req.GetConversationVersion(),
			FriendVersion:       req.GetFriendVersion(),
			MaxSeqs:             req.GetMaxSeqs(),
		})
		if err != nil {
			c.respond(ctx, contract.OpSyncCheck, nil, err)
			return
		}
		res := &syncwire.SyncCheckRes{
			ConversationVersion: result.ConversationVersion,
			FriendVersion:       result.FriendVersion,
			FriendChanged:       result.FriendChanged,
			FullSyncRequired:    result.FullSyncRequired,
			Truncated:           result.Truncated,
			ServerTime:          result.ServerTime,
		}
		for _, diff := range result.Diffs {
			res.ConversationDiff = append(res.ConversationDiff, &syncwire.SyncRange{
				ConvId: diff.ConversationID, FromSeq: diff.FromSeq, ToSeq: diff.ToSeq,
			})
		}
		payload, marshalErr := proto.Marshal(res)
		if marshalErr != nil {
			c.respond(ctx, contract.OpSyncCheck, nil, errs.ErrInternal.WithCause(marshalErr))
			return
		}
		c.respond(ctx, contract.OpSyncCheck, payload, nil)
	})
}

// syncMissing handles op:1514.
func (c *core) syncMissing(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSyncMissing, nil, errs.ErrUnauthorized)
			return
		}
		if c.sync == nil {
			c.respond(ctx, contract.OpSyncMissing, nil, errs.ErrInternal)
			return
		}
		req := &syncwire.SyncMissingReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSyncMissing, nil, errs.ErrMessageEmpty)
			return
		}
		messages, missing, err := c.sync.Missing(ctx.Context(), uid, req.GetConversationId(),
			req.GetSeqs(), int(req.GetLimit()))
		if err != nil {
			c.respond(ctx, contract.OpSyncMissing, nil, err)
			return
		}
		res := &syncwire.SyncMissingRes{Missing: missing}
		for _, message := range messages {
			res.Messages = append(res.Messages, &syncwire.SyncMissingItem{
				ServerMsgId: message.ServerMsgID, ClientMsgId: message.ClientMsgID,
				Seq: message.Seq, SendId: message.SendID, RecvId: message.RecvID,
				GroupId: message.GroupID, MsgType: message.MsgType, Content: message.Content,
				SendTime: message.SendTime, Status: message.Status,
			})
		}
		payload, marshalErr := proto.Marshal(res)
		if marshalErr != nil {
			c.respond(ctx, contract.OpSyncMissing, nil, errs.ErrInternal.WithCause(marshalErr))
			return
		}
		c.respond(ctx, contract.OpSyncMissing, payload, nil)
	})
}
