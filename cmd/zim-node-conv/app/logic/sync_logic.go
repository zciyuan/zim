package logic

import (
	"github.com/dobyte/due/v2/cluster/node"

	"github.com/zciyuan/ZIM/internal/model"
	syncwire "github.com/zciyuan/ZIM/internal/protocol/sync"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func (c *core) syncConversation(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		req := &syncwire.SyncConversationReq{}
		res := &syncwire.SyncConversationRes{}
		if _, err := parse(ctx, req); err != nil {
			respond(ctx, contract.OpSyncConversation, res, errs.ErrInternal)
			return
		}
		uid, err := uidOf(ctx)
		if err != nil {
			respond(ctx, contract.OpSyncConversation, res, err)
			return
		}
		entries, err := c.changes.List(ctx.Context(), uid, model.EntityConversation, req.GetVersion(), int(req.GetLimit()))
		if err != nil {
			respond(ctx, contract.OpSyncConversation, res, errs.ErrInternal.WithCause(err))
			return
		}
		res.Version = req.GetVersion()
		res.Changes = make([]*syncwire.SyncChange, 0, len(entries))
		for _, entry := range entries {
			res.Changes = append(res.Changes, &syncwire.SyncChange{
				ObjId: entry.ObjID, State: entry.State, Version: entry.Version, Mtime: entry.Mtime,
			})
			if entry.Version > res.Version {
				res.Version = entry.Version
			}
		}
		res.HasMore = len(entries) >= int(req.GetLimit())
		respond(ctx, contract.OpSyncConversation, res, nil)
	})
}
