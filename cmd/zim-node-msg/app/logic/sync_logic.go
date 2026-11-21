package logic

import (
	"strconv"
	"strings"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/model"
	syncwire "github.com/zciyuan/ZIM/internal/protocol/sync"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

func (c *core) syncFriend(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSyncFriend, nil, errs.ErrUnauthorized)
			return
		}
		req := &syncwire.SyncFriendReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSyncFriend, nil, errs.ErrInternal)
			return
		}
		entries, err := c.changes.List(ctx.Context(), uid, model.EntityFriend, req.GetVersion(), int(req.GetLimit()))
		if err != nil {
			c.respond(ctx, contract.OpSyncFriend, nil, errs.ErrInternal.WithCause(err))
			return
		}
		changes, version := syncChanges(entries, req.GetVersion())
		payload, _ := proto.Marshal(&syncwire.SyncFriendRes{
			Version: version, Changes: changes, HasMore: len(entries) >= int(req.GetLimit()),
		})
		c.respond(ctx, contract.OpSyncFriend, payload, nil)
	})
}

func (c *core) syncGroupMember(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSyncGroupMember, nil, errs.ErrUnauthorized)
			return
		}
		req := &syncwire.SyncGroupMemberReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSyncGroupMember, nil, errs.ErrInternal)
			return
		}
		entries, err := c.changes.List(ctx.Context(), uid, model.EntityGroupMember, req.GetVersion(), int(req.GetLimit()))
		if err != nil {
			c.respond(ctx, contract.OpSyncGroupMember, nil, errs.ErrInternal.WithCause(err))
			return
		}
		prefix := strconv.FormatInt(req.GetGroupId(), 10) + ":"
		filtered := entries[:0]
		if req.GetGroupId() > 0 {
			for _, entry := range entries {
				if strings.HasPrefix(entry.ObjID, prefix) {
					filtered = append(filtered, entry)
				}
			}
		} else {
			filtered = entries
		}
		changes, version := syncChanges(filtered, req.GetVersion())
		payload, _ := proto.Marshal(&syncwire.SyncGroupMemberRes{
			Version: version, Changes: changes, HasMore: len(entries) >= int(req.GetLimit()),
		})
		c.respond(ctx, contract.OpSyncGroupMember, payload, nil)
	})
}

func syncChanges(entries []*model.ChangeLog, since int64) ([]*syncwire.SyncChange, int64) {
	changes := make([]*syncwire.SyncChange, 0, len(entries))
	version := since
	for _, entry := range entries {
		changes = append(changes, &syncwire.SyncChange{
			ObjId: entry.ObjID, State: entry.State, Version: entry.Version, Mtime: entry.Mtime,
		})
		if entry.Version > version {
			version = entry.Version
		}
	}
	return changes, version
}
