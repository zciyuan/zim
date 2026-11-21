package logic

import (
	"context"
	"strconv"
	"strings"

	"github.com/dobyte/due/v2/cluster/node"
	duejson "github.com/dobyte/due/v2/encoding/json"
	"google.golang.org/protobuf/proto"

	syncwire "github.com/zciyuan/ZIM/internal/protocol/sync"
	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	relationclient "github.com/zciyuan/ZIM/internal/service/relation/client"
	relationpb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// fullSync handles op:1606 by type. Conversation uses the local read model;
// friend/group use the relation/group mesh and return proto-encoded items.
func (c *core) fullSync(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpFullSync, nil, errs.ErrUnauthorized)
			return
		}
		req := &syncwire.FullSyncReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpFullSync, nil, errs.ErrInternal)
			return
		}
		offset, _ := strconv.Atoi(req.GetCursor())
		if offset < 0 {
			offset = 0
		}
		limit := int(req.GetLimit())
		if limit <= 0 || limit > 200 {
			limit = 100
		}
		res := &syncwire.FullSyncRes{Type: req.GetType()}
		if res.Type == "" {
			res.Type = "conversation"
		}
		switch res.Type {
		case "conversation":
			items, listErr := c.conv.List(ctx.Context(), uid, offset, limit)
			if listErr != nil {
				c.respond(ctx, contract.OpFullSync, nil, errs.ErrInternal.WithCause(listErr))
				return
			}
			for _, item := range items {
				data, _ := duejson.Marshal(item)
				res.Items = append(res.Items, &syncwire.FullSyncItem{
					Id: item.ConversationID, Version: item.LastMsgSeq, Data: data,
				})
			}
			res.HasMore = len(items) == limit
		case "friend":
			items, listErr := c.fullSyncFriends(ctx.Context(), uid)
			if listErr != nil {
				c.respond(ctx, contract.OpFullSync, nil, listErr)
				return
			}
			res.Items = pageProto(offset, limit, items)
			res.HasMore = offset+len(res.Items) < len(items)
		case "group":
			items, listErr := c.fullSyncGroups(ctx.Context(), uid)
			if listErr != nil {
				c.respond(ctx, contract.OpFullSync, nil, listErr)
				return
			}
			res.Items = pageProto(offset, limit, items)
			res.HasMore = offset+len(res.Items) < len(items)
		case "message":
			items, more, msgErr := c.fullSyncMessages(ctx.Context(), uid, req.GetCursor(), limit)
			if msgErr != nil {
				c.respond(ctx, contract.OpFullSync, nil, msgErr)
				return
			}
			res.Items = items
			res.HasMore = more
		default:
			c.respond(ctx, contract.OpFullSync, nil, errs.ErrInternal)
			return
		}
		res.NextCursor = strconv.Itoa(offset + len(res.Items))
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpFullSync, payload, nil)
	})
}

func (c *core) fullSyncFriends(ctx context.Context, uid int64) ([]*syncwire.FullSyncItem, error) {
	client, err := relationclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return nil, errs.ErrServerBusy.WithCause(err)
	}
	reply, err := client.ListFriends(ctx, &relationpb.ListFriendsArgs{Uid: uid})
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	items := make([]*syncwire.FullSyncItem, 0, len(reply.GetFriends()))
	for _, friend := range reply.GetFriends() {
		data, _ := proto.Marshal(friend)
		items = append(items, &syncwire.FullSyncItem{
			Id: strconv.FormatInt(friend.GetFriendId(), 10), Version: friend.GetCreatedAt(), Data: data,
		})
	}
	return items, nil
}

func (c *core) fullSyncGroups(ctx context.Context, uid int64) ([]*syncwire.FullSyncItem, error) {
	client, err := groupclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return nil, errs.ErrServerBusy.WithCause(err)
	}
	reply, err := client.MyGroups(ctx, &grouppb.MyGroupsArgs{UserId: uid})
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	items := make([]*syncwire.FullSyncItem, 0, len(reply.GetGroups()))
	for _, group := range reply.GetGroups() {
		data, _ := proto.Marshal(group)
		items = append(items, &syncwire.FullSyncItem{
			Id: strconv.FormatInt(group.GetGroupId(), 10), Version: group.GetCreatedAt(), Data: data,
		})
	}
	return items, nil
}

func pageProto(offset, limit int, items []*syncwire.FullSyncItem) []*syncwire.FullSyncItem {
	if offset >= len(items) {
		return nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func (c *core) fullSyncMessages(ctx context.Context, uid int64, cursor string, limit int) ([]*syncwire.FullSyncItem, bool, error) {
	conversations, err := c.conv.List(ctx, uid, 0, 200)
	if err != nil {
		return nil, false, errs.ErrInternal.WithCause(err)
	}
	startConv := 0
	var seq int64
	if cursor != "" {
		parts := strings.SplitN(cursor, ":", 2)
		if len(parts) == 2 {
			seq, _ = strconv.ParseInt(parts[1], 10, 64)
			for index, conversation := range conversations {
				if conversation.ConversationID == parts[0] {
					startConv = index
					break
				}
			}
		}
	}
	for index := startConv; index < len(conversations); index++ {
		conversation := conversations[index]
		begin := seq + 1
		if conversation.ConversationID != strings.SplitN(cursor, ":", 2)[0] {
			begin = 1
		}
		end := begin + int64(limit) - 1
		messages, hasMore, pullErr := c.msg.Pull(ctx, conversation.ConversationID, begin, end, limit)
		if pullErr != nil {
			return nil, false, errs.ErrInternal.WithCause(pullErr)
		}
		if len(messages) == 0 {
			continue
		}
		items := make([]*syncwire.FullSyncItem, 0, len(messages))
		for _, message := range messages {
			data, _ := proto.Marshal(&syncwire.SyncMissingItem{
				ServerMsgId: message.ServerMsgID, ClientMsgId: message.ClientMsgID,
				Seq: message.Seq, SendId: message.SendID, RecvId: message.RecvID,
				GroupId: message.GroupID, MsgType: message.MsgType, Content: message.Content,
				SendTime: message.SendTime, Status: message.Status,
			})
			items = append(items, &syncwire.FullSyncItem{
				Id: message.ServerMsgID, Version: message.Seq, Data: data,
			})
		}
		more := hasMore || index+1 < len(conversations)
		return items, more, nil
	}
	return nil, false, nil
}
