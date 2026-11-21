package logic

import (
	"context"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	relwire "github.com/zciyuan/ZIM/internal/protocol/relation"
	relationclient "github.com/zciyuan/ZIM/internal/service/relation/client"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// addFriend handles op:1201 (WS mirror of POST /v1/friends).
func (c *core) addFriend(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpAddFriend, nil, errs.ErrUnauthorized)
			return
		}
		req := &relwire.AddFriendReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetFriendId() <= 0 {
			c.respond(ctx, contract.OpAddFriend, nil, errs.ErrUserNotExist)
			return
		}
		client, err := relationclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpAddFriend, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.SendFriendRequest(ctx.Context(), &pb.SendFriendRequestArgs{
			FromId: uid, ToId: req.GetFriendId(), Why: req.GetWhy(),
		})
		if err != nil {
			c.respond(ctx, contract.OpAddFriend, nil, relationError(err))
			return
		}
		c.respondRelation(ctx, contract.OpAddFriend, reply.GetOk())
		c.pushFriendEvent(ctx.Context(), 1, uid, req.GetFriendId())
	})
}

// answerFriend handles op:1202 (WS mirror of POST /v1/friend-requests/answer).
func (c *core) answerFriend(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpAnswerFriend, nil, errs.ErrUnauthorized)
			return
		}
		req := &relwire.AnswerFriendReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetFriendId() <= 0 {
			c.respond(ctx, contract.OpAnswerFriend, nil, errs.ErrFriendRequestNotExist)
			return
		}
		client, err := relationclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpAnswerFriend, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.AnswerFriend(ctx.Context(), &pb.AnswerFriendArgs{
			FromId: req.GetFriendId(), ToId: uid, Accept: req.GetAccept(),
		})
		if err != nil {
			c.respond(ctx, contract.OpAnswerFriend, nil, relationError(err))
			return
		}
		c.respondRelation(ctx, contract.OpAnswerFriend, reply.GetOk())
		kind, from, to := answerFriendEvent(uid, req.GetFriendId(), req.GetAccept())
		c.pushFriendEvent(ctx.Context(), kind, from, to)
	})
}

// deleteFriend handles op:1204 (WS mirror of DELETE /v1/friends/{uid}).
func (c *core) deleteFriend(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpDeleteFriend, nil, errs.ErrUnauthorized)
			return
		}
		req := &relwire.DeleteFriendReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetFriendId() <= 0 {
			c.respond(ctx, contract.OpDeleteFriend, nil, errs.ErrUserNotExist)
			return
		}
		client, err := relationclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpDeleteFriend, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.DeleteFriend(ctx.Context(), &pb.DeleteFriendArgs{
			Uid: uid, FriendId: req.GetFriendId(),
		})
		if err != nil {
			c.respond(ctx, contract.OpDeleteFriend, nil, relationError(err))
			return
		}
		c.respondRelation(ctx, contract.OpDeleteFriend, reply.GetOk())
		c.pushFriendEvent(ctx.Context(), 3, uid, req.GetFriendId())
	})
}

// addBlack handles op:1208 (WS mirror of POST/DELETE /v1/blacklist).
func (c *core) addBlack(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpAddBlack, nil, errs.ErrUnauthorized)
			return
		}
		req := &relwire.AddBlackReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetUid() <= 0 {
			c.respond(ctx, contract.OpAddBlack, nil, errs.ErrUserNotExist)
			return
		}
		client, err := relationclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpAddBlack, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.AddBlack(ctx.Context(), &pb.AddBlackArgs{
			Uid: uid, BlackId: req.GetUid(), Remove: req.GetRemove(),
		})
		if err != nil {
			c.respond(ctx, contract.OpAddBlack, nil, relationError(err))
			return
		}
		c.respondRelation(ctx, contract.OpAddBlack, reply.GetOk())
		blackKind := int32(4)
		if req.GetRemove() {
			blackKind = 5
		}
		c.pushFriendEvent(ctx.Context(), blackKind, uid, req.GetUid())
	})
}

// respondRelation writes the shared ok:true response.
func (c *core) respondRelation(ctx node.Context, opcode int32, ok bool) {
	payload, _ := proto.Marshal(&relwire.RelationOperationRes{Ok: ok})
	c.respond(ctx, opcode, payload, nil)
}

// relationError maps mesh relation errors onto contract error codes, so the WS
// channel reports the same business code as REST (120001/120002/120003) instead
// of collapsing an already-handled request into 900002.
func relationError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errs.ErrFriendRequestNotExist
	case codes.AlreadyExists:
		return errs.ErrAlreadyFriend
	case codes.PermissionDenied:
		return errs.ErrBlockedByPeer
	case codes.InvalidArgument:
		return errs.ErrUserNotExist
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

// answerFriendEvent returns the 1909 kind and direction for an answer. For an
// accepted request the pair keeps the original request direction (requester ->
// acceptor), so the acceptor sees the requester in from_uid and can remove the
// pending inbox item locally. A rejection uses the "deleted" kind with the
// actor (the rejecting user) as from_uid.
func answerFriendEvent(uid, friendID int64, accept bool) (kind int32, from, to int64) {
	if accept {
		return 2, friendID, uid
	}
	return 3, uid, friendID
}

// pushFriendEvent tells both sides that a friend relation changed (1909).
func (c *core) pushFriendEvent(ctx context.Context, kind int32, from, to int64) {
	payload, err := proto.Marshal(&msgsvc.FriendEventPush{Kind: kind, FromUid: from, ToUid: to})
	if err != nil {
		return
	}
	for _, uid := range []int64{from, to} {
		if uid <= 0 {
			continue
		}
		c.pushDeviceKeys(ctx, c.deviceKeys(ctx, uid), contract.OpPushFriend, payload, false)
	}
}
