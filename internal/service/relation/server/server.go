package server

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// Repository is the social graph data required by the service.
type Repository interface {
	CreateRequest(ctx context.Context, req *model.FriendRequest) error
	GetRequest(ctx context.Context, id int64) (*model.FriendRequest, error)
	AcceptRequest(ctx context.Context, fromID, toID, now int64) error
	HandleRequest(ctx context.Context, fromID, toID int64, status int16, now int64) error
	ListRequests(ctx context.Context, uid int64, inbound bool) ([]*model.FriendRequest, error)
	AddFriends(ctx context.Context, a, b int64, now int64) error
	DeleteFriend(ctx context.Context, a, b int64) error
	List(ctx context.Context, uid int64) ([]*model.Friend, error)
	UpdateRemark(ctx context.Context, uid, friendID int64, remark string) error
	UpdatePin(ctx context.Context, uid, friendID int64, pinned int16) error
	AddBlack(ctx context.Context, uid, blackID int64, now int64) error
	RemoveBlack(ctx context.Context, uid, blackID int64) error
	Blacklist(ctx context.Context, uid int64) ([]*model.BlacklistEntry, error)
	IsFriend(ctx context.Context, a, b int64) (bool, error)
	IsBlack(ctx context.Context, uid, target int64) (bool, error)
	Relation(ctx context.Context, a, b int64) (bool, bool, error)
	CanAddFriend(ctx context.Context, uid int64) (bool, error)
}

var _ pb.RelationServer = (*Server)(nil)

// NewServer returns the relation service.
func NewServer(proxy *mesh.Proxy, repo Repository) *Server {
	return &Server{proxy: proxy, repo: repo, ids: snowflake.New()}
}

// Init registers the service provider under the discovery name relation.
func (s *Server) Init() {
	s.proxy.AddServiceProvider(define.ServiceNameRelation, &pb.Relation_ServiceDesc, s)
}

// SetChangeLog enables version-log writes.
func (s *Server) SetChangeLog(changes *changelog.Service) {
	s.changes = changes
}

// SetWebhook enables social event callbacks.
func (s *Server) SetWebhook(notifier webhook.Notifier) {
	s.webhook = notifier
}

func (s *Server) notify(eventType string, actor, target int64, status string) {
	if s.webhook == nil {
		return
	}
	s.webhook.Notify(webhook.Event{Type: eventType, SendID: actor, RecvID: target, Status: status})
}

func (s *Server) record(ctx context.Context, uid, friendID int64, state string) {
	if s.changes == nil {
		return
	}
	if err := s.changes.Append(ctx, uid, model.EntityFriend, strconv.FormatInt(friendID, 10), state); err != nil {
		log.Warnf("append friend change log failed: uid = %d, friend = %d, state = %s, err = %v",
			uid, friendID, state, err)
	}
}

// SendFriendRequest creates a pending request.
func (s *Server) doSendFriendRequest(ctx context.Context, args sendFriendRequestArgs) (*pb.OperationReply, error) {
	if args.FromID <= 0 || args.ToID <= 0 || args.FromID == args.ToID {
		return nil, status.Error(codes.InvalidArgument, "invalid user")
	}
	if friend, err := s.repo.IsFriend(ctx, args.FromID, args.ToID); err != nil {
		return nil, status.Error(codes.Internal, "relation lookup failed")
	} else if friend {
		return nil, status.Error(codes.AlreadyExists, "already friend")
	}
	if black, err := s.repo.IsBlack(ctx, args.ToID, args.FromID); err != nil {
		return nil, status.Error(codes.Internal, "relation lookup failed")
	} else if black {
		return nil, status.Error(codes.PermissionDenied, "blocked by peer")
	}
	if allowed, privacyErr := s.repo.CanAddFriend(ctx, args.ToID); privacyErr != nil {
		return nil, status.Error(codes.Internal, "privacy lookup failed")
	} else if !allowed {
		return nil, status.Error(codes.PermissionDenied, "target does not accept friend requests")
	}
	now := time.Now().UnixMilli()
	if err := s.repo.CreateRequest(ctx, &model.FriendRequest{
		ID: s.ids.Next(), FromID: args.FromID, ToID: args.ToID,
		Why: args.Why, Status: model.FriendRequestPending, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return nil, status.Error(codes.Internal, "create request failed")
	}
	s.notify("friend.request", args.FromID, args.ToID, "pending")
	return &pb.OperationReply{Ok: true}, nil
}

// AnswerFriend accepts or rejects a request addressed to the caller.
//
// The UPDATE on zim_friend_request is the arbiter: the caller must not first
// scan a page of requests and then act on the row it found, because that page
// is capped at 100 rows and can move between the list call and the click. A
// request that is already handled (or never existed) yields 120002 instead of
// a 500, and accepting a pair that is already friends stays idempotent.
func (s *Server) doAnswerFriend(ctx context.Context, args answerFriendArgs) (*pb.OperationReply, error) {
	if args.FromID <= 0 || args.ToID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid user")
	}
	now := time.Now().UnixMilli()
	if args.Accept {
		if err := s.repo.AcceptRequest(ctx, args.FromID, args.ToID, now); err != nil {
			if !dueerrors.Is(err, pgx.ErrNoRows) {
				return nil, status.Error(codes.Internal, "accept request failed")
			}
			// Accept is idempotent: a retry after a lost response must not
			// surface 120002 when the two users are already friends.
			if friend, friendErr := s.repo.IsFriend(ctx, args.FromID, args.ToID); friendErr == nil && friend {
				return &pb.OperationReply{Ok: true}, nil
			}
			return nil, status.Error(codes.NotFound, "friend request not found")
		}
		s.invalidateFriends(ctx, args.FromID, args.ToID)
		s.record(ctx, args.FromID, args.ToID, model.ChangeInsert)
		s.record(ctx, args.ToID, args.FromID, model.ChangeInsert)
		s.notify("friend.accepted", args.ToID, args.FromID, "accepted")
		return &pb.OperationReply{Ok: true}, nil
	}
	if err := s.repo.HandleRequest(ctx, args.FromID, args.ToID, model.FriendRequestRejected, now); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "friend request not found")
		}
		return nil, status.Error(codes.Internal, "reject request failed")
	}
	s.notify("friend.rejected", args.ToID, args.FromID, "rejected")
	return &pb.OperationReply{Ok: true}, nil
}

// ListFriendRequests returns inbound or outbound requests. The inbound inbox
// only exposes pending items: once a request is accepted or rejected, the
// recipient must no longer see it (and must not be offered accept/reject
// actions that would fail with 120002). Outbound items keep their status so the
// sender can see whether the peer accepted or rejected.
func (s *Server) doListFriendRequests(ctx context.Context, args listFriendRequestsArgs) (*pb.FriendRequestsReply, error) {
	requests, err := s.repo.ListRequests(ctx, args.UID, args.Inbound)
	if err != nil {
		return nil, status.Error(codes.Internal, "request lookup failed")
	}
	reply := &pb.FriendRequestsReply{Requests: make([]*pb.FriendRequestItem, 0, len(requests))}
	for _, item := range requests {
		if args.Inbound && item.Status != model.FriendRequestPending {
			continue
		}
		reply.Requests = append(reply.Requests, &pb.FriendRequestItem{
			Id: item.ID, FromId: item.FromID, ToId: item.ToID, Why: item.Why,
			Status: int32(item.Status), CreatedAt: item.CreatedAt,
		})
	}
	return reply, nil
}

// DeleteFriend removes both directions.
func (s *Server) doDeleteFriend(ctx context.Context, args deleteFriendArgs) (*pb.OperationReply, error) {
	if err := s.repo.DeleteFriend(ctx, args.UID, args.FriendID); err != nil {
		return nil, status.Error(codes.Internal, "delete friend failed")
	}
	s.invalidateFriends(ctx, args.UID, args.FriendID)
	s.record(ctx, args.UID, args.FriendID, model.ChangeDelete)
	s.record(ctx, args.FriendID, args.UID, model.ChangeDelete)
	s.notify("friend.deleted", args.UID, args.FriendID, "deleted")
	return &pb.OperationReply{Ok: true}, nil
}

// ListFriends returns the caller's friend rows.
func (s *Server) doListFriends(ctx context.Context, args listFriendsArgs) (*pb.FriendsReply, error) {
	friends, ok := s.cachedFriends(ctx, args.UID)
	if !ok {
		var err error
		friends, err = s.repo.List(ctx, args.UID)
		if err != nil {
			return nil, status.Error(codes.Internal, "friend list failed")
		}
		s.storeFriends(ctx, args.UID, friends)
	}
	reply := &pb.FriendsReply{Friends: make([]*pb.FriendItem, 0, len(friends))}
	for _, item := range friends {
		reply.Friends = append(reply.Friends, &pb.FriendItem{
			FriendId: item.FriendID, Remark: item.Remark, Pinned: int32(item.Pinned), CreatedAt: item.CreatedAt,
		})
	}
	return reply, nil
}

// UpdateRemark updates a friend remark.
func (s *Server) doUpdateRemark(ctx context.Context, args updateRemarkArgs) (*pb.OperationReply, error) {
	if err := s.repo.UpdateRemark(ctx, args.UID, args.FriendID, args.Remark); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "friend not found")
		}
		return nil, status.Error(codes.Internal, "update remark failed")
	}
	s.invalidateFriends(ctx, args.UID)
	s.record(ctx, args.UID, args.FriendID, model.ChangeUpdate)
	return &pb.OperationReply{Ok: true}, nil
}

// PinFriend updates the pin flag.
func (s *Server) doPinFriend(ctx context.Context, args pinFriendArgs) (*pb.OperationReply, error) {
	pinned := int16(0)
	if args.Pinned {
		pinned = 1
	}
	if err := s.repo.UpdatePin(ctx, args.UID, args.FriendID, pinned); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "friend not found")
		}
		return nil, status.Error(codes.Internal, "pin friend failed")
	}
	s.invalidateFriends(ctx, args.UID)
	s.record(ctx, args.UID, args.FriendID, model.ChangeUpdate)
	return &pb.OperationReply{Ok: true}, nil
}

// AddBlack adds or removes a blacklist entry.
func (s *Server) doAddBlack(ctx context.Context, args addBlackArgs) (*pb.OperationReply, error) {
	var err error
	if args.Remove {
		err = s.repo.RemoveBlack(ctx, args.UID, args.BlackID)
	} else {
		err = s.repo.AddBlack(ctx, args.UID, args.BlackID, time.Now().UnixMilli())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "blacklist update failed")
	}
	return &pb.OperationReply{Ok: true}, nil
}

// ListBlacklist returns blocked user IDs.
func (s *Server) doListBlacklist(ctx context.Context, args listBlacklistArgs) (*pb.ListBlacklistReply, error) {
	entries, err := s.repo.Blacklist(ctx, args.UID)
	if err != nil {
		return nil, status.Error(codes.Internal, "blacklist lookup failed")
	}
	reply := &pb.ListBlacklistReply{Uids: make([]int64, 0, len(entries))}
	for _, item := range entries {
		reply.Uids = append(reply.Uids, item.BlackID)
	}
	return reply, nil
}

// IsFriend reports friendship and whether the target blocked the caller.
func (s *Server) doIsFriend(ctx context.Context, args isFriendArgs) (*pb.IsFriendReply, error) {
	friend, black, err := s.repo.Relation(ctx, args.UID, args.TargetID)
	if err != nil {
		return nil, status.Error(codes.Internal, "relation lookup failed")
	}
	return &pb.IsFriendReply{Friend: friend, Black: black}, nil
}
