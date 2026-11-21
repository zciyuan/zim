package server

import (
	"context"

	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
)

// SendFriendRequest handles the RPC.
func (s *Server) SendFriendRequest(ctx context.Context, args *pb.SendFriendRequestArgs) (*pb.OperationReply, error) {
	return s.doSendFriendRequest(ctx, sendFriendRequestArgs{FromID: args.GetFromId(), ToID: args.GetToId(), Why: args.GetWhy()})
}

// AnswerFriend handles the RPC.
func (s *Server) AnswerFriend(ctx context.Context, args *pb.AnswerFriendArgs) (*pb.OperationReply, error) {
	return s.doAnswerFriend(ctx, answerFriendArgs{FromID: args.GetFromId(), ToID: args.GetToId(), Accept: args.GetAccept()})
}

// ListFriendRequests handles the RPC.
func (s *Server) ListFriendRequests(ctx context.Context, args *pb.ListFriendRequestsArgs) (*pb.FriendRequestsReply, error) {
	return s.doListFriendRequests(ctx, listFriendRequestsArgs{UID: args.GetUid(), Inbound: args.GetInbound()})
}

// DeleteFriend handles the RPC.
func (s *Server) DeleteFriend(ctx context.Context, args *pb.DeleteFriendArgs) (*pb.OperationReply, error) {
	return s.doDeleteFriend(ctx, deleteFriendArgs{UID: args.GetUid(), FriendID: args.GetFriendId()})
}

// ListFriends handles the RPC.
func (s *Server) ListFriends(ctx context.Context, args *pb.ListFriendsArgs) (*pb.FriendsReply, error) {
	return s.doListFriends(ctx, listFriendsArgs{UID: args.GetUid()})
}

// UpdateRemark handles the RPC.
func (s *Server) UpdateRemark(ctx context.Context, args *pb.UpdateRemarkArgs) (*pb.OperationReply, error) {
	return s.doUpdateRemark(ctx, updateRemarkArgs{UID: args.GetUid(), FriendID: args.GetFriendId(), Remark: args.GetRemark()})
}

// PinFriend handles the RPC.
func (s *Server) PinFriend(ctx context.Context, args *pb.PinFriendArgs) (*pb.OperationReply, error) {
	return s.doPinFriend(ctx, pinFriendArgs{UID: args.GetUid(), FriendID: args.GetFriendId(), Pinned: args.GetPinned()})
}

// AddBlack handles the RPC.
func (s *Server) AddBlack(ctx context.Context, args *pb.AddBlackArgs) (*pb.OperationReply, error) {
	return s.doAddBlack(ctx, addBlackArgs{UID: args.GetUid(), BlackID: args.GetBlackId(), Remove: args.GetRemove()})
}

// ListBlacklist handles the RPC.
func (s *Server) ListBlacklist(ctx context.Context, args *pb.ListBlacklistArgs) (*pb.ListBlacklistReply, error) {
	return s.doListBlacklist(ctx, listBlacklistArgs{UID: args.GetUid()})
}

// IsFriend handles the RPC.
func (s *Server) IsFriend(ctx context.Context, args *pb.IsFriendArgs) (*pb.IsFriendReply, error) {
	return s.doIsFriend(ctx, isFriendArgs{UID: args.GetUid(), TargetID: args.GetTargetId()})
}
