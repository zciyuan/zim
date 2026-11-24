package server

import (
	"context"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
)

// record keeps the fixed result metric and a trace log line per route.
func (s *Service) record(route string, uid, roomID int64, err error) {
	result := "ok"
	if err != nil {
		result = "fail"
		log.Errorf("voiceroom route failed: route = %s, uid = %d, room_id = %d, err = %v", route, uid, roomID, err)
	} else {
		log.Infof("voiceroom route ok: route = %s, uid = %d, room_id = %d", route, uid, roomID)
	}
	metrics.Default.Inc("zim_voiceroom_total", map[string]string{"result": result})
}

// CreateRoom handles the RPC (op 1808).
func (s *Service) CreateRoom(ctx context.Context, args *pb.CreateRoomArgs) (*pb.CreateRoomReply, error) {
	reply, err := s.doCreateRoom(ctx, createRoomArgs{
		OwnerUID: args.GetOwnerUid(), RoomType: args.GetRoomType(), GroupID: args.GetGroupId(),
		Title: args.GetTitle(), Password: args.GetPassword(), SeatCount: args.GetSeatCount(),
		JoinSeatMode: args.GetJoinSeatMode(),
	})
	s.record("CreateRoom", args.GetOwnerUid(), 0, err)
	return reply, err
}

// ListRooms handles the RPC (op 1809).
func (s *Service) ListRooms(ctx context.Context, args *pb.ListRoomsArgs) (*pb.ListRoomsReply, error) {
	reply, err := s.doListRooms(ctx, listRoomsArgs{GroupID: args.GetGroupId(), Offset: args.GetOffset(), Limit: args.GetLimit()})
	s.record("ListRooms", 0, 0, err)
	return reply, err
}

// JoinRoom handles the RPC (op 1810).
func (s *Service) JoinRoom(ctx context.Context, args *pb.JoinRoomArgs) (*pb.JoinRoomReply, error) {
	reply, err := s.doJoinRoom(ctx, joinRoomArgs{RoomID: args.GetRoomId(), UID: args.GetUid(), Password: args.GetPassword()})
	s.record("JoinRoom", args.GetUid(), args.GetRoomId(), err)
	return reply, err
}

// RefreshToken handles the RPC (op 1811).
func (s *Service) RefreshToken(ctx context.Context, args *pb.RefreshTokenArgs) (*pb.RefreshTokenReply, error) {
	reply, err := s.doRefreshToken(ctx, refreshTokenArgs{RoomID: args.GetRoomId(), UID: args.GetUid()})
	s.record("RefreshToken", args.GetUid(), args.GetRoomId(), err)
	return reply, err
}

// TakeSeat handles the RPC (op 1812).
func (s *Service) TakeSeat(ctx context.Context, args *pb.TakeSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doTakeSeat(ctx, takeSeatArgs{RoomID: args.GetRoomId(), UID: args.GetUid(), SeatIndex: args.GetSeatIndex()})
	s.record("TakeSeat", args.GetUid(), args.GetRoomId(), err)
	return reply, err
}

// LeaveSeat handles the RPC (op 1813).
func (s *Service) LeaveSeat(ctx context.Context, args *pb.LeaveSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doLeaveSeat(ctx, leaveSeatArgs{RoomID: args.GetRoomId(), UID: args.GetUid()})
	s.record("LeaveSeat", args.GetUid(), args.GetRoomId(), err)
	return reply, err
}

// KickSeat handles the RPC (op 1814).
func (s *Service) KickSeat(ctx context.Context, args *pb.KickSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doKickSeat(ctx, kickSeatArgs{
		RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), UID: args.GetUid(), SeatIndex: args.GetSeatIndex(),
	})
	s.record("KickSeat", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

// ForceMuteSeat handles the RPC (op 1815).
func (s *Service) ForceMuteSeat(ctx context.Context, args *pb.ForceMuteSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doForceMuteSeat(ctx, forceMuteSeatArgs{
		RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), SeatIndex: args.GetSeatIndex(), Muted: args.GetMuted(),
	})
	s.record("ForceMuteSeat", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

// LockSeat handles the RPC (op 1816).
func (s *Service) LockSeat(ctx context.Context, args *pb.LockSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doLockSeat(ctx, lockSeatArgs{
		RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), SeatIndex: args.GetSeatIndex(), Locked: args.GetLocked(),
	})
	s.record("LockSeat", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

// HandUp handles the RPC (op 1817).
func (s *Service) HandUp(ctx context.Context, args *pb.HandUpArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doHandUp(ctx, handUpArgs{RoomID: args.GetRoomId(), UID: args.GetUid()})
	s.record("HandUp", args.GetUid(), args.GetRoomId(), err)
	return reply, err
}

// ApproveHandUp handles the RPC (op 1818).
func (s *Service) ApproveHandUp(ctx context.Context, args *pb.ApproveHandUpArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doApproveHandUp(ctx, approveHandUpArgs{
		RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), UID: args.GetUid(),
		SeatIndex: args.GetSeatIndex(), Accept: args.GetAccept(),
	})
	s.record("ApproveHandUp", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

// InviteToSeat handles the RPC (op 1819).
func (s *Service) InviteToSeat(ctx context.Context, args *pb.InviteToSeatArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doInviteToSeat(ctx, inviteToSeatArgs{
		RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), UID: args.GetUid(), SeatIndex: args.GetSeatIndex(),
	})
	s.record("InviteToSeat", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

// GetRoom handles the RPC.
func (s *Service) GetRoom(ctx context.Context, args *pb.GetRoomArgs) (*pb.RoomInfo, error) {
	reply, err := s.doGetRoom(ctx, getRoomArgs{RoomID: args.GetRoomId()})
	s.record("GetRoom", 0, args.GetRoomId(), err)
	return reply, err
}

// CloseRoom handles the RPC.
func (s *Service) CloseRoom(ctx context.Context, args *pb.CloseRoomArgs) (*pb.SeatOperationReply, error) {
	reply, err := s.doCloseRoom(ctx, closeRoomArgs{RoomID: args.GetRoomId(), OperatorUID: args.GetOperatorUid(), Reason: args.GetReason()})
	s.record("CloseRoom", args.GetOperatorUid(), args.GetRoomId(), err)
	return reply, err
}

var _ pb.VoiceroomServer = (*Service)(nil)
