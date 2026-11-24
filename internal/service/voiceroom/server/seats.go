package server

import (
	"context"
	"sort"
	"strconv"

	"github.com/dobyte/due/v2/log"

	rtcwire "github.com/zciyuan/ZIM/internal/protocol/rtc"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// seatErr maps a Lua return code to the frozen voiceroom error set.
func seatErr(code int64) error {
	switch code {
	case luaErrRoomMissing:
		return errs.ErrVoiceRoomNotExist
	case luaErrRoomClosed:
		return errs.ErrVoiceRoomClosed
	case luaErrNoPermission, luaErrNotInHandQueue:
		return errs.ErrNoSeatPermission
	case luaErrSeatOccupied:
		return errs.ErrSeatOccupied
	case luaErrSeatLocked:
		return errs.ErrSeatLocked
	case luaErrAlreadySeated:
		return errs.ErrSeatOccupied
	case luaErrSeatNotFound:
		return errs.ErrNotInVoiceRoom
	case luaErrBadSeatIndex:
		return errs.ErrVoiceRoomParamInvalid
	case luaErrNotRoomMember:
		return errs.ErrNotInVoiceRoom
	default:
		return errs.ErrInternal
	}
}

// doTakeSeat occupies a seat and grants publish permission on the media
// plane. In hand-up mode only the owner may sit without approval.
func (s *Service) doTakeSeat(ctx context.Context, args takeSeatArgs) (*pb.SeatOperationReply, error) {
	if args.UID <= 0 {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	approved := int64(1)
	if room.JoinSeatMode == 2 && args.UID != room.OwnerUID {
		return nil, errs.ErrNoSeatPermission
	}
	result, err := takeSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID), handupKey(room.ID)},
		args.UID, args.SeatIndex, s.now().UnixMilli(), approved).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	roomName := roomNameOf(room.ID)
	if err = s.media.UpdateParticipantPermission(ctx, roomName, args.UID, true); err != nil {
		log.Warnf("grant publish failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doLeaveSeat frees the caller's seat and revokes publish permission.
func (s *Service) doLeaveSeat(ctx context.Context, args leaveSeatArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	result, err := leaveSeatScript.Run(ctx, s.client, []string{seatsKey(room.ID)}, args.UID).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	if err = s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), args.UID, false); err != nil {
		log.Warnf("revoke publish failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doKickSeat frees a seat on behalf of the owner, revokes publish and drops
// the member from the media room.
func (s *Service) doKickSeat(ctx context.Context, args kickSeatArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	result, err := kickSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID)},
		args.OperatorUID, args.UID, args.SeatIndex).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	roomName := roomNameOf(room.ID)
	if err = s.media.UpdateParticipantPermission(ctx, roomName, args.UID, false); err != nil {
		log.Warnf("revoke publish on kick failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	if err = s.media.RemoveParticipant(ctx, roomName, args.UID); err != nil {
		log.Warnf("remove participant on kick failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	if _, err = s.removeMember(ctx, room, args.UID); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doForceMuteSeat toggles force mute; muted seats lose publish permission,
// unmuting restores it.
func (s *Service) doForceMuteSeat(ctx context.Context, args forceMuteSeatArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	muted := int64(0)
	if args.Muted {
		muted = 1
	}
	result, err := forceMuteSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID)},
		args.OperatorUID, args.SeatIndex, muted).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	uid := s.seatUID(ctx, room.ID, args.SeatIndex)
	if uid > 0 {
		roomName := roomNameOf(room.ID)
		if err = s.media.UpdateParticipantPermission(ctx, roomName, uid, !args.Muted); err != nil {
			log.Warnf("sync force mute failed: uid = %d, room_id = %d, err = %v", uid, room.ID, err)
		}
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doLockSeat locks or unlocks an empty seat.
func (s *Service) doLockSeat(ctx context.Context, args lockSeatArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	locked := int64(0)
	if args.Locked {
		locked = 1
	}
	result, err := lockSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID)},
		args.OperatorUID, args.SeatIndex, locked).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doHandUp enqueues a hand-up request in approval mode.
func (s *Service) doHandUp(ctx context.Context, args handUpArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	if room.JoinSeatMode != 2 {
		return nil, errs.ErrNoSeatPermission
	}
	exists, err := s.client.HExists(ctx, roomKey(room.ID), "m:"+strconv.FormatInt(args.UID, 10)).Result()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if !exists {
		return nil, errs.ErrNotInVoiceRoom
	}
	result, err := handUpScript.Run(ctx, s.client, []string{handupKey(room.ID)}, args.UID, s.now().UnixMilli()).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, errs.ErrAlreadyHandUp
	}
	s.touch(ctx, room.ID)
	return &pb.SeatOperationReply{Ok: true}, nil
}

// doApproveHandUp accepts (seats) or rejects the named hand-up request.
func (s *Service) doApproveHandUp(ctx context.Context, args approveHandUpArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	if args.OperatorUID != room.OwnerUID {
		return nil, errs.ErrNoSeatPermission
	}
	if !args.Accept {
		_ = s.client.ZRem(ctx, handupKey(room.ID), args.UID).Err()
		return &pb.SeatOperationReply{Ok: true}, nil
	}
	if err := s.client.ZScore(ctx, handupKey(room.ID), strconv.FormatInt(args.UID, 10)).Err(); err != nil {
		return nil, errs.ErrNoSeatPermission
	}
	result, err := takeSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID), handupKey(room.ID)},
		args.UID, args.SeatIndex, s.now().UnixMilli(), 1).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	if err = s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), args.UID, true); err != nil {
		log.Warnf("grant publish on approve failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// doInviteToSeat seats an invited room member.
func (s *Service) doInviteToSeat(ctx context.Context, args inviteToSeatArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	if args.OperatorUID != room.OwnerUID {
		return nil, errs.ErrNoSeatPermission
	}
	result, err := takeSeatScript.Run(ctx, s.client,
		[]string{roomKey(room.ID), seatsKey(room.ID), handupKey(room.ID)},
		args.UID, args.SeatIndex, s.now().UnixMilli(), 1).Int64()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if result < 0 {
		return nil, seatErr(result)
	}
	if err = s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), args.UID, true); err != nil {
		log.Warnf("grant publish on invite failed: uid = %d, room_id = %d, err = %v", args.UID, room.ID, err)
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return s.seatReply(ctx, room.ID)
}

// seats returns the ordered seat snapshot for a room.
func (s *Service) seats(ctx context.Context, roomID int64) ([]*rtcwire.SeatState, error) {
	values, err := s.client.HGetAll(ctx, seatsKey(roomID)).Result()
	if err != nil {
		return nil, err
	}
	states := make([]*rtcwire.SeatState, 0, len(values))
	for field, value := range values {
		index, parseErr := strconv.ParseInt(field, 10, 64)
		if parseErr != nil {
			continue
		}
		uid, status := parseSeatValue(value)
		states = append(states, &rtcwire.SeatState{SeatIndex: int32(index), Uid: uid, Status: status})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].SeatIndex < states[j].SeatIndex })
	return states, nil
}

func (s *Service) seatReply(ctx context.Context, roomID int64) (*pb.SeatOperationReply, error) {
	states, err := s.seats(ctx, roomID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &pb.SeatOperationReply{Ok: true, Seats: seatInfos(states)}, nil
}

func seatInfos(states []*rtcwire.SeatState) []*pb.SeatInfo {
	infos := make([]*pb.SeatInfo, 0, len(states))
	for _, state := range states {
		infos = append(infos, &pb.SeatInfo{SeatIndex: state.SeatIndex, Uid: state.Uid, Status: state.Status})
	}
	return infos
}

func (s *Service) seatUID(ctx context.Context, roomID int64, seatIndex int32) int64 {
	value, err := s.client.HGet(ctx, seatsKey(roomID), strconv.FormatInt(int64(seatIndex), 10)).Result()
	if err != nil {
		return 0
	}
	uid, _ := parseSeatValue(value)
	return uid
}

func parseSeatValue(value string) (int64, int32) {
	var uid int64
	var status int32
	for i := 0; i < len(value); i++ {
		if value[i] == ':' {
			uid, _ = strconv.ParseInt(value[:i], 10, 64)
			parsed, _ := strconv.ParseInt(value[i+1:], 10, 32)
			status = int32(parsed)
			break
		}
	}
	return uid, status
}
