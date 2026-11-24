package server

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/protocol/rtc"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

// pushSeats broadcasts the current seat snapshot (1912) to room members.
func (s *Service) pushSeats(ctx context.Context, roomID int64) {
	states, err := s.seats(ctx, roomID)
	if err != nil {
		log.Warnf("seat snapshot failed: room_id = %d, err = %v", roomID, err)
		return
	}
	payload, err := proto.Marshal(&rtcwire.SeatUpdatePush{
		RoomId: strconv.FormatInt(roomID, 10),
		Seats:  states,
	})
	if err != nil {
		log.Warnf("seat push marshal failed: room_id = %d, err = %v", roomID, err)
		return
	}
	s.pushRoom(ctx, roomID, contract.OpPushSeatUpdate, payload)
}

// pushClosed broadcasts the room-closed event (1913).
func (s *Service) pushClosed(ctx context.Context, room *Room, reason int32, uids []int64) {
	payload, err := proto.Marshal(&rtcwire.VoiceRoomClosedPush{
		RoomId: strconv.FormatInt(room.ID, 10),
		Reason: reason,
	})
	if err != nil {
		log.Warnf("closed push marshal failed: room_id = %d, err = %v", room.ID, err)
		return
	}
	envelope, err := contract.EncodeEnvelope(contract.NewEnvelope(contract.OpPushVoiceRoomClosed, payload))
	if err != nil {
		log.Warnf("closed push envelope failed: room_id = %d, err = %v", room.ID, err)
		return
	}
	s.pusher.PushRoom(ctx, room, contract.OpPushVoiceRoomClosed, envelope, uids)
}

func (s *Service) pushRoom(ctx context.Context, roomID int64, opcode int32, payload []byte) {
	room, err := s.loadRoom(ctx, roomID)
	if err != nil {
		log.Warnf("push room lookup failed: room_id = %d, err = %v", roomID, err)
		return
	}
	uids, err := s.memberUIDs(ctx, room)
	if err != nil {
		log.Warnf("push member lookup failed: room_id = %d, err = %v", roomID, err)
		return
	}
	envelope, err := contract.EncodeEnvelope(contract.NewEnvelope(opcode, payload))
	if err != nil {
		log.Warnf("push envelope failed: room_id = %d, err = %v", roomID, err)
		return
	}
	s.pusher.PushRoom(ctx, room, opcode, envelope, uids)
}
