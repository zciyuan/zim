package server

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func joinMember(t *testing.T, service *Service, roomID, uid int64) {
	t.Helper()
	if _, err := service.JoinRoom(context.Background(), &pb.JoinRoomArgs{RoomId: roomID, Uid: uid}); err != nil {
		t.Fatalf("JoinRoom(%d): %v", uid, err)
	}
}

func TestTakeSeatSyncsLiveKitPermission(t *testing.T) {
	service, _, media, pusher := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)

	reply, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1})
	if err != nil || !reply.GetOk() {
		t.Fatalf("reply = %+v, err = %v", reply, err)
	}
	if !media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not granted on take seat")
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "1002:1" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	if pusher.count(1912) != 1 {
		t.Fatalf("1912 pushes = %d", pusher.count(1912))
	}
}

func TestTakeSeatErrors(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)

	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 0}); !errs.IsCode(err, 180102) {
		t.Fatalf("occupied seat err = %v", err)
	}
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 8}); !errs.IsCode(err, 180108) {
		t.Fatalf("bad index err = %v", err)
	}
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1003, SeatIndex: 1}); !errs.IsCode(err, 180105) {
		t.Fatalf("non-member err = %v", err)
	}
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID + 5, Uid: 1002, SeatIndex: 1}); !errs.IsCode(err, 180100) {
		t.Fatalf("missing room err = %v", err)
	}
}

func TestLeaveSeatRevokesPermission(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LeaveSeat(ctx, &pb.LeaveSeatArgs{RoomId: roomID, Uid: 1002}); err != nil {
		t.Fatal(err)
	}
	if media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not revoked on leave seat")
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "0:0" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	// Leaving again: not seated.
	if _, err = service.LeaveSeat(ctx, &pb.LeaveSeatArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180105) {
		t.Fatalf("double leave err = %v", err)
	}
}

func TestForceMuteSeat(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ForceMuteSeat(ctx, &pb.ForceMuteSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 1, Muted: true}); err != nil {
		t.Fatal(err)
	}
	if media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not revoked on force mute")
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "1002:3" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	if _, err = service.ForceMuteSeat(ctx, &pb.ForceMuteSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 1, Muted: false}); err != nil {
		t.Fatal(err)
	}
	if !media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not restored on unmute")
	}
	// Non-owner cannot force mute.
	if _, err = service.ForceMuteSeat(ctx, &pb.ForceMuteSeatArgs{RoomId: roomID, OperatorUid: 1002, SeatIndex: 1, Muted: true}); !errs.IsCode(err, 180101) {
		t.Fatalf("non-owner mute err = %v", err)
	}
	// Empty seat cannot be muted.
	if _, err = service.ForceMuteSeat(ctx, &pb.ForceMuteSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 2, Muted: true}); !errs.IsCode(err, 180105) {
		t.Fatalf("empty seat mute err = %v", err)
	}
}

func TestLockSeat(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)

	if _, err := service.LockSeat(ctx, &pb.LockSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 1, Locked: true}); err != nil {
		t.Fatal(err)
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "0:4" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	if _, err = service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); !errs.IsCode(err, 180103) {
		t.Fatalf("locked seat err = %v", err)
	}
	if _, err = service.LockSeat(ctx, &pb.LockSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 1, Locked: false}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatalf("take after unlock: %v", err)
	}
	// Locking an occupied seat fails.
	if _, err = service.LockSeat(ctx, &pb.LockSeatArgs{RoomId: roomID, OperatorUid: 1001, SeatIndex: 1, Locked: true}); !errs.IsCode(err, 180102) {
		t.Fatalf("lock occupied err = %v", err)
	}
	// Non-owner cannot lock.
	if _, err = service.LockSeat(ctx, &pb.LockSeatArgs{RoomId: roomID, OperatorUid: 1002, SeatIndex: 2, Locked: true}); !errs.IsCode(err, 180101) {
		t.Fatalf("non-owner lock err = %v", err)
	}
}

func TestKickSeat(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.KickSeat(ctx, &pb.KickSeatArgs{RoomId: roomID, OperatorUid: 1001, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not revoked on kick")
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "0:0" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	exists, err := service.client.HExists(ctx, roomKey(roomID), "m:1002").Result()
	if err != nil || exists {
		t.Fatalf("kicked member presence exists = %v, err = %v", exists, err)
	}
	// Kicking a seat whose holder differs fails.
	if _, err = service.KickSeat(ctx, &pb.KickSeatArgs{RoomId: roomID, OperatorUid: 1001, Uid: 9999, SeatIndex: 0}); !errs.IsCode(err, 180102) {
		t.Fatalf("wrong target kick err = %v", err)
	}
}

func TestHandUpAndApprove(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, int32(model.VoiceRoomJoinHandUp))
	joinMember(t, service, roomID, 1002)

	if _, err := service.HandUp(ctx, &pb.HandUpArgs{RoomId: roomID, Uid: 1002}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.HandUp(ctx, &pb.HandUpArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180106) {
		t.Fatalf("double hand up err = %v", err)
	}
	// Direct take-seat is denied in hand-up mode.
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); !errs.IsCode(err, 180101) {
		t.Fatalf("direct take err = %v", err)
	}
	// Non-owner cannot approve.
	if _, err := service.ApproveHandUp(ctx, &pb.ApproveHandUpArgs{RoomId: roomID, OperatorUid: 1002, Uid: 1002, SeatIndex: 1, Accept: true}); !errs.IsCode(err, 180101) {
		t.Fatalf("non-owner approve err = %v", err)
	}
	// Approving a uid not in the queue fails.
	if _, err := service.ApproveHandUp(ctx, &pb.ApproveHandUpArgs{RoomId: roomID, OperatorUid: 1001, Uid: 1003, SeatIndex: 1, Accept: true}); !errs.IsCode(err, 180101) {
		t.Fatalf("unknown approve err = %v", err)
	}
	reply, err := service.ApproveHandUp(ctx, &pb.ApproveHandUpArgs{RoomId: roomID, OperatorUid: 1001, Uid: 1002, SeatIndex: 1, Accept: true})
	if err != nil || !reply.GetOk() {
		t.Fatalf("approve reply = %+v, err = %v", reply, err)
	}
	if !media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not granted on approve")
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "1002:1" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
}

func TestHandUpFreeModeDenied(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, int32(model.VoiceRoomJoinFree))
	joinMember(t, service, roomID, 1002)
	if _, err := service.HandUp(ctx, &pb.HandUpArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180101) {
		t.Fatalf("free mode hand up err = %v", err)
	}
}

func TestInviteToSeat(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)

	if _, err := service.InviteToSeat(ctx, &pb.InviteToSeatArgs{RoomId: roomID, OperatorUid: 1001, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if !media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not granted on invite")
	}
	if _, err := service.InviteToSeat(ctx, &pb.InviteToSeatArgs{RoomId: roomID, OperatorUid: 1002, Uid: 1002, SeatIndex: 2}); !errs.IsCode(err, 180101) {
		t.Fatalf("non-owner invite err = %v", err)
	}
	if _, err := service.InviteToSeat(ctx, &pb.InviteToSeatArgs{RoomId: roomID, OperatorUid: 1001, Uid: 1003, SeatIndex: 2}); !errs.IsCode(err, 180105) {
		t.Fatalf("non-member invite err = %v", err)
	}
}
