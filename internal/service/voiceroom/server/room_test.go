package server

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func newTestService(t *testing.T) (*Service, *fakeRepo, *fakeProvider, *fakePusher) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	repo := newFakeRepo()
	media := newFakeProvider()
	pusher := &fakePusher{}
	cfg := DefaultConfig()
	cfg.RoomTTL = time.Hour
	service := New(nil, repo, client, media, pusher, snowflake.NewWithWorker(7), cfg)
	return service, repo, media, pusher
}

func createRoom(t *testing.T, service *Service, owner int64, roomType int32, joinMode int32) int64 {
	t.Helper()
	reply, err := service.CreateRoom(context.Background(), &pb.CreateRoomArgs{
		OwnerUid: owner, RoomType: roomType, GroupId: 100, Title: "test room",
		SeatCount: 8, JoinSeatMode: joinMode,
	})
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	if reply.GetRoom().GetRoomId() <= 0 || reply.GetToken() == "" {
		t.Fatalf("reply = %+v", reply)
	}
	return reply.GetRoom().GetRoomId()
}

func TestCreateRoomPersistsAndSeatsOwner(t *testing.T) {
	service, repo, media, _ := newTestService(t)
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))
	if roomID <= 0 {
		t.Fatalf("room id = %d", roomID)
	}
	stored, err := repo.Get(context.Background(), roomID)
	if err != nil || stored.Status != model.VoiceRoomStatusLive || stored.OwnerUID != 1001 {
		t.Fatalf("stored = %+v, err = %v", stored, err)
	}
	values, err := service.client.HGetAll(context.Background(), seatsKey(roomID)).Result()
	if err != nil || len(values) != 8 {
		t.Fatalf("seats = %v, err = %v", values, err)
	}
	if values["0"] != "1001:1" {
		t.Fatalf("owner seat = %q", values["0"])
	}
	if !media.permit(roomNameOf(roomID), 1001) {
		t.Fatal("owner publish permission not granted")
	}
	online, err := service.client.HGet(context.Background(), roomKey(roomID), "online").Int64()
	if err != nil || online != 1 {
		t.Fatalf("online = %d, err = %v", online, err)
	}
}

func TestCreateRoomValidation(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	cases := []struct {
		name string
		args *pb.CreateRoomArgs
	}{
		{"empty title", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 1, GroupId: 1, Title: "  "}},
		{"bad type", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 9, Title: "x"}},
		{"group without group id", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 1, Title: "x"}},
		{"password room without password", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 3, Title: "x"}},
		{"too many seats", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 2, Title: "x", SeatCount: 13}},
		{"bad join mode", &pb.CreateRoomArgs{OwnerUid: 1, RoomType: 2, Title: "x", JoinSeatMode: 3}},
		{"missing owner", &pb.CreateRoomArgs{OwnerUid: 0, RoomType: 2, Title: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := service.CreateRoom(ctx, tc.args); !errs.IsCode(err, 180108) {
				t.Fatalf("err = %v, want code 180108", err)
			}
		})
	}
}

func TestJoinRoom(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))

	reply, err := service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 1002})
	if err != nil {
		t.Fatal(err)
	}
	if reply.GetToken() == "" || len(reply.GetSeats()) != 8 {
		t.Fatalf("reply = %+v", reply)
	}
	online, err := service.client.HGet(ctx, roomKey(roomID), "online").Int64()
	if err != nil || online != 2 {
		t.Fatalf("online = %d, err = %v", online, err)
	}
	if _, err = service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID + 999, Uid: 1002}); !errs.IsCode(err, 180100) {
		t.Fatalf("missing room err = %v", err)
	}
}

func TestJoinPasswordRoom(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	reply, err := service.CreateRoom(ctx, &pb.CreateRoomArgs{
		OwnerUid: 1001, RoomType: int32(model.VoiceRoomTypePassword), Title: "secret",
		Password: "p@ss", SeatCount: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	roomID := reply.GetRoom().GetRoomId()
	if _, err = service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 2, Password: "wrong"}); !errs.IsCode(err, 180109) {
		t.Fatalf("wrong password err = %v", err)
	}
	if _, err = service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 2, Password: "p@ss"}); err != nil {
		t.Fatalf("join with password: %v", err)
	}
}

func TestRefreshTokenAndGetRoom(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))
	if _, err := service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 1002}); err != nil {
		t.Fatal(err)
	}
	token, err := service.RefreshToken(ctx, &pb.RefreshTokenArgs{RoomId: roomID, Uid: 1002})
	if err != nil || token.GetToken() == "" {
		t.Fatalf("token = %+v, err = %v", token, err)
	}
	if _, err = service.RefreshToken(ctx, &pb.RefreshTokenArgs{RoomId: roomID, Uid: 1003}); !errs.IsCode(err, 180105) {
		t.Fatalf("non-member refresh err = %v", err)
	}
	room, err := service.GetRoom(ctx, &pb.GetRoomArgs{RoomId: roomID})
	if err != nil || room.GetTitle() != "test room" || room.GetOwnerUid() != 1001 {
		t.Fatalf("room = %+v, err = %v", room, err)
	}
	if _, err = service.GetRoom(ctx, &pb.GetRoomArgs{RoomId: roomID + 1}); !errs.IsCode(err, 180100) {
		t.Fatalf("missing room err = %v", err)
	}
}

func TestListRooms(t *testing.T) {
	service, repo, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))
	// A second public room, listed through the repo fallback path.
	if err := repo.Create(ctx, &model.VoiceRoom{
		RoomID: roomID + 1, RoomType: model.VoiceRoomTypePublic, GroupID: 0, OwnerUID: 2001,
		Title: "other", SeatCount: 8, JoinSeatMode: 1, Status: model.VoiceRoomStatusLive, CreatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	groupRooms, err := service.ListRooms(ctx, &pb.ListRoomsArgs{GroupId: 100})
	if err != nil || len(groupRooms.GetRooms()) != 1 {
		t.Fatalf("group rooms = %+v, err = %v", groupRooms, err)
	}
	publicRooms, err := service.ListRooms(ctx, &pb.ListRoomsArgs{Limit: 10})
	if err != nil || len(publicRooms.GetRooms()) != 2 {
		t.Fatalf("public rooms = %+v, err = %v", publicRooms, err)
	}
}

func TestCloseRoom(t *testing.T) {
	service, repo, media, pusher := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))
	if _, err := service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 1002}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CloseRoom(ctx, &pb.CloseRoomArgs{RoomId: roomID, OperatorUid: 1002, Reason: CloseReasonOwner}); !errs.IsCode(err, 180101) {
		t.Fatalf("non-owner close err = %v", err)
	}
	if _, err := service.CloseRoom(ctx, &pb.CloseRoomArgs{RoomId: roomID, OperatorUid: 1001, Reason: CloseReasonOwner}); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.Get(ctx, roomID)
	if err != nil || stored.Status != model.VoiceRoomStatusClosed || stored.ClosedAt <= 0 {
		t.Fatalf("stored = %+v, err = %v", stored, err)
	}
	if !media.roomGone(roomNameOf(roomID)) {
		t.Fatal("livekit room not deleted")
	}
	if exists, _ := service.client.Exists(ctx, roomKey(roomID), seatsKey(roomID)).Result(); exists > 0 {
		t.Fatal("redis room keys not deleted")
	}
	if pusher.count(1913) != 1 {
		t.Fatalf("1913 pushes = %d", pusher.count(1913))
	}
	// Closing again is idempotent: the PG row is already CLOSED.
	if _, err = service.CloseRoom(ctx, &pb.CloseRoomArgs{RoomId: roomID, OperatorUid: 1001, Reason: CloseReasonOwner}); err != nil {
		t.Fatalf("idempotent close err = %v", err)
	}
	// A live room is joinable again after a closed-room lookup path.
	if _, err = service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180107) {
		t.Fatalf("join closed err = %v", err)
	}
}

func TestLoadRoomRehydratesFromRepo(t *testing.T) {
	service, repo, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, int32(model.VoiceRoomTypePublic), int32(model.VoiceRoomJoinFree))
	if err := service.client.Del(ctx, roomKey(roomID), seatsKey(roomID)).Err(); err != nil {
		t.Fatal(err)
	}
	room, err := service.loadRoom(ctx, roomID)
	if err != nil || room.ID != roomID || room.Title != "test room" {
		t.Fatalf("room = %+v, err = %v", room, err)
	}
	values, err := service.client.HGetAll(ctx, seatsKey(roomID)).Result()
	if err != nil || len(values) != 8 {
		t.Fatalf("rehydrated seats = %v, err = %v", values, err)
	}
	_ = repo
}
