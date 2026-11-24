package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func redisZ(score float64, member string) redis.Z {
	return redis.Z{Score: score, Member: member}
}

func TestServiceLifecycle(t *testing.T) {
	service, _, _, _ := newTestService(t)
	if service.Name() != define.ServiceVoiceroom {
		t.Fatalf("name = %s", service.Name())
	}
	service.Init() // nil proxy: no-op registration
	service.Start()
	service.Close()
	service.Close() // idempotent
	service.Destroy()
}

func TestServiceDefaultsAndNilDeps(t *testing.T) {
	cfg := DefaultConfig()
	service := New(nil, nil, nil, nil, nil, nil, Config{})
	if service.cfg.SeatCount != cfg.SeatCount || service.cfg.ReconnectTimeout != cfg.ReconnectTimeout ||
		service.cfg.EmptyTimeout != cfg.EmptyTimeout || service.cfg.RoomTTL != cfg.RoomTTL ||
		service.cfg.TokenTTL != cfg.TokenTTL || service.cfg.ReapInterval != cfg.ReapInterval {
		t.Fatalf("cfg = %+v", service.cfg)
	}
	if service.ids == nil || service.pusher == nil {
		t.Fatal("defaults must fill ids and pusher")
	}
	// Nil redis client: the reaper loop must not start and must not panic.
	service.Start()
	service.Close()
	if _, err := service.ReapReconnects(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReapEmptyRooms(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The default log pusher records nothing and never panics.
	service.pusher.PushRoom(context.Background(), &Room{ID: 1}, 1912, nil, []int64{1})
}

func TestLogPusher(t *testing.T) {
	var pusher Pusher = logPusher{}
	pusher.PushRoom(context.Background(), &Room{ID: 7}, 1912, nil, nil)
}

func TestProxyPusherGuards(t *testing.T) {
	pusher := NewProxyPusher(nil)
	pusher.PushRoom(context.Background(), &Room{ID: 1}, 1912, nil, []int64{1})
	pusher.PushRoom(context.Background(), &Room{ID: 1}, 1912, nil, nil)
}

func TestSeatErrMapping(t *testing.T) {
	cases := []struct {
		code int64
		want int
	}{
		{luaErrRoomMissing, 180100},
		{luaErrRoomClosed, 180107},
		{luaErrNoPermission, 180101},
		{luaErrNotInHandQueue, 180101},
		{luaErrSeatOccupied, 180102},
		{luaErrSeatLocked, 180103},
		{luaErrAlreadySeated, 180102},
		{luaErrSeatNotFound, 180105},
		{luaErrBadSeatIndex, 180108},
		{luaErrNotRoomMember, 180105},
		{99, 900003},
	}
	for _, tc := range cases {
		err := seatErr(tc.code)
		if !errs.IsCode(err, tc.want) {
			t.Fatalf("seatErr(%d) = %v, want code %d", tc.code, err, tc.want)
		}
	}
}

func TestVerifyLiveKitJWTEdges(t *testing.T) {
	valid := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	valid = valid[len("Bearer "):]
	if !verifyLiveKitJWT("Bearer "+valid, "secret") {
		t.Fatal("valid token rejected")
	}
	if verifyLiveKitJWT(valid, "") {
		t.Fatal("empty secret must reject")
	}
	if verifyLiveKitJWT("Bearer a.b", "secret") {
		t.Fatal("two-part token must reject")
	}
	if verifyLiveKitJWT("Bearer a.b.c.d", "secret") {
		t.Fatal("four-part token must reject")
	}
	if verifyLiveKitJWT("Bearer !!!.@@@.###", "secret") {
		t.Fatal("non-base64 token must reject")
	}
	if verifyLiveKitJWT("no bearer", "secret") {
		t.Fatal("missing bearer prefix must reject")
	}
	// Payload that is valid base64 but invalid JSON: signature cannot match.
	if verifyLiveKitJWT("Bearer aGVsbG8.aGVsbG8.c2ln", "secret") {
		t.Fatal("invalid payload must reject")
	}
}

func TestWebhookComponentHTTP(t *testing.T) {
	service, _, _, _ := newTestService(t)
	service.cfg.WebhookSecret = "secret"
	webhook := NewWebhook(service, "")
	if webhook.Name() != "voiceroom-webhook" {
		t.Fatalf("name = %s", webhook.Name())
	}
	webhook.Init()
	webhook.Start() // empty addr: disabled path
	webhook.Close()
	webhook.Destroy()

	// Serve on an ephemeral port and exercise the real HTTP path.
	webhook = NewWebhook(service, "127.0.0.1:0")
	webhook.Start()
	time.Sleep(100 * time.Millisecond)
	if webhook.server == nil {
		t.Fatal("server not started")
	}

	// Unauthorized delivery.
	recorder := httptest.NewRecorder()
	bad := httptest.NewRequest(http.MethodPost, "/livekit/webhook", strings.NewReader(`{"event":"participant_left"}`))
	bad.Header.Set("Authorization", "Bearer nope")
	webhook.handle(recorder, bad)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
	}
	webhook.Close()
}

func TestWebhookRoomFinishedIdempotentAndRepoFallback(t *testing.T) {
	service, repo, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("room_finished", roomNameOf(roomID), ""), auth); err != nil {
		t.Fatal(err)
	}
	// Second room_finished: room already closed, must be a no-op.
	if err := service.HandleWebhook(ctx, webhookBody("room_finished", roomNameOf(roomID), ""), auth); err != nil {
		t.Fatal(err)
	}
	// A room that never had Redis state but is closed in PG: no-op.
	if err := repo.Create(ctx, &model.VoiceRoom{
		RoomID: roomID + 2, RoomType: 2, OwnerUID: 1, Title: "pg-only",
		SeatCount: 8, JoinSeatMode: 1, Status: model.VoiceRoomStatusClosed, CreatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.HandleWebhook(ctx, webhookBody("room_finished", roomNameOf(roomID+2), ""), auth); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshTokenClosedRoom(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	if _, err := service.JoinRoom(ctx, &pb.JoinRoomArgs{RoomId: roomID, Uid: 1002}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CloseRoom(ctx, &pb.CloseRoomArgs{RoomId: roomID, OperatorUid: 1001, Reason: CloseReasonOwner}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RefreshToken(ctx, &pb.RefreshTokenArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180107) {
		t.Fatalf("refresh closed err = %v", err)
	}
}

func TestHandUpNonMember(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 2)
	if _, err := service.HandUp(ctx, &pb.HandUpArgs{RoomId: roomID, Uid: 1002}); !errs.IsCode(err, 180105) {
		t.Fatalf("non-member hand up err = %v", err)
	}
}

func TestReapEmptyRoomsMalformedAndGone(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	// Malformed member: removed without error.
	if err := service.client.ZAdd(ctx, emptyTimeoutKey, redisZ(1, "not-a-number")).Err(); err != nil {
		t.Fatal(err)
	}
	// Room whose Redis state is gone: stale keys cleaned, entry removed.
	if err := service.client.ZAdd(ctx, emptyTimeoutKey, redisZ(1, "12345")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := service.client.HSet(ctx, seatsKey(12345), "0", "0:0").Err(); err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	if count, err := service.ReapEmptyRooms(ctx); err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("count = %d", count)
	}
	if exists, _ := service.client.Exists(ctx, seatsKey(12345)).Result(); exists > 0 {
		t.Fatal("stale seat key not cleaned")
	}
}

func TestReapReconnectsMalformed(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	service.now = func() time.Time { return time.Now().Add(time.Hour) }
	if err := service.client.ZAdd(ctx, reconnectTimeoutKey, redisZ(1, "garbage")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := service.client.ZAdd(ctx, reconnectTimeoutKey, redisZ(1, "1:notint:2")).Err(); err != nil {
		t.Fatal(err)
	}
	if count, err := service.ReapReconnects(ctx); err != nil || count != 0 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
}

func TestTakeSeatClosedRoom(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.CloseRoom(ctx, &pb.CloseRoomArgs{RoomId: roomID, OperatorUid: 1001, Reason: CloseReasonOwner}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); !errs.IsCode(err, 180107) {
		t.Fatalf("take closed err = %v", err)
	}
}

func TestMemberUIDsAndRoomInfo(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	room, err := service.loadRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	uids, err := service.memberUIDs(ctx, room)
	if err != nil || len(uids) != 2 {
		t.Fatalf("uids = %v, err = %v", uids, err)
	}
	info := roomInfo(room)
	if info.GetRoomId() != roomID || info.GetSeatCount() != 8 || info.GetStatus() != 1 {
		t.Fatalf("info = %+v", info)
	}
}

func TestSeatUIDMissingSeat(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	// The owner sits on seat 0 from the start.
	if uid := service.seatUID(ctx, roomID, 0); uid != 1001 {
		t.Fatalf("owner seat uid = %d, want 1001", uid)
	}
	// An empty seat and an out-of-range index return 0.
	if uid := service.seatUID(ctx, roomID, 7); uid != 0 {
		t.Fatalf("empty seat uid = %d, want 0", uid)
	}
	if uid := service.seatUID(ctx, roomID, 8); uid != 0 {
		t.Fatalf("out-of-range uid = %d, want 0", uid)
	}
}
