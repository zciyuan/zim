package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func signWebhook(t *testing.T, secret string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(unsigned))
	return "Bearer " + unsigned + "." + enc.EncodeToString(mac.Sum(nil))
}

var webhookSeq atomic.Int64

// webhookBody builds a webhook payload with a unique event id so the
// dedupe guard does not swallow successive deliveries.
func webhookBody(event, room, identity string) []byte {
	id := "evt-" + strconv.FormatInt(webhookSeq.Add(1), 10)
	body, _ := json.Marshal(map[string]any{
		"event": event, "id": id,
		"room":        map[string]any{"name": room},
		"participant": map[string]any{"identity": identity},
	})
	return body
}

func TestWebhookDisconnectKeepsSeatReconnecting(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}

	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	service.cfg.WebhookSecret = "secret"
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "1002:5" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	online, err := service.client.HGet(ctx, roomKey(roomID), "online").Int64()
	if err != nil || online != 1 {
		t.Fatalf("online = %d, err = %v", online, err)
	}
	entry := reconnectMember(roomID, 1, 1002)
	score, err := service.client.ZScore(ctx, reconnectTimeoutKey, entry).Result()
	if err != nil || score <= 0 {
		t.Fatalf("reconnect entry score = %v, err = %v", score, err)
	}
}

func TestWebhookRejoinRestoresSeat(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	// The user rejoins the media plane before the grace expires.
	if err := service.HandleWebhook(ctx, webhookBody("participant_joined", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "1002:1" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	if exists, _ := service.client.ZScore(ctx, reconnectTimeoutKey, reconnectMember(roomID, 1, 1002)).Result(); exists > 0 {
		t.Fatal("reconnect entry not cleared on rejoin")
	}
	if !media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not re-granted on rejoin")
	}
}

func TestWebhookReconnectTimeoutReapsSeat(t *testing.T) {
	service, _, media, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	if _, err := service.TakeSeat(ctx, &pb.TakeSeatArgs{RoomId: roomID, Uid: 1002, SeatIndex: 1}); err != nil {
		t.Fatal(err)
	}
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}

	fixed := time.Now()
	service.now = func() time.Time { return fixed }
	if count, err := service.ReapReconnects(ctx); err != nil || count != 0 {
		t.Fatalf("early reap count = %d, err = %v", count, err)
	}
	service.now = func() time.Time { return fixed.Add(31 * time.Second) }
	if count, err := service.ReapReconnects(ctx); err != nil || count != 1 {
		t.Fatalf("late reap count = %d, err = %v", count, err)
	}
	seat, err := service.client.HGet(ctx, seatsKey(roomID), "1").Result()
	if err != nil || seat != "0:0" {
		t.Fatalf("seat = %q, err = %v", seat, err)
	}
	if media.permit(roomNameOf(roomID), 1002) {
		t.Fatal("publish permission not revoked after reconnect timeout")
	}
	// The grace entry is consumed exactly once.
	if count, err := service.ReapReconnects(ctx); err != nil || count != 0 {
		t.Fatalf("second reap count = %d, err = %v", count, err)
	}
}

func TestWebhookEmptyRoomGC(t *testing.T) {
	service, repo, _, pusher := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})

	fixed := time.Now()
	service.now = func() time.Time { return fixed }
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	// Owner also leaves: the room is empty now.
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1001"), auth); err != nil {
		t.Fatal(err)
	}
	if count, err := service.ReapEmptyRooms(ctx); err != nil || count != 0 {
		t.Fatalf("early empty reap count = %d, err = %v", count, err)
	}
	service.now = func() time.Time { return fixed.Add(301 * time.Second) }
	if count, err := service.ReapEmptyRooms(ctx); err != nil || count != 1 {
		t.Fatalf("late empty reap count = %d, err = %v", count, err)
	}
	stored, err := repo.Get(ctx, roomID)
	if err != nil || stored.Status != model.VoiceRoomStatusClosed {
		t.Fatalf("stored = %+v, err = %v", stored, err)
	}
	if pusher.count(1913) != 1 {
		t.Fatalf("1913 pushes = %d", pusher.count(1913))
	}
	if exists, _ := service.client.Exists(ctx, roomKey(roomID)).Result(); exists > 0 {
		t.Fatal("room keys not deleted by empty GC")
	}
}

func TestWebhookRejoinCancelsEmptyTimeout(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	fixed := time.Now()
	service.now = func() time.Time { return fixed }

	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1001"), auth); err != nil {
		t.Fatal(err)
	}
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	// 1002 rejoins before the empty timeout fires.
	if err := service.HandleWebhook(ctx, webhookBody("participant_joined", roomNameOf(roomID), "1002"), auth); err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return fixed.Add(301 * time.Second) }
	if count, err := service.ReapEmptyRooms(ctx); err != nil || count != 0 {
		t.Fatalf("reap count = %d, err = %v", count, err)
	}
}

func TestWebhookRoomFinishedClosesRoom(t *testing.T) {
	service, repo, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("room_finished", roomNameOf(roomID), ""), auth); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.Get(ctx, roomID)
	if err != nil || stored.Status != model.VoiceRoomStatusClosed {
		t.Fatalf("stored = %+v, err = %v", stored, err)
	}
}

func TestWebhookRejectsBadSignatureAndForeignRooms(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "other", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", "zim-vr-1", "1"), auth); !errs.IsCode(err, 100002) {
		t.Fatalf("bad signature err = %v", err)
	}
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", "", "1"), "Bearer x"); !errs.IsCode(err, 100002) {
		t.Fatalf("empty secret err = %v", err)
	}
	// A call-room event is not a voice-room event: ignored without error.
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", "zim-call-9", "1"), signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})); err != nil {
		t.Fatalf("foreign room err = %v", err)
	}
	// Malformed body.
	if err := service.HandleWebhook(ctx, []byte("{"), signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})); !errs.IsCode(err, 180108) {
		t.Fatalf("malformed body err = %v", err)
	}
	// Expired JWT.
	expired := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})
	if err := service.HandleWebhook(ctx, webhookBody("participant_left", "zim-vr-1", "1"), expired); !errs.IsCode(err, 100002) {
		t.Fatalf("expired token err = %v", err)
	}
}

func TestWebhookDedupe(t *testing.T) {
	service, _, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, service, 1001, 2, 1)
	joinMember(t, service, roomID, 1002)
	service.cfg.WebhookSecret = "secret"
	auth := signWebhook(t, "secret", map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	body := webhookBody("participant_left", roomNameOf(roomID), "1002")
	if err := service.HandleWebhook(ctx, body, auth); err != nil {
		t.Fatal(err)
	}
	// Second delivery of the same event must not double-remove presence.
	online, err := service.client.HGet(ctx, roomKey(roomID), "online").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if err = service.HandleWebhook(ctx, body, auth); err != nil {
		t.Fatal(err)
	}
	onlineAgain, err := service.client.HGet(ctx, roomKey(roomID), "online").Int64()
	if err != nil || onlineAgain != online {
		t.Fatalf("online = %d -> %d, err = %v", online, onlineAgain, err)
	}
}

func TestParseRoomName(t *testing.T) {
	cases := []struct {
		name string
		want int64
		ok   bool
	}{
		{"zim-vr-123", 123, true},
		{"zim-vr-0", 0, false},
		{"zim-call-1", 0, false},
		{"zim-vr-abc", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseRoomName(tc.name)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("parseRoomName(%q) = %d, %v; want %d, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestParseIdentityAndSeatValue(t *testing.T) {
	if uid := parseIdentity("10001:web"); uid != 10001 {
		t.Fatalf("identity = %d", uid)
	}
	if uid := parseIdentity("abc"); uid != 0 {
		t.Fatalf("identity = %d", uid)
	}
	uid, status := parseSeatValue("1002:3")
	if uid != 1002 || status != 3 {
		t.Fatalf("seat value = %d:%d", uid, status)
	}
	uid, status = parseSeatValue("0:0")
	if uid != 0 || status != 0 {
		t.Fatalf("seat value = %d:%d", uid, status)
	}
	uid, status = parseSeatValue("garbage")
	if uid != 0 || status != 0 {
		t.Fatalf("seat value = %d:%d", uid, status)
	}
}

func TestPasswordHashRoundTrip(t *testing.T) {
	hash, err := hashPassword("p@ss")
	if err != nil {
		t.Fatal(err)
	}
	if !verifyPassword(hash, "p@ss") || verifyPassword(hash, "wrong") || verifyPassword("", "") {
		t.Fatal("password verify mismatch")
	}
	if verifyPassword("not-a-hash", "x") {
		t.Fatal("malformed hash must not verify")
	}
}

func TestRoomNameRoundTrip(t *testing.T) {
	id := int64(180100202503010001)
	got, ok := parseRoomName(roomNameOf(id))
	if !ok || got != id {
		t.Fatalf("round trip = %d, %v", got, ok)
	}
}
