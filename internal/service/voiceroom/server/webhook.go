package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// livekitWebhook is the subset of LiveKit's webhook body the voice room
// service needs.
type livekitWebhook struct {
	Event string `json:"event"`
	ID    string `json:"id"`
	Room  struct {
		Name string `json:"name"`
	} `json:"room"`
	Participant struct {
		Identity string `json:"identity"`
	} `json:"participant"`
}

// HandleWebhook verifies the LiveKit signature (HS256 JWT, api secret) and
// feeds participant/room events into the seat machine. It is idempotent per
// event id so LiveKit retries cannot double-apply a leave.
func (s *Service) HandleWebhook(ctx context.Context, body []byte, authHeader string) error {
	if !verifyLiveKitJWT(authHeader, s.cfg.WebhookSecret) {
		return errs.ErrUnauthorized
	}
	event := &livekitWebhook{}
	if err := json.Unmarshal(body, event); err != nil {
		return errs.ErrVoiceRoomParamInvalid.WithCause(err)
	}
	roomID, ok := parseRoomName(event.Room.Name)
	if !ok {
		return nil
	}
	uid := parseIdentity(event.Participant.Identity)
	if event.ID != "" {
		claimed, err := s.client.SetNX(ctx, "zim:vr:webhook:"+event.ID, 1, time.Hour).Result()
		if err != nil {
			log.Warnf("voiceroom webhook dedupe failed: event = %s, err = %v", event.ID, err)
		} else if !claimed {
			return nil
		}
	}
	switch event.Event {
	case "participant_joined":
		return s.onParticipantJoined(ctx, roomID, uid)
	case "participant_left":
		return s.onParticipantLeft(ctx, roomID, uid)
	case "room_finished":
		return s.onRoomFinished(ctx, roomID)
	default:
		return nil
	}
}

// onParticipantJoined restores a reconnecting seat and clears both timeouts.
func (s *Service) onParticipantJoined(ctx context.Context, roomID, uid int64) error {
	room, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return err
	}
	seatIndex, runErr := restoreSeatScript.Run(ctx, s.client, []string{seatsKey(room.ID)}, uid).Int64()
	if runErr != nil {
		return errs.ErrInternal.WithCause(runErr)
	}
	if _, err = s.addMember(ctx, room, uid); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	if seatIndex >= 0 {
		_ = s.client.ZRem(ctx, reconnectTimeoutKey, reconnectMember(room.ID, seatIndex, uid)).Err()
		if err = s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), uid, true); err != nil {
			log.Warnf("re-grant publish on rejoin failed: uid = %d, room_id = %d, err = %v", uid, room.ID, err)
		}
	}
	_ = s.client.ZRem(ctx, emptyTimeoutKey, strconv.FormatInt(room.ID, 10)).Err()
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return nil
}

// onParticipantLeft moves the member's seat to RECONNECTING, arms the
// reconnect timer and the empty timer when the room empties.
func (s *Service) onParticipantLeft(ctx context.Context, roomID, uid int64) error {
	room, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return err
	}
	seatIndex, runErr := markReconnectScript.Run(ctx, s.client, []string{seatsKey(room.ID)}, uid).Int64()
	if runErr != nil {
		return errs.ErrInternal.WithCause(runErr)
	}
	online, err := s.removeMember(ctx, room, uid)
	if err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	if seatIndex >= 0 {
		deadline := s.now().UnixMilli() + s.cfg.ReconnectTimeout.Milliseconds()
		_ = s.client.ZAdd(ctx, reconnectTimeoutKey, redis.Z{
			Score: float64(deadline), Member: reconnectMember(room.ID, seatIndex, uid),
		}).Err()
	}
	if online == 0 {
		deadline := s.now().UnixMilli() + s.cfg.EmptyTimeout.Milliseconds()
		_ = s.client.ZAdd(ctx, emptyTimeoutKey, redis.Z{
			Score: float64(deadline), Member: strconv.FormatInt(room.ID, 10),
		}).Err()
	}
	s.touch(ctx, room.ID)
	s.pushSeats(ctx, room.ID)
	return nil
}

// onRoomFinished handles LiveKit's own room deletion (its empty timeout), so
// the room is closed even when the empty-timer zset entry was lost.
func (s *Service) onRoomFinished(ctx context.Context, roomID int64) error {
	room, err := s.loadRoom(ctx, roomID)
	if err != nil {
		// An already-closed room is a no-op for room_finished retries.
		if errs.IsCode(err, errs.ErrVoiceRoomClosed.Code().Code()) {
			return nil
		}
		if errs.IsCode(err, errs.ErrVoiceRoomNotExist.Code().Code()) {
			stored, getErr := s.repo.Get(ctx, roomID)
			if getErr == nil && stored.Status == model.VoiceRoomStatusClosed {
				return nil
			}
		}
		return err
	}
	if !room.Live() {
		return nil
	}
	if err = s.repo.UpdateStatus(ctx, room.ID, model.VoiceRoomStatusClosed, s.now().UnixMilli()); err != nil {
		log.Warnf("close finished room failed: room_id = %d, err = %v", room.ID, err)
	}
	s.deleteRoomKeys(ctx, room.ID)
	_ = s.client.ZRem(ctx, emptyTimeoutKey, strconv.FormatInt(room.ID, 10)).Err()
	s.pushClosed(ctx, room, CloseReasonEmptyTimeout, nil)
	return nil
}

func parseIdentity(identity string) int64 {
	if index := strings.IndexByte(identity, ':'); index >= 0 {
		identity = identity[:index]
	}
	uid, _ := strconv.ParseInt(identity, 10, 64)
	return uid
}

// verifyLiveKitJWT checks header.payload against HS256(secret); LiveKit
// signs webhooks with the API secret and expiry is enforced when present.
func verifyLiveKitJWT(authHeader, secret string) bool {
	if secret == "" {
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(parts[2])) {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) == nil && claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return false
	}
	return true
}
