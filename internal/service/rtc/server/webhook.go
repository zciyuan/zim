package rtc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
)

// livekitWebhook is the subset of LiveKit's webhook body ZIM needs.
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

// HandleWebhook verifies a LiveKit webhook JWT (HS256, apiSecret) and applies
// participant/room events to the call state machine. It is idempotent per event
// id, so LiveKit retries cannot double-leave a participant.
func (s *Service) HandleWebhook(ctx context.Context, body []byte, authHeader, secret string,
	deleteRoom func(context.Context, string) error) error {
	if secret == "" || !verifyLiveKitJWT(authHeader, secret) {
		return ErrForbidden
	}
	event := &livekitWebhook{}
	if err := json.Unmarshal(body, event); err != nil {
		return ErrInvalidCall
	}
	callID := strings.TrimPrefix(event.Room.Name, "zim-call-")
	if callID == "" || callID == event.Room.Name {
		return nil
	}
	if event.ID != "" {
		ok, err := s.client.SetNX(ctx, "zim:rtc:webhook:"+event.ID, 1, time.Hour).Result()
		if err != nil || !ok {
			return err
		}
	}
	uid := parseIdentity(event.Participant.Identity)
	call, err := s.Get(ctx, callID)
	if err != nil {
		return nil
	}
	switch event.Event {
	case "participant_joined":
		if uid > 0 {
			_ = s.MarkLeg(ctx, callID, uid, "joined")
		}
	case "participant_left":
		if uid <= 0 {
			return nil
		}
		if call.TargetType > 1 {
			_, _, ended, leaveErr := s.LeaveGroup(ctx, callID, uid)
			err = leaveErr
			if ended && deleteRoom != nil {
				_ = deleteRoom(ctx, call.Room)
			}
		} else {
			err = s.Hangup(ctx, callID, uid)
		}
	case "room_finished":
		if !isTerminal(call.State) {
			err = s.endCall(ctx, call)
		}
	}
	return err
}

func parseIdentity(identity string) int64 {
	if index := strings.IndexByte(identity, ':'); index >= 0 {
		identity = identity[:index]
	}
	uid, _ := strconv.ParseInt(identity, 10, 64)
	return uid
}

// verifyLiveKitJWT checks header.payload against HS256(apiSecret). LiveKit
// signs webhooks with the API secret; expiry is checked when present.
func verifyLiveKitJWT(authHeader, secret string) bool {
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
