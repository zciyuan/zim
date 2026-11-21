package rtc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
)

// LiveKitConfig configures the LiveKit provider.
type LiveKitConfig struct {
	URL       string
	APIKey    string
	APISecret string
	TokenTTL  time.Duration
}

// LiveKit is a minimal LiveKit server-API and token provider.
type LiveKit struct {
	base   string
	key    string
	secret []byte
	ttl    time.Duration
	client *http.Client
	events chan RoomEvent
}

// NewLiveKit returns a LiveKit provider.
func NewLiveKit(cfg LiveKitConfig) *LiveKit {
	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = time.Hour
	}
	return &LiveKit{
		base: strings.TrimRight(cfg.URL, "/"), key: cfg.APIKey,
		secret: []byte(cfg.APISecret), ttl: cfg.TokenTTL,
		client: &http.Client{Timeout: 5 * time.Second}, events: make(chan RoomEvent, 64),
	}
}

// CreateRoom creates a LiveKit room; an existing room is treated as success.
func (l *LiveKit) CreateRoom(ctx context.Context, req RoomReq) (*Room, error) {
	body, _ := json.Marshal(map[string]any{"name": req.Room, "empty_timeout": 300})
	status, raw, err := l.call(ctx, "livekit.RoomService/CreateRoom", body, "")
	if err != nil {
		return nil, err
	}
	if status == http.StatusConflict || strings.Contains(string(raw), "already exists") {
		return &Room{ID: req.Room, Name: req.Room, Node: l.base}, nil
	}
	if status >= 300 {
		return nil, fmt.Errorf("livekit create room: status %d: %s", status, raw)
	}
	var response struct {
		SID  string `json:"sid"`
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw, &response)
	return &Room{ID: response.SID, Name: response.Name, Node: l.base}, nil
}

// IssueToken signs a LiveKit join token.
func (l *LiveKit) IssueToken(_ context.Context, room string, uid int64, role Role) (string, error) {
	now := time.Now()
	claims := map[string]any{
		"iss": l.key, "sub": strconv.FormatInt(uid, 10),
		"nbf": now.Add(-10 * time.Second).Unix(), "exp": now.Add(l.ttl).Unix(),
		"video": map[string]any{
			// Every joined role may publish. A participant token that cannot publish
			// joins the room silently with no audio and no video, which is exactly
			// what a call looks like when the grant is too narrow.
			"room": room, "roomJoin": true, "canPublish": role >= RoleParticipant,
			"canSubscribe": true, "canPublishData": true,
		},
	}
	return signJWT(l.key, l.secret, claims)
}

// RemoveParticipant removes a participant from a room.
func (l *LiveKit) RemoveParticipant(ctx context.Context, room string, uid int64) error {
	body, _ := json.Marshal(map[string]any{"room": room, "identity": strconv.FormatInt(uid, 10)})
	status, raw, err := l.call(ctx, "livekit.RoomService/RemoveParticipant", body, room)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status >= 300 {
		return fmt.Errorf("livekit remove participant: status %d: %s", status, raw)
	}
	return nil
}

// DeleteRoom deletes a room; a missing room is treated as success so terminal
// cleanup stays idempotent.
func (l *LiveKit) DeleteRoom(ctx context.Context, room string) error {
	body, _ := json.Marshal(map[string]any{"room": room})
	status, raw, err := l.call(ctx, "livekit.RoomService/DeleteRoom", body, room)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status >= 300 {
		return fmt.Errorf("livekit delete room: status %d: %s", status, raw)
	}
	return nil
}

// MuteTrack is not supported by the minimal server API without a track SID.
func (l *LiveKit) MuteTrack(context.Context, string, string, bool) error {
	return fmt.Errorf("livekit mute track requires track SID")
}

// Events returns provider events (webhook feeding is deployment-specific).
func (l *LiveKit) Events() <-chan RoomEvent { return l.events }

func (l *LiveKit) call(ctx context.Context, method string, body []byte, room string) (int, []byte, error) {
	token, err := signJWT(l.key, l.secret, map[string]any{
		"iss": l.key, "sub": l.key,
		"nbf":   time.Now().Add(-10 * time.Second).Unix(),
		"exp":   time.Now().Add(5 * time.Minute).Unix(),
		"video": serviceGrant(room),
	})
	if err != nil {
		return 0, nil, err
	}
	endpoint := l.base + "/twirp/" + method
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := l.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, raw, nil
}

func signJWT(key string, secret []byte, claims map[string]any) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + enc.EncodeToString(mac.Sum(nil)), nil
}

func serviceGrant(room string) map[string]any {
	grant := map[string]any{"roomCreate": true, "roomList": true, "roomAdmin": true}
	if room != "" {
		grant["room"] = room
	}
	return grant
}
