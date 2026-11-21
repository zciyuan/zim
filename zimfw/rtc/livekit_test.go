package rtc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLiveKitToken(t *testing.T) {
	provider := NewLiveKit(LiveKitConfig{URL: "http://localhost", APIKey: "key", APISecret: "secret"})
	token, err := provider.IssueToken(context.Background(), "room-1", 10001, RolePublisher)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token = %q", token)
	}
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) != parts[2] {
		t.Fatal("signature mismatch")
	}
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	if err = json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	video := claims["video"].(map[string]any)
	if claims["sub"] != "10001" || video["room"] != "room-1" || video["canPublish"] != true {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestLiveKitCreateAndRemove(t *testing.T) {
	var createAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		createAuth = r.Header.Get("Authorization")
		switch {
		case strings.HasSuffix(r.URL.Path, "CreateRoom"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"sid":"RM_1","name":"room-1"}`))
		case strings.HasSuffix(r.URL.Path, "RemoveParticipant"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider := NewLiveKit(LiveKitConfig{URL: server.URL, APIKey: "key", APISecret: "secret"})
	room, err := provider.CreateRoom(context.Background(), RoomReq{Room: "room-1"})
	if err != nil || room.ID != "RM_1" {
		t.Fatalf("room = %+v, err = %v", room, err)
	}
	if !strings.HasPrefix(createAuth, "Bearer ") {
		t.Fatalf("auth = %q", createAuth)
	}
	if err = provider.RemoveParticipant(context.Background(), "room-1", 10001); err != nil {
		t.Fatal(err)
	}
}

func TestLiveKitTTLDefault(t *testing.T) {
	provider := NewLiveKit(LiveKitConfig{URL: "http://localhost", APIKey: "key", APISecret: "secret", TokenTTL: time.Minute})
	if provider.ttl != time.Minute {
		t.Fatalf("ttl = %v", provider.ttl)
	}
}
