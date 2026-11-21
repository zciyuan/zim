package rtc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"
)

func signWebhook(secret string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":` + "4102444800" + `}`))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(header + "." + payload))
	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestWebhookLeavesDirectCall(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	if _, err := service.Invite(ctx, "call-w", 1, 2, 1, "room-w"); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"event":"participant_left","id":"evt-1","room":{"name":"zim-call-call-w"},"participant":{"identity":"2"}}`)
	secret := "secret"
	if err := service.HandleWebhook(ctx, body, "Bearer "+signWebhook(secret), secret, nil); err != nil {
		t.Fatal(err)
	}
	call, err := service.Get(ctx, "call-w")
	if err != nil || call.State != StateEnded {
		t.Fatalf("call = %+v, err = %v", call, err)
	}
	started := time.Now()
	if err := service.HandleWebhook(ctx, body, "Bearer bad.token.here", secret, nil); err == nil {
		t.Fatal("expected bad signature error")
	}
	if time.Since(started) > time.Second {
		t.Fatal("signature check too slow")
	}
}
