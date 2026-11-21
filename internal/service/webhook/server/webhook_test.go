package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotifySignsAndRetries(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := attempts.Add(1)
		body, _ := io.ReadAll(r.Body)
		timestamp := r.Header.Get("X-ZIM-Timestamp")
		mac := hmac.New(sha256.New, []byte("secret"))
		_, _ = mac.Write([]byte(timestamp))
		_, _ = mac.Write(body)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if r.Header.Get("X-Signature") != want {
			t.Errorf("signature mismatch")
		}
		if current < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	notifier := New(server.URL, "secret")
	notifier.Notify(Event{Type: "message", ServerMsgID: "m1", Seq: 3})
	deadline := time.Now().Add(2 * time.Second)
	for attempts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() < 2 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
}
