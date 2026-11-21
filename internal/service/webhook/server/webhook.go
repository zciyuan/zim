// Package webhook delivers message events to a customer endpoint with HMAC
// signing and bounded retries.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/component/metrics"
)

// Event is the stable webhook payload.
type Event struct {
	Type           string `json:"type"`
	ServerMsgID    string `json:"server_msg_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SendID         int64  `json:"send_id"`
	RecvID         int64  `json:"recv_id,omitempty"`
	GroupID        int64  `json:"group_id,omitempty"`
	MsgType        int32  `json:"msg_type"`
	SendTime       int64  `json:"send_time"`
	Status         string `json:"status,omitempty"`
}

// Notifier accepts events without blocking the caller.
type Notifier interface {
	Notify(event Event)
}

// HTTPNotifier posts events to one configured endpoint.
type HTTPNotifier struct {
	url    string
	secret string
	client *http.Client
	dlq    DLQ
}

// New returns an HTTP webhook notifier.
func New(url, secret string) *HTTPNotifier {
	return &HTTPNotifier{url: url, secret: secret, client: &http.Client{Timeout: 5 * time.Second}}
}

// NewWithDLQ returns a notifier that persists exhausted events.
func NewWithDLQ(url, secret string, dlq DLQ) *HTTPNotifier {
	notifier := New(url, secret)
	notifier.dlq = dlq
	return notifier
}

// Notify sends asynchronously and never panics the caller.
func (n *HTTPNotifier) Notify(event Event) {
	if n == nil || n.url == "" {
		return
	}
	go n.deliver(event)
}

func (n *HTTPNotifier) deliver(event Event) {
	body, err := json.Marshal(event)
	if err != nil {
		metrics.Default.Inc("zim_webhook_total", map[string]string{"result": "encode_error"})
		return
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err = n.post(body); err == nil {
			metrics.Default.Inc("zim_webhook_total", map[string]string{"result": "ok"})
			return
		}
		metrics.Default.Inc("zim_webhook_total", map[string]string{"result": "error"})
		if attempt == 3 && n.dlq != nil {
			_ = n.dlq.Add(context.Background(), event, err)
		}
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
}

func (n *HTTPNotifier) post(body []byte) error {
	request, err := http.NewRequest(http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-ZIM-Timestamp", timestamp)
	request.Header.Set("X-Signature", sign(n.secret, timestamp, body))
	response, err := n.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", response.StatusCode)
	}
	return nil
}

func sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
