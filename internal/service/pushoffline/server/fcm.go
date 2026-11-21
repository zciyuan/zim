package pushoffline

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
)

// FCMProvider uses the legacy FCM HTTP endpoint. It is kept as a simple
// third-party adapter; FCM HTTP v1 can be placed behind the HTTP gateway.
type FCMProvider struct {
	key      string
	endpoint string
	client   *http.Client
}

// NewFCMProvider returns an FCM provider.
func NewFCMProvider(serverKey, endpoint string, timeout time.Duration) *FCMProvider {
	if endpoint == "" {
		endpoint = "https://fcm.googleapis.com/fcm/send"
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &FCMProvider{key: serverKey, endpoint: endpoint, client: &http.Client{Timeout: timeout}}
}

// Name returns "fcm".
func (p *FCMProvider) Name() string { return "fcm" }

// Send sends one FCM batch and maps the ordered results back to tokens.
func (p *FCMProvider) Send(ctx context.Context, notifications []Notification) ([]Result, error) {
	if len(notifications) == 0 {
		return nil, nil
	}
	tokens := make([]string, 0, len(notifications))
	for _, notification := range notifications {
		tokens = append(tokens, notification.Token)
	}
	payload := map[string]any{
		"registration_ids": tokens,
		"priority":         "high",
		"notification": map[string]string{
			"title": notifications[0].Title,
			"body":  notifications[0].Body,
		},
		"data": notifications[0].Extra,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "key="+p.key)
	response, err := p.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return nil, &providerError{message: "fcm status " + response.Status + ": " + string(raw)}
	}
	var reply struct {
		Results []struct {
			Error string `json:"error"`
		} `json:"results"`
	}
	raw, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, readErr
	}
	if err = json.Unmarshal(raw, &reply); err != nil {
		return nil, err
	}
	results := make([]Result, len(notifications))
	for i := range notifications {
		result := Result{Token: notifications[i].Token}
		if i < len(reply.Results) && reply.Results[i].Error != "" {
			result.Err = &providerError{message: reply.Results[i].Error}
			result.Invalid = strings.Contains(reply.Results[i].Error, "Registration")
		} else {
			result.OK = true
		}
		results[i] = result
	}
	return results, nil
}
