package pushoffline

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
)

// ErrProviderNotConfigured reports a missing provider binding.
var ErrProviderNotConfigured = &providerError{"offline push provider not configured"}

// ErrProviderResultMismatch reports a provider that did not answer per token.
var ErrProviderResultMismatch = &providerError{"offline push provider result mismatch"}

type providerError struct{ message string }

func (e *providerError) Error() string { return e.message }

// HTTPProvider sends batches to a self-hosted push gateway.
type HTTPProvider struct {
	name   string
	url    string
	secret string
	client *http.Client
}

// NewHTTPProvider returns a self-hosted HTTP provider.
func NewHTTPProvider(name, url, secret string, timeout time.Duration) *HTTPProvider {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPProvider{name: name, url: url, secret: secret, client: &http.Client{Timeout: timeout}}
}

// Name returns the configured provider name.
func (p *HTTPProvider) Name() string { return p.name }

// Send POSTs one batch. The gateway must answer:
// {"results":[{"token":"...","ok":true,"invalid":false,"error":"..."}]}.
func (p *HTTPProvider) Send(ctx context.Context, notifications []Notification) ([]Result, error) {
	payload := struct {
		Notifications []Notification `json:"notifications"`
	}{Notifications: notifications}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if p.secret != "" {
		mac := hmac.New(sha256.New, []byte(p.secret))
		_, _ = mac.Write(body)
		request.Header.Set("X-ZIM-Signature", hex.EncodeToString(mac.Sum(nil)))
	}
	response, err := p.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil, &providerError{message: "offline push gateway status " + response.Status}
	}
	var reply struct {
		Results []Result `json:"results"`
	}
	raw, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, readErr
	}
	if err = json.Unmarshal(raw, &reply); err != nil {
		return nil, err
	}
	if len(reply.Results) != len(notifications) {
		return nil, ErrProviderResultMismatch
	}
	return reply.Results, nil
}
