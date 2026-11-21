package pushoffline

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
)

// APNsProvider sends token-based (p8) Apple Push Notification requests.
type APNsProvider struct {
	teamID  string
	keyID   string
	topic   string
	key     *ecdsa.PrivateKey
	client  *http.Client
	mu      sync.Mutex
	token   string
	expires time.Time
}

// NewAPNsProvider returns an APNs provider. privateKey is the PEM content of
// the .p8 key. Empty endpoint uses the production APNs host.
func NewAPNsProvider(teamID, keyID, topic, privateKey string, timeout time.Duration) (*APNsProvider, error) {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return nil, fmt.Errorf("apns: invalid p8 key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("apns: parse p8 key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("apns: p8 key is not ECDSA")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &APNsProvider{
		teamID: teamID, keyID: keyID, topic: topic, key: key,
		client: &http.Client{Timeout: timeout, Transport: &http.Transport{ForceAttemptHTTP2: true}},
	}, nil
}

// Name returns "apns".
func (p *APNsProvider) Name() string { return "apns" }

// Send sends one request per device token and preserves input order.
func (p *APNsProvider) Send(ctx context.Context, notifications []Notification) ([]Result, error) {
	results := make([]Result, len(notifications))
	jwt, err := p.authToken()
	if err != nil {
		return nil, err
	}
	for index, notification := range notifications {
		result := Result{Token: notification.Token}
		payload := map[string]any{
			"aps": map[string]any{
				"alert": map[string]string{"title": notification.Title, "body": notification.Body},
				"badge": notification.Badge,
			},
			"extra": notification.Extra,
		}
		body, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			result.Err = marshalErr
			results[index] = result
			continue
		}
		request, reqErr := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.push.apple.com/3/device/"+notification.Token, bytes.NewReader(body))
		if reqErr != nil {
			result.Err = reqErr
			results[index] = result
			continue
		}
		request.Header.Set("authorization", "bearer "+jwt)
		request.Header.Set("apns-topic", p.topic)
		request.Header.Set("apns-push-type", "alert")
		request.Header.Set("apns-priority", "10")
		response, sendErr := p.client.Do(request)
		if sendErr != nil {
			result.Err = sendErr
			results[index] = result
			continue
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusOK:
			result.OK = true
		case http.StatusGone, http.StatusBadRequest:
			result.Invalid = true
			result.Err = &providerError{message: "apns status " + response.Status}
		default:
			result.Err = &providerError{message: "apns status " + response.Status}
		}
		results[index] = result
	}
	return results, nil
}

func (p *APNsProvider) authToken() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Now().Before(p.expires) {
		return p.token, nil
	}
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": p.keyID})
	claims, _ := json.Marshal(map[string]any{"iss": p.teamID, "iat": time.Now().Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	r, s, err := ecdsa.Sign(rand.Reader, p.key, digest[:])
	if err != nil {
		return "", err
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	p.token = unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
	p.expires = time.Now().Add(50 * time.Minute)
	return p.token, nil
}
