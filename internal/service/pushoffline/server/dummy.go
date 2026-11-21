package pushoffline

import (
	"context"
)

// Dummy is the local/test provider. It accepts every notification and is the
// default until a real vendor provider (APNs/FCM/HMS/…) is configured.
type Dummy struct{}

// Name returns the provider name.
func (Dummy) Name() string { return "dummy" }

// Send accepts every notification.
func (Dummy) Send(_ context.Context, notifications []Notification) ([]Result, error) {
	results := make([]Result, 0, len(notifications))
	for _, notification := range notifications {
		results = append(results, Result{Token: notification.Token, OK: true})
	}
	return results, nil
}
