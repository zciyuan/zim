package webhook

import (
	"context"
)

// DLQ stores events that exhausted retries.
type DLQ interface {
	Add(ctx context.Context, event Event, err error) error
}

// NoopDLQ drops failed events.
type NoopDLQ struct{}

// Add implements DLQ.
func (NoopDLQ) Add(context.Context, Event, error) error { return nil }
