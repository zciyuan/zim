package route

import (
	"context"
	"fmt"

	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"
)

// BatchLocator resolves several device keys in one round trip.
//
// Implementations must treat a missing location as "offline" and omit it from
// the result map instead of returning an error.
type BatchLocator interface {
	// LocateDeviceKeys returns the gateway ID for every device key that is
	// currently bound. Keys that are offline are omitted.
	LocateDeviceKeys(ctx context.Context, deviceKeys []int64) (map[int64]string, error)
}

// RedisBatchLocator implements BatchLocator over due's locate keys:
//
//	{prefix}:locate:user:{deviceKey}:gate
//
// The prefix defaults to due's "due:locate" and must match `etc.locate.redis.prefix`.
type RedisBatchLocator struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisBatchLocator returns a batch locator. The client is required.
func NewRedisBatchLocator(client redis.UniversalClient, prefix string) (*RedisBatchLocator, error) {
	if client == nil {
		return nil, ErrLocatorMissing
	}
	if prefix == "" {
		prefix = "due:locate"
	}
	return &RedisBatchLocator{client: client, prefix: prefix}, nil
}

// LocateDeviceKeys pipelines one GET per key. Missing keys are skipped.
func (l *RedisBatchLocator) LocateDeviceKeys(ctx context.Context, deviceKeys []int64) (map[int64]string, error) {
	result := make(map[int64]string, len(deviceKeys))
	if len(deviceKeys) == 0 {
		return result, nil
	}

	pipeline := l.client.Pipeline()
	commands := make([]*redis.StringCmd, len(deviceKeys))
	for i, deviceKey := range deviceKeys {
		commands[i] = pipeline.Get(ctx, l.key(deviceKey))
	}

	// Exec reports the first command error; redis.Nil only means "offline".
	if _, err := pipeline.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, command := range commands {
		gateID, err := command.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		if gateID != "" {
			result[deviceKeys[i]] = gateID
		}
	}
	return result, nil
}

func (l *RedisBatchLocator) key(deviceKey int64) string {
	return fmt.Sprintf("%s:locate:user:%d:gate", l.prefix, deviceKey)
}
