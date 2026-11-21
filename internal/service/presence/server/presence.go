// Package presence reports online state from the per-user device index.
package presence

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Service reads the online device set maintained by zimfw/route.
type Service struct {
	client redis.UniversalClient
	prefix string
}

// New returns a presence service. prefix defaults to "zim".
func New(client redis.UniversalClient, prefix string) *Service {
	if prefix == "" {
		prefix = "zim"
	}
	return &Service{client: client, prefix: prefix}
}

// Online returns whether each uid has at least one bound device.
func (s *Service) Online(ctx context.Context, uids []int64) (map[int64]bool, error) {
	result := make(map[int64]bool, len(uids))
	if s == nil || s.client == nil {
		return result, nil
	}
	for start := 0; start < len(uids); start += 500 {
		end := start + 500
		if end > len(uids) {
			end = len(uids)
		}
		pipeline := s.client.Pipeline()
		commands := make(map[int64]*redis.IntCmd, end-start)
		for _, uid := range uids[start:end] {
			commands[uid] = pipeline.Exists(ctx, fmt.Sprintf("%s:udev:%d", s.prefix, uid))
		}
		if _, err := pipeline.Exec(ctx); err != nil {
			return nil, err
		}
		for uid, command := range commands {
			result[uid] = command.Val() > 0
		}
	}
	return result, nil
}
