// Package groupversion tracks group member and join versions in Redis.
package groupversion

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Store increments and reads group versions.
type Store interface {
	IncrMember(ctx context.Context, gid int64) (int64, error)
	IncrJoin(ctx context.Context, gid int64) (int64, error)
	Member(ctx context.Context, gid int64) (int64, error)
	Join(ctx context.Context, gid int64) (int64, error)
}

// Service is the Redis-backed version store.
type Service struct{ client redis.UniversalClient }

// New returns a group version service.
func New(client redis.UniversalClient) *Service { return &Service{client: client} }

func memberKey(gid int64) string { return "zim:group:memberver:" + strconv.FormatInt(gid, 10) }
func joinKey(gid int64) string   { return "zim:group:joinver:" + strconv.FormatInt(gid, 10) }

// IncrMember increments the group member version.
func (s *Service) IncrMember(ctx context.Context, gid int64) (int64, error) {
	return s.client.Incr(ctx, memberKey(gid)).Result()
}

// IncrJoin increments the group join version.
func (s *Service) IncrJoin(ctx context.Context, gid int64) (int64, error) {
	return s.client.Incr(ctx, joinKey(gid)).Result()
}

// Member returns the current member version.
func (s *Service) Member(ctx context.Context, gid int64) (int64, error) {
	value, err := s.client.Get(ctx, memberKey(gid)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return value, err
}

// Join returns the current join version.
func (s *Service) Join(ctx context.Context, gid int64) (int64, error) {
	value, err := s.client.Get(ctx, joinKey(gid)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return value, err
}
