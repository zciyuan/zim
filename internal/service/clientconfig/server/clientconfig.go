// Package clientconfig exposes runtime client configuration.
package clientconfig

import (
	"context"
	"strings"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/localcache"
	adminsvc "github.com/zciyuan/ZIM/internal/service/admin/server"
)

// Repository loads client settings.
type Repository interface {
	ListSettings(ctx context.Context, prefix string) ([]*adminsvc.Setting, error)
}

// Service reads client configuration keys.
type Service struct {
	repo   Repository
	prefix string
	cache  *localcache.Cache[string, []*adminsvc.Setting]
	redis  redis.UniversalClient
}

// SetCache enables Redis L2.
func (s *Service) SetCache(client redis.UniversalClient) { s.redis = client }

// New returns a client-config service.
func New(repo Repository) *Service {
	return &Service{repo: repo, prefix: "client.", cache: localcache.New[string, []*adminsvc.Setting](30*time.Second, 1)}
}

// All returns all client-visible settings.
func (s *Service) All(ctx context.Context) ([]*adminsvc.Setting, error) {
	if items, ok := s.cache.Get("all"); ok {
		return items, nil
	}
	if s.redis != nil {
		if value, err := s.redis.Get(ctx, "zim:clientconfig:all").Result(); err == nil {
			items := make([]*adminsvc.Setting, 0)
			if json.UnmarshalString(value, &items) == nil {
				s.cache.Set("all", items)
				return items, nil
			}
		}
	}
	items, err := s.repo.ListSettings(ctx, s.prefix+"%")
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		item.Key = strings.TrimPrefix(item.Key, s.prefix)
	}
	s.cache.Set("all", items)
	if s.redis != nil {
		if value, err := json.Marshal(items); err == nil {
			_ = s.redis.Set(ctx, "zim:clientconfig:all", string(value), 5*time.Minute).Err()
		}
	}
	return items, nil
}

// Invalidate clears both L1 and Redis after an admin config write.
func (s *Service) Invalidate(ctx context.Context) {
	s.cache.Delete("all")
	if s.redis != nil {
		_ = s.redis.Del(ctx, "zim:clientconfig:all").Err()
	}
}
