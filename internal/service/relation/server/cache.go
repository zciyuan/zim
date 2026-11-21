package server

import (
	"context"
	"strconv"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/localcache"
	"github.com/zciyuan/ZIM/internal/model"
)

const friendCacheTTL = 5 * time.Minute

func friendKey(uid int64) string { return "zim:friend:list:" + strconv.FormatInt(uid, 10) }

// SetCache enables the L1+Redis friend-list cache.
func (s *Server) SetCache(client redis.UniversalClient) {
	s.redis = client
	s.friends = localcache.New[int64, []*model.Friend](3*time.Second, 100000)
}

func (s *Server) cachedFriends(ctx context.Context, uid int64) ([]*model.Friend, bool) {
	if s.friends != nil {
		if items, ok := s.friends.Get(uid); ok {
			return items, true
		}
	}
	if s.redis == nil {
		return nil, false
	}
	value, err := s.redis.Get(ctx, friendKey(uid)).Result()
	if err != nil {
		return nil, false
	}
	items := make([]*model.Friend, 0)
	if json.UnmarshalString(value, &items) != nil {
		return nil, false
	}
	if s.friends != nil {
		s.friends.Set(uid, items)
	}
	return items, true
}

func (s *Server) storeFriends(ctx context.Context, uid int64, items []*model.Friend) {
	if s.redis != nil {
		if value, err := json.Marshal(items); err == nil {
			_ = s.redis.Set(ctx, friendKey(uid), string(value), friendCacheTTL).Err()
		}
	}
	if s.friends != nil {
		s.friends.Set(uid, items)
	}
}

func (s *Server) invalidateFriends(ctx context.Context, uids ...int64) {
	for _, uid := range uids {
		if s.friends != nil {
			s.friends.Delete(uid)
		}
		if s.redis != nil {
			_ = s.redis.Del(ctx, friendKey(uid)).Err()
		}
	}
}
