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

const userCacheTTL = 5 * time.Minute

func userKey(uid int64) string { return "zim:user:profile:" + strconv.FormatInt(uid, 10) }

// SetCache enables the L1+Redis profile cache.
func (s *Server) SetCache(client redis.UniversalClient) {
	s.redis = client
	s.profiles = localcache.New[int64, *model.User](3*time.Second, 100000)
}

func (s *Server) cachedProfile(ctx context.Context, uid int64) (*model.User, bool) {
	if s.profiles != nil {
		if user, ok := s.profiles.Get(uid); ok {
			return user, true
		}
	}
	if s.redis == nil {
		return nil, false
	}
	value, err := s.redis.Get(ctx, userKey(uid)).Result()
	if err != nil {
		return nil, false
	}
	user := &model.User{}
	if json.UnmarshalString(value, user) != nil {
		return nil, false
	}
	if s.profiles != nil {
		s.profiles.Set(uid, user)
	}
	return user, true
}

func (s *Server) storeProfile(ctx context.Context, user *model.User) {
	if user == nil || user.ID <= 0 || s.redis == nil {
		return
	}
	value, err := json.Marshal(user)
	if err == nil {
		_ = s.redis.Set(ctx, userKey(user.ID), string(value), userCacheTTL).Err()
	}
	if s.profiles != nil {
		s.profiles.Set(user.ID, user)
	}
}

func (s *Server) invalidateProfile(ctx context.Context, uid int64) {
	if s.profiles != nil {
		s.profiles.Delete(uid)
	}
	if s.redis != nil {
		_ = s.redis.Del(ctx, userKey(uid)).Err()
	}
}
