// Package sensitive filters message content against the admin-managed word list.
package sensitive

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/dobyte/due/v2/encoding/json"
	"github.com/redis/go-redis/v9"
)

// Repository loads blocked words.
type Repository interface {
	List(ctx context.Context) ([]string, error)
}

// Service caches the blocked word list for a short TTL.
type Service struct {
	repo    Repository
	ttl     time.Duration
	mu      sync.RWMutex
	words   []string
	loaded  time.Time
	version int64
	loadErr error
	redis   redis.UniversalClient
}

// New returns a sensitive-word filter.
func New(repo Repository) *Service {
	return &Service{repo: repo, ttl: 30 * time.Second}
}

// SetCache enables the Redis L2 word list.
func (s *Service) SetCache(client redis.UniversalClient) { s.redis = client }

// Check reports whether content contains a blocked word.
func (s *Service) Check(ctx context.Context, content string) (bool, error) {
	version := int64(0)
	if s.redis != nil {
		if value, err := s.redis.Get(ctx, "zim:sensitive:version").Int64(); err == nil {
			version = value
		}
	}

	s.mu.RLock()
	words, loaded, cachedVersion, loadErr := s.words, s.loaded, s.version, s.loadErr
	s.mu.RUnlock()

	if loaded.IsZero() || time.Since(loaded) > s.ttl || cachedVersion != version {
		next, nextErr := s.load(ctx, version)
		if nextErr == nil {
			words, loadErr = next, nil
		} else {
			loadErr = nextErr
		}
		s.mu.Lock()
		s.words, s.loaded, s.version, s.loadErr = words, time.Now(), version, loadErr
		s.mu.Unlock()
	}
	if loadErr != nil {
		return false, loadErr
	}
	for _, word := range words {
		if word != "" && strings.Contains(content, word) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) load(ctx context.Context, version int64) ([]string, error) {
	if s.redis != nil {
		key := "zim:sensitive:words:" + strconv.FormatInt(version, 10)
		if value, err := s.redis.Get(ctx, key).Result(); err == nil {
			var words []string
			if json.UnmarshalString(value, &words) == nil {
				return words, nil
			}
		}
		words, err := s.repo.List(ctx)
		if err != nil {
			return nil, err
		}
		if value, err := json.Marshal(words); err == nil {
			_ = s.redis.Set(ctx, key, string(value), time.Hour).Err()
		}
		return words, nil
	}
	return s.repo.List(ctx)
}
