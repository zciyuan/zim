package pushoffline

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// rateScript is a fixed-window limiter: INCR + PEXPIRE on first hit.
const rateScript = `
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
  return 0
end
return 1
`

var limiterScript = redis.NewScript(rateScript)

// NewRedisLimiter returns a per-user fixed-window limiter.
func NewRedisLimiter(client redis.UniversalClient, limit int64, window time.Duration) *RedisLimiter {
	if limit <= 0 {
		limit = 5
	}
	if window <= 0 {
		window = time.Minute
	}
	return &RedisLimiter{client: client, limit: limit, window: window, script: limiterScript}
}

// Allow reports whether uid may receive another notification in this window.
func (l *RedisLimiter) Allow(ctx context.Context, uid int64) (bool, error) {
	if l == nil || l.client == nil {
		return true, nil
	}
	key := "zim:push:freq:" + strconv.FormatInt(uid, 10)
	allowed, err := l.script.Run(ctx, l.client, []string{key},
		l.window.Milliseconds(), l.limit).Int()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

// AllowMany applies the same fixed-window limit for a user batch in one
// Redis pipeline instead of one round trip per user.
func (l *RedisLimiter) AllowMany(ctx context.Context, uids []int64) (map[int64]bool, error) {
	result := make(map[int64]bool, len(uids))
	if l == nil || l.client == nil {
		for _, uid := range uids {
			result[uid] = true
		}
		return result, nil
	}
	pipeline := l.client.Pipeline()
	commands := make(map[int64]*redis.Cmd, len(uids))
	for _, uid := range uids {
		key := "zim:push:freq:" + strconv.FormatInt(uid, 10)
		commands[uid] = pipeline.Eval(ctx, rateScript, []string{key}, l.window.Milliseconds(), l.limit)
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return nil, err
	}
	for uid, command := range commands {
		value, err := command.Int()
		if err != nil {
			return nil, err
		}
		result[uid] = value == 1
	}
	return result, nil
}
