package reaction

import "github.com/redis/go-redis/v9"

// Reaction counters.
//
//	zim:reaction:{cid}:{seq}        HASH  emoji -> count
//	zim:reaction:mine:{cid}:{seq}:{uid}  SET   emojis this user added
//
// Both scripts are atomic so a concurrent add/remove cannot desync the counter
// from the membership set (ZIM-02 §Q56 mandates one row per user+emoji).
var (
	addScript = redis.NewScript(`
if redis.call('SADD', KEYS[2], ARGV[1]) == 1 then
  redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
  return 1
end
return 0
`)

	removeScript = redis.NewScript(`
if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
  local left = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
  if left <= 0 then
    redis.call('HDEL', KEYS[1], ARGV[1])
  end
  return 1
end
return 0
`)
)
