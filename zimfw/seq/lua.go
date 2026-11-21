package seq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// KEYS[1] = zim:seq:malloc:{conv}
// ARGV[1] = block size
// Returns {start, end}. CURR is the last handed-out local number; LAST is the
// reserved upper bound. Both are advanced atomically so a crashed process can
// only leave an unused gap, never reuse a number.
const mallocScript = `
local curr = redis.call('HGET', KEYS[1], 'CURR')
local last = redis.call('HGET', KEYS[1], 'LAST')
if (not curr) or (not last) or (tonumber(curr) >= tonumber(last)) then
  local endNum = redis.call('HINCRBY', KEYS[1], 'LAST', tonumber(ARGV[1]))
  local startNum = endNum - tonumber(ARGV[1]) + 1
  redis.call('HSET', KEYS[1], 'CURR', endNum)
  return {startNum, endNum}
end
local nextNum = redis.call('HINCRBY', KEYS[1], 'CURR', 1)
return {nextNum, tonumber(last)}
`

// KEYS[1] = zim:seq:malloc:{conv}
const floorScript = `
local floor = tonumber(ARGV[2])
local curr = redis.call('HGET', KEYS[1], 'CURR')
local last = redis.call('HGET', KEYS[1], 'LAST')
if (not curr) or (not last) or (tonumber(last) < floor) then
  redis.call('HSET', KEYS[1], 'LAST', floor)
  redis.call('HSET', KEYS[1], 'CURR', floor)
  curr = tostring(floor)
  last = tostring(floor)
end
if (tonumber(curr) >= tonumber(last)) then
  local endNum = redis.call('HINCRBY', KEYS[1], 'LAST', tonumber(ARGV[1]))
  local startNum = endNum - tonumber(ARGV[1]) + 1
  redis.call('HSET', KEYS[1], 'CURR', endNum)
  return {startNum, endNum}
end
local nextNum = redis.call('HINCRBY', KEYS[1], 'CURR', 1)
return {nextNum, tonumber(last)}
`

// KEYS[1] = zim:seq:malloc:{conv}
const maxScript = `
local last = redis.call('HGET', KEYS[1], 'LAST')
if not last then return 0 end
return tonumber(last)
`

var (
	malloc  = redis.NewScript(mallocScript)
	floor   = redis.NewScript(floorScript)
	current = redis.NewScript(maxScript)
)

// NewRedisRunner returns a Runner backed by Redis.
func NewRedisRunner(client redis.UniversalClient) Runner {
	return &redisRunner{client: client, malloc: malloc, max: current, floor: floor}
}

func (r *redisRunner) Malloc(ctx context.Context, conversationID string, size int64) (int64, int64, error) {
	values, err := r.malloc.Run(ctx, r.client, []string{seqKey(conversationID)}, size).Int64Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, ErrExhausted
	}
	return values[0], values[1], nil
}

func (r *redisRunner) Max(ctx context.Context, conversationID string) (int64, error) {
	return r.max.Run(ctx, r.client, []string{seqKey(conversationID)}).Int64()
}

func seqKey(conversationID string) string {
	return "zim:seq:malloc:" + conversationID
}

// MallocFloor reserves a block after raising LAST to at least floor.
func (r *redisRunner) MallocFloor(ctx context.Context, conversationID string, size, floor int64) (int64, int64, error) {
	values, err := r.floor.Run(ctx, r.client, []string{seqKey(conversationID)}, size, floor).Int64Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, ErrExhausted
	}
	return values[0], values[1], nil
}
