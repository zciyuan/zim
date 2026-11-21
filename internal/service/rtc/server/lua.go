package rtc

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Group participant scripts.
//
// Both scripts touch exactly one key so they stay cluster-safe: call state
// (hash), participants (set) and the group index live on different slots, so
// multi-key Lua would fail on a Redis Cluster.
var (
	// joinGroupMemberScript adds uid when the room has capacity.
	//
	// KEYS[1] = zim:rtc:call:{call_id}:members (SET)
	// ARGV[1] = uid
	// ARGV[2] = max participants
	// Returns 1 when uid is (or becomes) a member, 0 when the room is full.
	joinGroupMemberScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 1
end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
return 1
`)

	// leaveGroupMemberScript removes uid and reports the remaining count.
	//
	// KEYS[1] = zim:rtc:call:{call_id}:members (SET)
	// ARGV[1] = uid
	// Returns {removed, left}.
	leaveGroupMemberScript = redis.NewScript(`
local removed = redis.call('SREM', KEYS[1], ARGV[1])
return {removed, redis.call('SCARD', KEYS[1])}
`)

	// deleteGroupIndexScript removes the group index only while it still points
	// at the call the caller observed. Two servers racing to replace a stale
	// index must not delete the winner's fresh index.
	//
	// KEYS[1] = zim:rtc:group:{gid} (STRING)
	// ARGV[1] = expected call_id
	// Returns 1 when deleted, 0 when the value changed.
	deleteGroupIndexScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)
)

// runJoinGroupMemberScript returns true when uid is a participant after the
// call. A false result means the room is at capacity.
func runJoinGroupMemberScript(ctx context.Context, client redis.UniversalClient,
	members string, uid int64, max int32) (bool, error) {
	result, err := joinGroupMemberScript.Run(ctx, client, []string{members}, uid, max).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// runLeaveGroupMemberScript returns removed and the remaining participants.
func runLeaveGroupMemberScript(ctx context.Context, client redis.UniversalClient,
	members string, uid int64) (int64, int64, error) {
	values, err := leaveGroupMemberScript.Run(ctx, client, []string{members}, uid).Int64Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, ErrInvalidCall
	}
	return values[0], values[1], nil
}

// runDeleteGroupIndexScript deletes the group index only when it still points
// at expected. A zero result means another call already owns the group.
func runDeleteGroupIndexScript(ctx context.Context, client redis.UniversalClient,
	key, expected string) (bool, error) {
	deleted, err := deleteGroupIndexScript.Run(ctx, client, []string{key}, expected).Int64()
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}
