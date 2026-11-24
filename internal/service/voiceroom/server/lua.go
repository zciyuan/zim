package server

import (
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Redis key layout (ADR-0043):
//
//	zim:vr:room:{room_id}        Hash   room snapshot + m:{uid} presence + online count
//	zim:vr:seats:{room_id}       Hash   field=seat index, value="uid:status"
//	zim:vr:handup:{room_id}      ZSet   member=uid, score=hand-up time (FIFO)
//	zim:vr:reconnect_timeout     ZSet   member="{room}:{seat}:{uid}", score=deadline
//	zim:vr:empty_timeout         ZSet   member=room id, score=deadline
func roomKey(roomID int64) string { return "zim:vr:room:" + strconv.FormatInt(roomID, 10) }

func seatsKey(roomID int64) string { return "zim:vr:seats:" + strconv.FormatInt(roomID, 10) }

func handupKey(roomID int64) string { return "zim:vr:handup:" + strconv.FormatInt(roomID, 10) }

const reconnectTimeoutKey = "zim:vr:reconnect_timeout"

const emptyTimeoutKey = "zim:vr:empty_timeout"

// Lua script return codes shared by the seat scripts.
const (
	luaErrRoomMissing    int64 = -1
	luaErrRoomClosed     int64 = -2
	luaErrNoPermission   int64 = -3
	luaErrSeatOccupied   int64 = -4
	luaErrSeatLocked     int64 = -5
	luaErrAlreadySeated  int64 = -6
	luaErrSeatNotFound   int64 = -7
	luaErrBadSeatIndex   int64 = -8
	luaErrNotRoomMember  int64 = -9
	luaErrNotInHandQueue int64 = -10
)

// takeSeatScript occupies one seat atomically.
//
// KEYS[1]=room hash, KEYS[2]=seats hash, KEYS[3]=handup zset
// ARGV[1]=uid, ARGV[2]=seat index, ARGV[3]=now ms, ARGV[4]=approved(0|1)
// Returns >=0 seat index taken, else the negative luaErr codes above.
//
// Free mode lets any member sit; hand-up mode requires owner or an approved
// request. A member can hold at most one seat; a reconnect seat belongs to
// the same uid and is simply restored.
var takeSeatScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'status') or '0') ~= 1 then return -2 end
local owner = tonumber(redis.call('HGET', KEYS[1], 'owner_uid') or '0')
local mode = tonumber(redis.call('HGET', KEYS[1], 'join_seat_mode') or '1')
local uid = tonumber(ARGV[1])
local idx = tonumber(ARGV[2])
local seatCount = tonumber(redis.call('HGET', KEYS[1], 'seat_count') or '0')
if idx < 0 or idx >= seatCount then return -8 end
if redis.call('HEXISTS', KEYS[1], 'm:' .. ARGV[1]) == 0 then return -9 end
if mode == 2 and uid ~= owner and tonumber(ARGV[4]) ~= 1 then return -3 end
local fields = redis.call('HGETALL', KEYS[2])
for i = 1, #fields, 2 do
  local suid, sstatus = fields[i + 1]:match("^(%-?%d+):(%-?%d+)$")
  if suid and sstatus then
    suid = tonumber(suid)
    sstatus = tonumber(sstatus)
    if suid == uid and sstatus > 0 and sstatus ~= 4 then return -6 end
  end
end
local seat = redis.call('HGET', KEYS[2], tostring(idx))
if seat then
  local suid, sstatus = seat:match("^(%-?%d+):(%-?%d+)$")
  if suid and sstatus then
    suid = tonumber(suid)
    sstatus = tonumber(sstatus)
    if sstatus == 4 then return -5 end
    if suid > 0 and suid ~= uid then return -4 end
  end
end
if tonumber(ARGV[4]) == 1 and redis.call('ZSCORE', KEYS[3], ARGV[1]) ~= false then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
redis.call('HSET', KEYS[2], tostring(idx), ARGV[1] .. ':1')
return idx
`)

// leaveSeatScript frees the caller's seat.
//
// KEYS[1]=seats hash, ARGV[1]=uid
// Returns the freed seat index, or luaErrSeatNotFound when not seated.
var leaveSeatScript = redis.NewScript(`
local uid = tonumber(ARGV[1])
local fields = redis.call('HGETALL', KEYS[1])
for i = 1, #fields, 2 do
  local suid, sstatus = fields[i + 1]:match("^(%-?%d+):(%-?%d+)$")
  if suid and sstatus then
    suid = tonumber(suid)
    sstatus = tonumber(sstatus)
    if suid == uid and sstatus > 0 and sstatus ~= 4 then
      redis.call('HSET', KEYS[1], fields[i], '0:0')
      return tonumber(fields[i])
    end
  end
end
return -7
`)

// kickSeatScript frees one seat on behalf of the owner.
//
// KEYS[1]=room hash, KEYS[2]=seats hash
// ARGV[1]=operator uid, ARGV[2]=target uid, ARGV[3]=seat index
// Returns the seat index, else the negative luaErr codes above.
var kickSeatScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'owner_uid') or '0') ~= tonumber(ARGV[1]) then return -3 end
local seat = redis.call('HGET', KEYS[2], ARGV[3])
if not seat then return -7 end
local suid = seat:match("^(%-?%d+)")
if tonumber(suid) ~= tonumber(ARGV[2]) then return -4 end
redis.call('HSET', KEYS[2], ARGV[3], '0:0')
return tonumber(ARGV[3])
`)

// forceMuteSeatScript toggles the force-mute state; unmuting returns the
// seat to OCCUPIED_SPEAKING.
//
// KEYS[1]=room hash, KEYS[2]=seats hash
// ARGV[1]=operator uid, ARGV[2]=seat index, ARGV[3]=muted(0|1)
// Returns the seat index when changed, else the negative luaErr codes above.
var forceMuteSeatScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'owner_uid') or '0') ~= tonumber(ARGV[1]) then return -3 end
local seat = redis.call('HGET', KEYS[2], ARGV[2])
if not seat then return -7 end
local suid, sstatus = seat:match("^(%-?%d+):(%-?%d+)$")
if not suid or not sstatus then return -7 end
suid = tonumber(suid)
sstatus = tonumber(sstatus)
if suid <= 0 then return -7 end
if sstatus == 4 then return -5 end
if tonumber(ARGV[3]) == 1 then
  if sstatus == 3 then return tonumber(ARGV[2]) end
  redis.call('HSET', KEYS[2], ARGV[2], suid .. ':3')
else
  if sstatus ~= 3 then return tonumber(ARGV[2]) end
  redis.call('HSET', KEYS[2], ARGV[2], suid .. ':1')
end
return tonumber(ARGV[2])
`)

// lockSeatScript locks or unlocks an empty seat; an occupied seat cannot be
// locked (luaErrSeatOccupied), and a locked seat with no occupant cannot be
// force-muted.
//
// KEYS[1]=room hash, KEYS[2]=seats hash
// ARGV[1]=operator uid, ARGV[2]=seat index, ARGV[3]=locked(0|1)
// Returns the seat index when changed, else the negative luaErr codes above.
var lockSeatScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'owner_uid') or '0') ~= tonumber(ARGV[1]) then return -3 end
local seat = redis.call('HGET', KEYS[2], ARGV[2])
if not seat then return -7 end
local suid, sstatus = seat:match("^(%-?%d+):(%-?%d+)$")
if not suid or not sstatus then return -7 end
suid = tonumber(suid)
sstatus = tonumber(sstatus)
if tonumber(ARGV[3]) == 1 then
  if sstatus == 4 then return tonumber(ARGV[2]) end
  if suid > 0 then return -4 end
  redis.call('HSET', KEYS[2], ARGV[2], '0:4')
else
  if sstatus ~= 4 then return tonumber(ARGV[2]) end
  redis.call('HSET', KEYS[2], ARGV[2], '0:0')
end
return tonumber(ARGV[2])
`)

// markReconnectScript moves a seated member to RECONNECTING on a media
// disconnect. KEYS[1]=seats hash, ARGV[1]=uid; returns the seat index or
// luaErrSeatNotFound when the member holds no seat.
var markReconnectScript = redis.NewScript(`
local uid = tonumber(ARGV[1])
local fields = redis.call('HGETALL', KEYS[1])
for i = 1, #fields, 2 do
  local suid, sstatus = fields[i + 1]:match("^(%-?%d+):(%-?%d+)$")
  if suid and sstatus then
    suid = tonumber(suid)
    sstatus = tonumber(sstatus)
    if suid == uid and sstatus > 0 and sstatus ~= 4 and sstatus ~= 5 then
      redis.call('HSET', KEYS[1], fields[i], ARGV[1] .. ':5')
      return tonumber(fields[i])
    end
  end
end
return -7
`)

// restoreSeatScript restores a RECONNECTING seat to OCCUPIED_SPEAKING on a
// media rejoin. KEYS[1]=seats hash, ARGV[1]=uid; returns the seat index or
// luaErrSeatNotFound.
var restoreSeatScript = redis.NewScript(`
local uid = tonumber(ARGV[1])
local fields = redis.call('HGETALL', KEYS[1])
for i = 1, #fields, 2 do
  local suid, sstatus = fields[i + 1]:match("^(%-?%d+):(%-?%d+)$")
  if suid and sstatus then
    suid = tonumber(suid)
    sstatus = tonumber(sstatus)
    if suid == uid and sstatus == 5 then
      redis.call('HSET', KEYS[1], fields[i], ARGV[1] .. ':1')
      return tonumber(fields[i])
    end
  end
end
return -7
`)

// reapSeatScript frees a seat whose reconnect grace expired, unless the
// member reconnected (or took another seat) meanwhile.
//
// KEYS[1]=seats hash, ARGV[1]=seat index, ARGV[2]=uid
// Returns 1 reclaimed, 0 already recovered, luaErrSeatNotFound otherwise.
var reapSeatScript = redis.NewScript(`
local seat = redis.call('HGET', KEYS[1], ARGV[1])
if not seat then return -7 end
local suid, sstatus = seat:match("^(%-?%d+):(%-?%d+)$")
if not suid or not sstatus then return 0 end
suid = tonumber(suid)
sstatus = tonumber(sstatus)
if suid ~= tonumber(ARGV[2]) or sstatus ~= 5 then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], '0:0')
return 1
`)

// memberJoinScript adds one member presence idempotently.
//
// KEYS[1]=room hash, ARGV[1]=uid, ARGV[2]=now ms
// Returns the online count after the change, or luaErrRoomMissing.
var memberJoinScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HEXISTS', KEYS[1], 'm:' .. ARGV[1]) == 0 then
  redis.call('HSET', KEYS[1], 'm:' .. ARGV[1], ARGV[2])
  redis.call('HINCRBY', KEYS[1], 'online', 1)
end
return tonumber(redis.call('HGET', KEYS[1], 'online') or '0')
`)

// memberLeaveScript removes one member presence idempotently.
//
// KEYS[1]=room hash, ARGV[1]=uid
// Returns the online count after the change, or luaErrRoomMissing.
var memberLeaveScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HEXISTS', KEYS[1], 'm:' .. ARGV[1]) == 1 then
  redis.call('HDEL', KEYS[1], 'm:' .. ARGV[1])
  local online = tonumber(redis.call('HGET', KEYS[1], 'online') or '0')
  if online > 0 then online = online - 1 end
  redis.call('HSET', KEYS[1], 'online', online)
  return online
end
return tonumber(redis.call('HGET', KEYS[1], 'online') or '0')
`)

// handUpScript enqueues one hand-up request; duplicates return -1.
//
// KEYS[1]=handup zset, ARGV[1]=uid, ARGV[2]=now ms
// Returns 1 enqueued, -1 already queued.
var handUpScript = redis.NewScript(`
if redis.call('ZSCORE', KEYS[1], ARGV[1]) ~= false then return -1 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

// initSeatsScript lazily initialises the seat hash.
//
// KEYS[1]=seats hash, ARGV[1]=seat count
// Returns the number of seats.
var initSeatsScript = redis.NewScript(`
local count = tonumber(ARGV[1])
for i = 0, count - 1 do
  redis.call('HSETNX', KEYS[1], tostring(i), '0:0')
end
return count
`)

func reconnectMember(roomID int64, seatIndex int64, uid int64) string {
	return strconv.FormatInt(roomID, 10) + ":" + strconv.FormatInt(seatIndex, 10) + ":" + strconv.FormatInt(uid, 10)
}
