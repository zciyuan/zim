package groupread

import (
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	readTTLSeconds = 30 * 24 * 60 * 60
	flushInterval  = 5 * time.Second
)

var setMaxScript = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], ARGV[1])
if (not current) or tonumber(ARGV[2]) > tonumber(current) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`)

func readKey(groupID int64) string {
	return "zim:group:read:" + strconv.FormatInt(groupID, 10)
}
