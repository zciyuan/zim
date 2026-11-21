package redis

import (
	"github.com/dobyte/due/v2/component"
	"github.com/redis/go-redis/v9"
)

// Redis is the process-wide Redis component.
type Redis struct {
	component.Base
	client redis.UniversalClient
}
