// Package redis owns the application Redis client. It intentionally reuses the
// due locate.redis bootstrap keys so the process has one Redis configuration.
package redis

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"
)

// Redis is the process-wide Redis component.

// New returns the Redis component.
func New() *Redis {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        etc.Get("etc.locate.redis.addrs", []string{"127.0.0.1:6379"}).Strings(),
		DB:           etc.Get("etc.locate.redis.db", 0).Int(),
		Username:     etc.Get("etc.locate.redis.username").String(),
		Password:     etc.Get("etc.locate.redis.password").String(),
		PoolSize:     etc.Get("etc.locate.redis.poolSize", 0).Int(),
		MinIdleConns: etc.Get("etc.locate.redis.minIdleConns", 0).Int(),
	})
	return &Redis{client: client}
}

// Name returns the component name.
func (r *Redis) Name() string {
	return "redis"
}

// Start verifies connectivity once.
func (r *Redis) Start() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.client.Ping(ctx).Err()
	cancel()
	if err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Info("redis connected")
}

// Close closes the client.
func (r *Redis) Close() {
	if r.client != nil {
		_ = r.client.Close()
	}
}

// Client returns the shared client.
func (r *Redis) Client() redis.UniversalClient {
	return r.client
}
