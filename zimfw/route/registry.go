package route

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ConnRegistry tracks the device keys bound to a gateway. It exists so that a
// gateway that dies without unbinding (kill -9) can have its stale locations
// reclaimed (ADR-0022).
//
// Keys:
//
//	{prefix}:gate:ids            set of known gateway IDs
//	{prefix}:gate:{gid}:conns    set of device keys bound to that gateway
type ConnRegistry interface {
	// Add records that deviceKey is bound to gateID.
	Add(ctx context.Context, gateID string, deviceKey int64) error
	// Remove drops a single binding.
	Remove(ctx context.Context, gateID string, deviceKey int64) error
	// AddUserDevice adds one device to the user's online set.
	AddUserDevice(ctx context.Context, uid, deviceKey int64) error
	// RemoveUserDevice removes one device from the user's online set.
	RemoveUserDevice(ctx context.Context, uid, deviceKey int64) error
}

// RedisConnRegistry implements ConnRegistry on a Redis cluster.
type RedisConnRegistry struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisConnRegistry returns a registry. The client is required.
func NewRedisConnRegistry(client redis.UniversalClient, prefix string) (*RedisConnRegistry, error) {
	if client == nil {
		return nil, ErrLocatorMissing
	}
	if prefix == "" {
		prefix = "due:locate"
	}
	return &RedisConnRegistry{client: client, prefix: prefix}, nil
}

// Add writes both the gateway set and the gateway ID index in one pipeline.
func (r *RedisConnRegistry) Add(ctx context.Context, gateID string, deviceKey int64) error {
	pipeline := r.client.Pipeline()
	pipeline.SAdd(ctx, r.connsKey(gateID), deviceKey)
	pipeline.SAdd(ctx, r.idsKey(), gateID)
	_, err := pipeline.Exec(ctx)
	return err
}

// Remove drops a single device key from the gateway set.
func (r *RedisConnRegistry) Remove(ctx context.Context, gateID string, deviceKey int64) error {
	return r.client.SRem(ctx, r.connsKey(gateID), deviceKey).Err()
}

// IDsKey returns the gateway ID index key.
func (r *RedisConnRegistry) IDsKey() string {
	return r.idsKey()
}

// AddUserDevice adds one device to the user's online set.
func (r *RedisConnRegistry) AddUserDevice(ctx context.Context, uid, deviceKey int64) error {
	return r.client.SAdd(ctx, r.udevKey(uid), deviceKey).Err()
}

// RemoveUserDevice removes one device from the user's online set.
func (r *RedisConnRegistry) RemoveUserDevice(ctx context.Context, uid, deviceKey int64) error {
	return r.client.SRem(ctx, r.udevKey(uid), deviceKey).Err()
}

// udevKey returns the per-user online device set key.
func (r *RedisConnRegistry) udevKey(uid int64) string {
	return fmt.Sprintf("%s:udev:%d", r.prefix, uid)
}

// ConnsKey returns the per-gateway device key set.
func (r *RedisConnRegistry) ConnsKey(gateID string) string {
	return r.connsKey(gateID)
}

func (r *RedisConnRegistry) idsKey() string {
	return r.prefix + ":gate:ids"
}

func (r *RedisConnRegistry) connsKey(gateID string) string {
	return fmt.Sprintf("%s:gate:%s:conns", r.prefix, gateID)
}
