package gateclean

import (
	"github.com/dobyte/due/v2/registry"
	"github.com/redis/go-redis/v9"
)

// Stats reports one cleanup pass.
type Stats struct {
	// LiveGates is the number of gateways currently registered.
	LiveGates int
	// DeadGates is the number of gateways that disappeared from the registry.
	DeadGates int
	// Members is the number of indexed device keys inspected for dead gateways.
	Members int
	// Deleted is the number of location keys reclaimed.
	Deleted int
	// Skipped is the number of members that had already rebound to another gateway.
	Skipped int
}

// Service reclaims locations left behind by gateways that no longer exist.
type Service struct {
	client    redis.UniversalClient
	discovery registry.Discovery
	prefix    string
	service   string
	batch     int
}
