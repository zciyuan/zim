package gateclean

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/dobyte/due/v2/registry"
	"github.com/redis/go-redis/v9"
)

type fakeDiscovery struct {
	instances []*registry.ServiceInstance
	err       error
}

func (f *fakeDiscovery) Services(context.Context, string) ([]*registry.ServiceInstance, error) {
	return f.instances, f.err
}

func (f *fakeDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, nil
}

func (f *fakeDiscovery) Close() error { return nil }

func newService(t *testing.T, discovery registry.Discovery) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client, discovery, "zim"), server
}

func TestRunReclaimsOnlyDeadGateLocations(t *testing.T) {
	service, server := newService(t, &fakeDiscovery{instances: []*registry.ServiceInstance{{ID: "gate-live"}}})
	if _, err := server.SAdd("zim:gate:ids", "gate-dead", "gate-live"); err != nil {
		t.Fatal(err)
	}
	if _, err := server.SAdd("zim:gate:gate-dead:conns", "101", "102"); err != nil {
		t.Fatal(err)
	}
	if err := server.Set("zim:locate:user:101:gate", "gate-dead"); err != nil {
		t.Fatal(err)
	}
	// 102 rebound to a live gate: the reclaimer must not touch it.
	if err := server.Set("zim:locate:user:102:gate", "gate-live"); err != nil {
		t.Fatal(err)
	}

	stats, err := service.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.LiveGates != 1 || stats.DeadGates != 1 || stats.Members != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.Deleted != 1 || stats.Skipped != 1 {
		t.Fatalf("deleted/skipped = %d/%d", stats.Deleted, stats.Skipped)
	}
	if server.Exists("zim:locate:user:101:gate") {
		t.Fatal("dead gate location must be deleted")
	}
	if gate, err := server.Get("zim:locate:user:102:gate"); err != nil || gate != "gate-live" {
		t.Fatalf("live location = %q, err = %v", gate, err)
	}
	if server.Exists("zim:gate:gate-dead:conns") {
		t.Fatal("dead gate conns set must be removed")
	}
	if ids, err := server.Members("zim:gate:ids"); err != nil || len(ids) != 1 || ids[0] != "gate-live" {
		t.Fatalf("ids = %v, err = %v", ids, err)
	}
}

func TestRunKeepsLiveGates(t *testing.T) {
	service, server := newService(t, &fakeDiscovery{instances: []*registry.ServiceInstance{{ID: "gate-live"}}})
	if _, err := server.SAdd("zim:gate:ids", "gate-live"); err != nil {
		t.Fatal(err)
	}
	if _, err := server.SAdd("zim:gate:gate-live:conns", "101"); err != nil {
		t.Fatal(err)
	}
	if err := server.Set("zim:locate:user:101:gate", "gate-live"); err != nil {
		t.Fatal(err)
	}

	stats, err := service.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeadGates != 0 || stats.Deleted != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if !server.Exists("zim:locate:user:101:gate") {
		t.Fatal("live location must stay")
	}
}

func TestRunDiscoveryError(t *testing.T) {
	service, _ := newService(t, &fakeDiscovery{err: errors.New("nacos down")})
	if _, err := service.Run(context.Background()); err == nil {
		t.Fatal("discovery error must surface")
	}
}

func TestRunWithoutDependencies(t *testing.T) {
	service := New(nil, nil, "")
	stats, err := service.Run(context.Background())
	if err != nil || stats != (Stats{}) {
		t.Fatalf("stats = %+v, err = %v", stats, err)
	}
}
