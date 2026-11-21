package route

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

type fakeRegistry struct {
	added   []int64
	removed []int64
	err     error
}

func (f *fakeRegistry) Add(_ context.Context, _ string, deviceKey int64) error {
	if f.err != nil {
		return f.err
	}
	f.added = append(f.added, deviceKey)
	return nil
}

func (f *fakeRegistry) AddUserDevice(_ context.Context, _, _ int64) error {
	return f.err
}

func (f *fakeRegistry) RemoveUserDevice(_ context.Context, _, _ int64) error {
	return f.err
}

func (f *fakeRegistry) Remove(_ context.Context, _ string, deviceKey int64) error {
	if f.err != nil {
		return f.err
	}
	f.removed = append(f.removed, deviceKey)
	return nil
}

func TestRouterIndexesBindAndUnbind(t *testing.T) {
	registry := &fakeRegistry{}
	router, err := New(&fakeLocator{}, "g1", WithConnRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = router.Bind(ctx, 77, 10001, multi.PlatformAndroid, 1); err != nil {
		t.Fatal(err)
	}
	want := multi.Key(10001, multi.PlatformAndroid, 1)
	if len(registry.added) != 1 || registry.added[0] != want {
		t.Fatalf("indexed = %v, want %d", registry.added, want)
	}
	if err = router.Unbind(ctx, 10001, multi.PlatformAndroid, 1); err != nil {
		t.Fatal(err)
	}
	if len(registry.removed) != 1 || registry.removed[0] != want {
		t.Fatalf("removed = %v, want %d", registry.removed, want)
	}
}

func TestRouterIndexFailureDoesNotFailBind(t *testing.T) {
	registry := &fakeRegistry{err: errors.New("redis down")}
	router, err := New(&fakeLocator{}, "g1", WithConnRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err = router.Bind(context.Background(), 1, 10001, multi.PlatformIOS, 1); err != nil {
		t.Fatalf("index failure must not fail bind: %v", err)
	}
}

func TestRedisConnRegistry(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	registry, err := NewRedisConnRegistry(client, "zim")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = registry.Add(ctx, "gate-a", 101); err != nil {
		t.Fatal(err)
	}
	if err = registry.Add(ctx, "gate-b", 102); err != nil {
		t.Fatal(err)
	}

	members, err := server.Members(registry.ConnsKey("gate-a"))
	if err != nil || len(members) != 1 || members[0] != "101" {
		t.Fatalf("members = %v, err = %v", members, err)
	}
	ids, err := server.Members(registry.IDsKey())
	if err != nil || len(ids) != 2 {
		t.Fatalf("ids = %v, err = %v", ids, err)
	}
	if err = registry.Remove(ctx, "gate-a", 101); err != nil {
		t.Fatal(err)
	}
	// miniredis drops an empty set, so "no such key" is the expected success case.
	if members, memberErr := server.Members(registry.ConnsKey("gate-a")); memberErr == nil && len(members) != 0 {
		t.Fatalf("members after remove = %v", members)
	}
}

func TestNewRedisConnRegistryRequiresClient(t *testing.T) {
	if _, err := NewRedisConnRegistry(nil, "zim"); err == nil {
		t.Fatal("nil client must be rejected")
	}
}
