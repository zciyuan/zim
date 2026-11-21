package reaction

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestService(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{server.Addr()}})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), server
}

func TestAddAndList(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	if err := service.Add(ctx, "s_1_2", 10, 1, "👍"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Idempotent per (uid, emoji).
	if err := service.Add(ctx, "s_1_2", 10, 1, "👍"); err != nil {
		t.Fatalf("Add duplicate: %v", err)
	}
	if err := service.Add(ctx, "s_1_2", 10, 2, "👍"); err != nil {
		t.Fatalf("Add other: %v", err)
	}

	got, err := service.List(ctx, "s_1_2", []int64{10}, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	items := got[10]
	if len(items) != 1 || items[0].Emoji != "👍" || items[0].Count != 2 || !items[0].Mine {
		t.Fatalf("items = %+v", items)
	}

	other, _ := service.List(ctx, "s_1_2", []int64{10}, 3)
	if len(other[10]) != 1 || other[10][0].Mine {
		t.Fatalf("other view = %+v", other[10])
	}
}

func TestRemove(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	_ = service.Add(ctx, "g_9", 3, 5, "🎉")
	_ = service.Add(ctx, "g_9", 3, 6, "🎉")
	if err := service.Remove(ctx, "g_9", 3, 5, "🎉"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := service.Remove(ctx, "g_9", 3, 5, "🎉"); err != nil {
		t.Fatalf("Remove idempotent: %v", err)
	}
	items, _ := service.List(ctx, "g_9", []int64{3}, 6)
	if len(items[3]) != 1 || items[3][0].Count != 1 {
		t.Fatalf("after remove = %+v", items[3])
	}

	_ = service.Remove(ctx, "g_9", 3, 6, "🎉")
	items, _ = service.List(ctx, "g_9", []int64{3}, 6)
	if len(items[3]) != 0 {
		t.Fatalf("counter not cleaned: %+v", items[3])
	}
}

func TestValidation(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	if err := service.Add(ctx, "", 1, 1, "👍"); err != ErrInvalidArgument {
		t.Fatalf("empty conversation err = %v", err)
	}
	if err := service.Add(ctx, "s_1_2", 1, 1, ""); err != ErrInvalidEmoji {
		t.Fatalf("empty emoji err = %v", err)
	}
	if err := service.Add(ctx, "s_1_2", 1, 1, "123456789"); err != ErrInvalidEmoji {
		t.Fatalf("oversized emoji err = %v", err)
	}

	for i := 0; i < maxEmojis; i++ {
		if err := service.Add(ctx, "s_1_2", 2, 1, string(rune('a'+i))); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if err := service.Add(ctx, "s_1_2", 2, 1, "z"); err != ErrTooManyEmojis {
		t.Fatalf("too many err = %v", err)
	}
}

func TestListMissingMessage(t *testing.T) {
	service, _ := newTestService(t)
	items, err := service.List(context.Background(), "s_1_2", []int64{404}, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %+v", items)
	}
}
