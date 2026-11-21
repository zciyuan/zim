package maintenance

import (
	"context"
	"testing"
	"time"
)

type fakeFriends struct{ count int64 }

func (f *fakeFriends) DeleteExpiredRequests(_ context.Context, _ int64) (int64, error) {
	return f.count, nil
}

type fakeMessages struct{ count int64 }

func (f *fakeMessages) DeleteBefore(_ context.Context, _ int64) (int64, error) {
	return f.count, nil
}

func TestRun(t *testing.T) {
	service := New(&fakeFriends{count: 2}, &fakeMessages{count: 3}, 24*time.Hour)
	stats, err := service.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.FriendRequests != 2 || stats.MessageBlocks != 3 {
		t.Fatalf("stats = %+v", stats)
	}
}
