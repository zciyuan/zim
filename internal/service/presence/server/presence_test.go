package presence

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestOnline(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	if _, err := server.SAdd("zim:udev:10001", "123"); err != nil {
		t.Fatal(err)
	}
	service := New(client, "zim")
	online, err := service.Online(context.Background(), []int64{10001, 10002})
	if err != nil {
		t.Fatal(err)
	}
	if !online[10001] || online[10002] {
		t.Fatalf("online = %v", online)
	}
}
