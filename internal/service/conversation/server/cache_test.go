package conversation

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
)

func TestReadCacheAndUnread(t *testing.T) {
	repo := newFakeRepo()
	cache := &fakeReadCache{values: map[string]int64{readSeqKey("s_1_2", 1): 3}}
	service := New(repo, cache)
	_ = repo.Create(context.Background(), &model.Conversation{OwnerID: 1, ConvID: "s_1_2", LastMsgSeq: 5})
	items, err := service.List(context.Background(), 1, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if items[0].ReadSeq != 3 || items[0].Unread != 2 {
		t.Fatalf("item = %+v", items[0])
	}
	if _, err = service.Read(context.Background(), 1, "s_1_2", 4); err != nil {
		t.Fatal(err)
	}
	if cache.values[readSeqKey("s_1_2", 1)] != 4 || cache.sets != 1 {
		t.Fatalf("cache = %+v", cache)
	}
	service.Flush(context.Background())
	state, _ := repo.GetSeqUser(context.Background(), 1, "s_1_2")
	if state.ReadSeq != 4 {
		t.Fatalf("flushed read_seq = %d, want 4", state.ReadSeq)
	}
	unread, err := service.Unread(context.Background(), 1, "s_1_2")
	if err != nil || unread != 1 {
		t.Fatalf("unread = %d, err = %v", unread, err)
	}
}

func TestServiceLifecycle(t *testing.T) {
	repo := newFakeRepo()
	service := New(repo, &fakeReadCache{})
	if service.Name() != "conversation-readseq" {
		t.Fatalf("name = %s", service.Name())
	}
	service.Start()
	service.Start()
	service.Close()
	service.Close()
	service.Flush(context.Background())
	New(repo).Close()
}
