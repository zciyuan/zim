package logic

import (
	"context"
	"testing"

	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	conversation "github.com/zciyuan/ZIM/internal/service/conversation/server"
	syncsvc "github.com/zciyuan/ZIM/internal/service/syncsvc/server"
)

type fakeSyncConversations struct {
	items []*conversation.Item
}

func (f *fakeSyncConversations) List(context.Context, int64, int, int) ([]*conversation.Item, error) {
	return f.items, nil
}

func (f *fakeSyncConversations) VisibleRange(context.Context, int64, string) (int64, int64, error) {
	return 0, 0, nil
}

type fakeSyncVersions struct {
	version int64
}

func (f *fakeSyncVersions) Current(context.Context, int64) (int64, error) {
	return f.version, nil
}

func TestFillSyncState(t *testing.T) {
	core := &core{sync: syncsvc.New(&fakeSyncConversations{items: []*conversation.Item{
		{ConversationID: "g_1", LastMsgSeq: 9},
	}}, nil, &fakeSyncVersions{version: 5})}
	res := &authwire.LoginRes{Uid: "10001"}

	core.fillSyncState(&fakeNodeContext{}, res)

	if res.GetSync() == nil {
		t.Fatal("sync state must be filled")
	}
	if res.GetSync().GetConversationVersion() != 5 {
		t.Fatalf("version = %d", res.GetSync().GetConversationVersion())
	}
	if res.GetSync().GetMaxSeqs()["g_1"] != 9 {
		t.Fatalf("max_seqs = %v", res.GetSync().GetMaxSeqs())
	}
}

func TestFillSyncStateDegrades(t *testing.T) {
	core := &core{}
	res := &authwire.LoginRes{Uid: "10001"}
	core.fillSyncState(&fakeNodeContext{}, res)
	if res.GetSync() != nil {
		t.Fatal("nil service must leave an empty state")
	}

	core.sync = syncsvc.New(&fakeSyncConversations{}, nil, &fakeSyncVersions{})
	res = &authwire.LoginRes{Uid: "not-a-number"}
	core.fillSyncState(&fakeNodeContext{}, res)
	if res.GetSync() != nil {
		t.Fatal("bad uid must leave an empty state")
	}
}
