package syncsvc

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/conversation/server"
)

type fakeConversations struct {
	items      []*conversation.Item
	minSeq     int64
	maxSeq     int64
	listErr    error
	visibleErr error
}

func (f *fakeConversations) List(_ context.Context, _ int64, offset, limit int) ([]*conversation.Item, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if offset >= len(f.items) {
		return nil, nil
	}
	end := offset + limit
	if end > len(f.items) {
		end = len(f.items)
	}
	return f.items[offset:end], nil
}

func (f *fakeConversations) VisibleRange(context.Context, int64, string) (int64, int64, error) {
	return f.minSeq, f.maxSeq, f.visibleErr
}

type fakeHistory struct {
	messages map[int64]*model.Message
	err      error
}

func (f *fakeHistory) Pull(_ context.Context, _ string, begin, end int64, limit int) ([]*model.Message, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	messages := make([]*model.Message, 0, end-begin+1)
	for seq := begin; seq <= end; seq++ {
		if message, ok := f.messages[seq]; ok {
			messages = append(messages, message)
		}
	}
	if len(messages) > limit {
		messages = messages[:limit]
	}
	return messages, len(messages) >= limit, nil
}

type fakeVersions struct {
	version int64
	err     error
}

func (f *fakeVersions) Current(context.Context, int64) (int64, error) {
	return f.version, f.err
}

func item(convID string, lastSeq int64) *conversation.Item {
	return &conversation.Item{ConversationID: convID, LastMsgSeq: lastSeq}
}

func TestBuildState(t *testing.T) {
	service := New(&fakeConversations{items: []*conversation.Item{
		item("g_1", 10), item("s_1_2", 3),
	}}, &fakeHistory{}, &fakeVersions{version: 42})

	state, err := service.BuildState(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if state.ConversationVersion != 42 || state.FriendVersion != 42 {
		t.Fatalf("state = %+v", state)
	}
	if state.MaxSeqs["g_1"] != 10 || state.MaxSeqs["s_1_2"] != 3 {
		t.Fatalf("max_seqs = %v", state.MaxSeqs)
	}
	if state.Truncated {
		t.Fatal("must not be truncated")
	}
}

func TestBuildStateTruncatedAtLimit(t *testing.T) {
	items := make([]*conversation.Item, 0, 250)
	for i := 0; i < 250; i++ {
		items = append(items, item("g_"+strconv.Itoa(i), int64(i+1)))
	}
	service := New(&fakeConversations{items: items}, &fakeHistory{}, &fakeVersions{version: 1})
	state, err := service.BuildState(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.MaxSeqs) != 200 || !state.Truncated {
		t.Fatalf("len = %d, truncated = %v", len(state.MaxSeqs), state.Truncated)
	}
}

func TestCheckRequiresFullSyncWhenEmpty(t *testing.T) {
	service := New(&fakeConversations{}, &fakeHistory{}, &fakeVersions{version: 9})
	result, err := service.Check(context.Background(), 7, Request{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.FullSyncRequired || len(result.Diffs) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestCheckClientAheadRequiresFullSync(t *testing.T) {
	service := New(&fakeConversations{}, &fakeHistory{}, &fakeVersions{version: 9})
	result, err := service.Check(context.Background(), 7, Request{ConversationVersion: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !result.FullSyncRequired {
		t.Fatal("client ahead must trigger full sync")
	}
}

func TestCheckDiffs(t *testing.T) {
	service := New(&fakeConversations{items: []*conversation.Item{
		item("g_1", 10), item("g_2", 5),
	}}, &fakeHistory{}, &fakeVersions{version: 20})

	result, err := service.Check(context.Background(), 7, Request{
		ConversationVersion: 20,
		MaxSeqs:             map[string]int64{"g_1": 4, "g_2": 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FullSyncRequired || result.Truncated {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Diffs) != 1 {
		t.Fatalf("diffs = %+v", result.Diffs)
	}
	diff := result.Diffs[0]
	if diff.ConversationID != "g_1" || diff.FromSeq != 5 || diff.ToSeq != 10 {
		t.Fatalf("diff = %+v", diff)
	}
	if result.FriendChanged {
		t.Fatal("friend version equal must not change")
	}
}

func TestCheckNewConversation(t *testing.T) {
	service := New(&fakeConversations{items: []*conversation.Item{
		item("g_1", 10), item("g_3", 3),
	}}, &fakeHistory{}, &fakeVersions{version: 20})

	result, err := service.Check(context.Background(), 7, Request{
		ConversationVersion: 20,
		MaxSeqs:             map[string]int64{"g_1": 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diffs) != 1 || result.Diffs[0].ConversationID != "g_3" ||
		result.Diffs[0].FromSeq != 1 || result.Diffs[0].ToSeq != 3 {
		t.Fatalf("diffs = %+v", result.Diffs)
	}
}

func TestCheckFriendChanged(t *testing.T) {
	service := New(&fakeConversations{}, &fakeHistory{}, &fakeVersions{version: 20})
	result, err := service.Check(context.Background(), 7, Request{
		ConversationVersion: 20,
		FriendVersion:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.FriendChanged {
		t.Fatal("friend version behind must report friend_changed")
	}
}

func TestCheckTruncatesDiffs(t *testing.T) {
	items := make([]*conversation.Item, 0, 12)
	for i := 0; i < 12; i++ {
		items = append(items, item("g_"+strconv.Itoa(i), 1))
	}
	service := New(&fakeConversations{items: items}, &fakeHistory{}, &fakeVersions{version: 1})
	service.maxDiffs = 10
	result, err := service.Check(context.Background(), 7, Request{
		ConversationVersion: 1,
		MaxSeqs:             map[string]int64{"g_0": 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Diffs) != 10 {
		t.Fatalf("truncated = %v, diffs = %d", result.Truncated, len(result.Diffs))
	}
}

func TestMissing(t *testing.T) {
	service := New(&fakeConversations{minSeq: 1, maxSeq: 100}, &fakeHistory{messages: map[int64]*model.Message{
		5: {Seq: 5}, 7: {Seq: 7},
	}}, nil)

	messages, missing, err := service.Missing(context.Background(), 7, "g_1", []int64{5, 7, 50}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || len(missing) != 1 || missing[0] != 50 {
		t.Fatalf("messages = %d, missing = %v", len(messages), missing)
	}
}

func TestMissingRespectsVisibleRange(t *testing.T) {
	service := New(&fakeConversations{minSeq: 10, maxSeq: 20}, &fakeHistory{messages: map[int64]*model.Message{
		15: {Seq: 15},
	}}, nil)

	messages, missing, err := service.Missing(context.Background(), 7, "g_1", []int64{5, 15, 25}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Seq != 15 {
		t.Fatalf("messages = %+v", messages)
	}
	if len(missing) != 2 || missing[0] != 5 || missing[1] != 25 {
		t.Fatalf("missing = %v", missing)
	}
}

func TestMissingDeduplicates(t *testing.T) {
	service := New(&fakeConversations{minSeq: 1, maxSeq: 10}, &fakeHistory{messages: map[int64]*model.Message{
		5: {Seq: 5},
	}}, nil)
	messages, missing, err := service.Missing(context.Background(), 7, "g_1", []int64{5, 5, 5}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || len(missing) != 0 {
		t.Fatalf("messages = %d, missing = %v", len(messages), missing)
	}
}

func TestMissingInvalidRequest(t *testing.T) {
	service := New(&fakeConversations{}, &fakeHistory{}, nil)
	if _, _, err := service.Missing(context.Background(), 7, "", []int64{1}, 10); err == nil {
		t.Fatal("empty conversation must be rejected")
	}
	if _, _, err := service.Missing(context.Background(), 7, "g_1", nil, 10); err == nil {
		t.Fatal("empty seq list must be rejected")
	}
}

func TestBuildStateVersionError(t *testing.T) {
	service := New(&fakeConversations{}, &fakeHistory{}, &fakeVersions{err: errors.New("redis down")})
	if _, err := service.BuildState(context.Background(), 7); err == nil {
		t.Fatal("version error must surface")
	}
}
