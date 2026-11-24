package burn

import (
	"context"
	"testing"
)

type fakeBurnRepo struct {
	rows   []*burnMessage
	erased []string
	burned bool
}

func (f *fakeBurnRepo) FindBurnExpired(_ context.Context, _ int64, limit int) ([]*burnMessage, error) {
	if limit > len(f.rows) {
		limit = len(f.rows)
	}
	return f.rows[:limit], nil
}

func (f *fakeBurnRepo) Burn(_ context.Context, serverMsgID string) error {
	f.erased = append(f.erased, serverMsgID)
	f.burned = true
	return nil
}

type fakePublisher struct {
	records map[string][][]byte
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{records: make(map[string][][]byte)}
}

func (f *fakePublisher) Publish(_ context.Context, topic, _ string, value []byte) error {
	f.records[topic] = append(f.records[topic], value)
	return nil
}

func TestSweepErasesAndPublishes(t *testing.T) {
	repo := &fakeBurnRepo{rows: []*burnMessage{
		{ServerMsgID: "m1", ConversationID: "s_1_2", Seq: 3, SendID: 1, RecvID: 2, MsgType: 2},
		{ServerMsgID: "m2", ConversationID: "s_1_2", Seq: 4, SendID: 1, RecvID: 2, MsgType: 1},
	}}
	toPush := newFakePublisher()
	media := newFakePublisher()
	sweeper := New(repo, toPush, media)
	result, err := sweeper.Run(context.Background(), 10)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.Erased != 2 || len(repo.erased) != 2 {
		t.Fatalf("erased = %d, want 2", result.Erased)
	}
	if result.Pushed != 2 {
		t.Fatalf("pushed = %d, want 2", result.Pushed)
	}
	// Only media messages (type 2..5) queue a media delete.
	if result.MediaQueued != 1 {
		t.Fatalf("media queued = %d, want 1", result.MediaQueued)
	}
	if len(toPush.records["zim.pipe.toPush"]) != 2 {
		t.Fatalf("toPush records = %d, want 2", len(toPush.records["zim.pipe.toPush"]))
	}
	if len(media.records["zim.pipe.burnMediaDelete"]) != 1 {
		t.Fatalf("media records = %d, want 1", len(media.records["zim.pipe.burnMediaDelete"]))
	}
}

func TestSweepDegradesWithoutPublishers(t *testing.T) {
	repo := &fakeBurnRepo{rows: []*burnMessage{
		{ServerMsgID: "m1", ConversationID: "s_1_2", Seq: 3, SendID: 1, RecvID: 2, MsgType: 1},
	}}
	sweeper := New(repo, nil, nil)
	result, err := sweeper.Run(context.Background(), 10)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.Erased != 1 || result.Pushed != 0 || result.MediaQueued != 0 {
		t.Fatalf("result = %+v, want erase-only", result)
	}
}

func TestObjectKeyExtraction(t *testing.T) {
	if key := objectKey(`{"url":"/v1/files/3803021/202611/48c7.png"}`); key != "3803021/202611/48c7.png" {
		t.Fatalf("object key = %q", key)
	}
	if key := objectKey(`{"text":"hi"}`); key != "" {
		t.Fatalf("non-media content key = %q, want empty", key)
	}
	if key := objectKey(""); key != "" {
		t.Fatalf("empty content key = %q, want empty", key)
	}
	if key := objectKey("not json"); key != "" {
		t.Fatalf("invalid json key = %q, want empty", key)
	}
}
