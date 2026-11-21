package history

import (
	"context"
	"strings"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeRepo struct {
	messages []*model.Message
}

func (f *fakeRepo) FindBySeq(_ context.Context, _ string, begin, end int64, limit int) ([]*model.Message, error) {
	result := make([]*model.Message, 0)
	for _, msg := range f.messages {
		if msg.Seq >= begin && msg.Seq <= end {
			result = append(result, msg)
		}
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (f *fakeRepo) Search(_ context.Context, _ string, keyword string, limit int) ([]*model.Message, error) {
	result := make([]*model.Message, 0)
	for _, msg := range f.messages {
		if strings.Contains(msg.Content, keyword) {
			result = append(result, msg)
		}
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func TestPull(t *testing.T) {
	service := New(&fakeRepo{messages: []*model.Message{{Seq: 1}, {Seq: 2}, {Seq: 3}}})
	messages, hasMore, err := service.Pull(context.Background(), "s_1_2", 1, 2, 10)
	if err != nil || len(messages) != 2 || hasMore {
		t.Fatalf("messages = %d, hasMore = %v, err = %v", len(messages), hasMore, err)
	}
	if _, _, err = service.Pull(context.Background(), "", 1, 2, 10); err == nil {
		t.Fatal("empty conversation must fail")
	}
}
