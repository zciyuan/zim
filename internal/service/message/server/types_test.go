package message

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	daomessage "github.com/zciyuan/ZIM/internal/dao/message"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/idem"
)

type fakeRepo struct {
	mu       sync.Mutex
	messages []*model.Message
}

func (f *fakeRepo) Append(_ context.Context, msg *model.Message) (*daomessage.AppendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.messages {
		if existing.ClientMsgID == msg.ClientMsgID {
			return &daomessage.AppendResult{ServerMsgID: existing.ServerMsgID, Seq: existing.Seq, Duplicate: true}, nil
		}
	}
	f.messages = append(f.messages, msg)
	return &daomessage.AppendResult{ServerMsgID: msg.ServerMsgID, Seq: msg.Seq}, nil
}

func (f *fakeRepo) FindBySeq(_ context.Context, _ string, begin, end int64, limit int) ([]*model.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeRepo) FindOne(_ context.Context, id string) (*model.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.messages {
		if msg.ServerMsgID == id {
			return msg, nil
		}
	}
	return nil, ErrRepositoryMissing
}

func (f *fakeRepo) Recall(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.messages {
		if msg.ServerMsgID == id {
			msg.Status = model.MessageRecalled
		}
	}
	return nil
}

type fakeSeq struct {
	mu   sync.Mutex
	next int64
}

func (f *fakeSeq) Next(_ context.Context, _ string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	return f.next, nil
}

type fakeIdem struct {
	mu      sync.Mutex
	results map[string]*idem.Result
}

func (f *fakeIdem) Reserve(_ context.Context, uid int64, clientMsgID string) (*idem.Result, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := idemKey(uid, clientMsgID)
	if result, ok := f.results[key]; ok {
		if result == nil {
			return nil, false, idem.ErrPending
		}
		return result, false, nil
	}
	f.results[key] = nil
	return nil, true, nil
}

func (f *fakeIdem) Complete(_ context.Context, uid int64, clientMsgID string, result *idem.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[idemKey(uid, clientMsgID)] = result
	return nil
}

func (f *fakeIdem) Release(_ context.Context, uid int64, clientMsgID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.results, idemKey(uid, clientMsgID))
	return nil
}

type fakeConv struct {
	mu    sync.Mutex
	touch []*model.Conversation
}

func (f *fakeConv) Touch(_ context.Context, conv *model.Conversation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touch = append(f.touch, conv)
	return nil
}

type fakeDir struct {
	membership *Membership
	memberErr  error
	friend     bool
	black      bool
}

func (f *fakeDir) Membership(_ context.Context, _, _ int64) (*Membership, error) {
	return f.membership, f.memberErr
}

func (f *fakeDir) Relation(_ context.Context, _, _ int64) (bool, bool, error) {
	return f.friend, f.black, nil
}

func idemKey(uid int64, clientMsgID string) string {
	return strconv.FormatInt(uid, 10) + ":" + clientMsgID
}

type fakeResultQueue struct {
	result *SendResult
	err    error
	uid    int64
	client string
}

func (f *fakeResultQueue) Publish(context.Context, int64, string, *SendResult) error { return nil }

func (f *fakeResultQueue) Wait(_ context.Context, uid int64, client string, _ time.Duration) (*SendResult, error) {
	f.uid = uid
	f.client = client
	return f.result, f.err
}

func (f *fakeResultQueue) WaitWithPublish(ctx context.Context, uid int64, client string, _ time.Duration,
	publish func(context.Context) error) (*SendResult, error) {
	f.uid = uid
	f.client = client
	if err := publish(ctx); err != nil {
		return nil, err
	}
	return f.result, f.err
}

// UpdateContent rewrites one message body.
func (f *fakeRepo) UpdateContent(_ context.Context, serverMsgID, content string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.messages {
		if msg.ServerMsgID == serverMsgID {
			msg.Content = content
		}
	}
	return nil
}

// MarkDeleted flags one message as deleted.
func (f *fakeRepo) MarkDeleted(_ context.Context, serverMsgID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.messages {
		if msg.ServerMsgID == serverMsgID {
			msg.Status = model.MessageDeleted
		}
	}
	return nil
}

// Search filters stored messages by substring, newest first.
func (f *fakeRepo) Search(_ context.Context, _ string, keyword string, limit int) ([]*model.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*model.Message, 0)
	for i := len(f.messages) - 1; i >= 0; i-- {
		msg := f.messages[i]
		if strings.Contains(msg.Content, keyword) {
			result = append(result, msg)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}
