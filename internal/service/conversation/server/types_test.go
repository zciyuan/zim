package conversation

import (
	"context"
	"sync"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

type fakeRepo struct {
	mu       sync.Mutex
	convs    map[string]*model.Conversation
	seqUsers map[string]*model.SeqUser
	batches  int
	lastSize int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		convs:    make(map[string]*model.Conversation),
		seqUsers: make(map[string]*model.SeqUser),
	}
}

func (f *fakeRepo) key(uid int64, convID string) string {
	return string(rune(uid)) + ":" + convID
}

func (f *fakeRepo) Create(_ context.Context, conv *model.Conversation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.key(conv.OwnerID, conv.ConvID)
	if _, ok := f.convs[key]; !ok {
		copy := *conv
		f.convs[key] = &copy
	}
	return nil
}

func (f *fakeRepo) Get(_ context.Context, ownerID int64, convID string) (*model.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	conv, ok := f.convs[f.key(ownerID, convID)]
	if !ok {
		return nil, errs.ErrConversationNotExist
	}
	return conv, nil
}

func (f *fakeRepo) TouchLastMessageBatch(ctx context.Context, conversations []*model.Conversation) error {
	for _, conv := range conversations {
		if err := f.TouchLastMessage(ctx, conv); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeRepo) TouchLastMessage(_ context.Context, conv *model.Conversation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.key(conv.OwnerID, conv.ConvID)
	f.convs[key] = conv
	return nil
}

func (f *fakeRepo) List(_ context.Context, ownerID int64, _, _ int) ([]*model.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*model.Conversation, 0)
	for _, conv := range f.convs {
		if conv.OwnerID == ownerID && conv.Deleted == 0 {
			items = append(items, conv)
		}
	}
	return items, nil
}

func (f *fakeRepo) Delete(_ context.Context, ownerID int64, convID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	conv, ok := f.convs[f.key(ownerID, convID)]
	if !ok {
		return errs.ErrConversationNotExist
	}
	conv.Deleted = 1
	return nil
}

func (f *fakeRepo) SetPin(_ context.Context, ownerID int64, convID string, pinned int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	conv, ok := f.convs[f.key(ownerID, convID)]
	if !ok {
		return errs.ErrConversationNotExist
	}
	conv.IsPinned = pinned
	return nil
}

func (f *fakeRepo) SetRecvOpt(_ context.Context, ownerID int64, convID string, opt int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	conv, ok := f.convs[f.key(ownerID, convID)]
	if !ok {
		return errs.ErrConversationNotExist
	}
	conv.RecvOpt = opt
	return nil
}

func (f *fakeRepo) GetSeqUser(_ context.Context, uid int64, convID string) (*model.SeqUser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state, ok := f.seqUsers[f.key(uid, convID)]; ok {
		return state, nil
	}
	return &model.SeqUser{UID: uid, ConversationID: convID}, nil
}

func (f *fakeRepo) SetReadSeq(_ context.Context, uid int64, convID string, seq int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.seqUsers[f.key(uid, convID)]
	if !ok {
		state = &model.SeqUser{UID: uid, ConversationID: convID}
		f.seqUsers[f.key(uid, convID)] = state
	}
	if seq > state.ReadSeq {
		state.ReadSeq = seq
	}
	if conv, ok := f.convs[f.key(uid, convID)]; ok && seq > conv.ReadSeq {
		conv.ReadSeq = seq
	}
	return nil
}

func (f *fakeRepo) RaiseMin(_ context.Context, uid int64, convID string, seq int64) error {
	return f.SetReadSeq(context.Background(), uid, convID, seq)
}

type fakeReadCache struct {
	values map[string]int64
	sets   int
}

func (f *fakeReadCache) Get(_ context.Context, convID string, uid int64) (int64, bool, error) {
	value, ok := f.values[readSeqKey(convID, uid)]
	return value, ok, nil
}

func (f *fakeReadCache) Set(_ context.Context, convID string, uid, seq int64) error {
	if f.values == nil {
		f.values = make(map[string]int64)
	}
	f.values[readSeqKey(convID, uid)] = seq
	f.sets++
	return nil
}

func (f *fakeRepo) TouchPreview(_ context.Context, conv *model.Conversation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy := *conv
	f.convs[f.key(conv.OwnerID, conv.ConvID)] = &copy
	return nil
}

func (f *fakeRepo) PreviewSeq(_ context.Context, convID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, conv := range f.convs {
		if conv.ConvID == convID {
			return conv.LastMsgSeq, nil
		}
	}
	return 0, nil
}

func (f *fakeRepo) TouchPreviewBatch(_ context.Context, conversations []*model.Conversation) error {
	for _, conv := range conversations {
		_ = f.TouchPreview(context.Background(), conv)
	}
	return nil
}

func (f *fakeRepo) GetSeqUsers(_ context.Context, uid int64, convIDs []string) (map[string]*model.SeqUser, error) {
	result := make(map[string]*model.SeqUser, len(convIDs))
	for _, convID := range convIDs {
		key := f.key(uid, convID)
		if state, ok := f.seqUsers[key]; ok {
			result[convID] = state
		}
	}
	return result, nil
}

func (f *fakeReadCache) GetMany(_ context.Context, convIDs []string, uid int64) (map[string]int64, error) {
	result := make(map[string]int64, len(convIDs))
	for _, convID := range convIDs {
		if value, ok := f.values[readSeqKey(convID, uid)]; ok {
			result[convID] = value
		}
	}
	return result, nil
}

func (f *fakeRepo) SetReadBatch(ctx context.Context, marks []*model.ReadMark) error {
	f.batches++
	f.lastSize = len(marks)
	for _, mark := range marks {
		if err := f.SetReadSeq(ctx, mark.UID, mark.ConvID, mark.Seq); err != nil {
			return err
		}
	}
	return nil
}
