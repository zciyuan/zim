// Package conversation implements conversation list, read and unread use cases.
package conversation

import (
	"context"
	"strings"
	"time"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	"github.com/zciyuan/ZIM/internal/utils/conv"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// Repository is the conversation persistence surface.
type Repository interface {
	Create(ctx context.Context, conv *model.Conversation) error
	Get(ctx context.Context, ownerID int64, convID string) (*model.Conversation, error)
	TouchLastMessage(ctx context.Context, conv *model.Conversation) error
	TouchLastMessageBatch(ctx context.Context, conversations []*model.Conversation) error
	TouchPreview(ctx context.Context, conv *model.Conversation) error
	TouchPreviewBatch(ctx context.Context, conversations []*model.Conversation) error
	PreviewSeq(ctx context.Context, convID string) (int64, error)
	List(ctx context.Context, ownerID int64, offset, limit int) ([]*model.Conversation, error)
	Delete(ctx context.Context, ownerID int64, convID string) error
	SetPin(ctx context.Context, ownerID int64, convID string, pinned int16) error
	SetRecvOpt(ctx context.Context, ownerID int64, convID string, opt int16) error
	GetSeqUser(ctx context.Context, uid int64, convID string) (*model.SeqUser, error)
	GetSeqUsers(ctx context.Context, uid int64, convIDs []string) (map[string]*model.SeqUser, error)
	SetReadSeq(ctx context.Context, uid int64, convID string, seq int64) error
	SetReadBatch(ctx context.Context, marks []*model.ReadMark) error
	RaiseMin(ctx context.Context, uid int64, convID string, seq int64) error
}

// New returns a conversation service. An optional cache enables Redis read_seq.
func New(repo Repository, caches ...ReadCache) *Service {
	service := &Service{repo: repo, now: time.Now, flushCh: make(chan struct{}, 1)}
	if len(caches) > 0 {
		service.cache = caches[0]
	}
	return service
}

// SetChangeLog enables append-only version-log writes.
func (s *Service) SetChangeLog(changes *changelog.Service) {
	s.changes = changes
}

func (s *Service) record(ctx context.Context, uid int64, convID, state string) {
	if s.changes == nil {
		return
	}
	if err := s.changes.Append(ctx, uid, model.EntityConversation, convID, state); err != nil {
		log.Warnf("append conversation change log failed: uid = %d, conv = %s, state = %s, err = %v",
			uid, convID, state, err)
	}
}

func (s *Service) readSeq(ctx context.Context, uid int64, convID string) (int64, error) {
	if s.cache != nil {
		if seq, ok, err := s.cache.Get(ctx, convID, uid); err != nil {
			return 0, err
		} else if ok {
			return seq, nil
		}
	}
	state, err := s.repo.GetSeqUser(ctx, uid, convID)
	if err != nil {
		return 0, err
	}
	return state.ReadSeq, nil
}

// List returns conversations with unread counts.
func (s *Service) List(ctx context.Context, ownerID int64, offset, limit int) ([]*Item, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	conversations, err := s.repo.List(ctx, ownerID, offset, limit)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	cached := make(map[string]int64, len(conversations))
	if s.cache != nil {
		ids := make([]string, 0, len(conversations))
		for _, conv := range conversations {
			ids = append(ids, conv.ConvID)
		}
		if values, cacheErr := s.cache.GetMany(ctx, ids, ownerID); cacheErr == nil {
			cached = values
		}
	}
	items := make([]*Item, 0, len(conversations))
	for _, conv := range conversations {
		readSeq := conv.ReadSeq
		if seq, ok := cached[conv.ConvID]; ok && seq > readSeq {
			readSeq = seq
		}
		if readSeq == 0 && conv.MinSeq > 1 {
			readSeq = conv.MinSeq - 1
		}
		if readSeq == 0 && conv.LastMsgSeq > 0 {
			readSeq = conv.LastMsgSeq - 1
		}
		maxSeq := conv.LastMsgSeq
		unread := maxSeq - readSeq
		if unread < 0 {
			unread = 0
		}
		items = append(items, &Item{
			ConversationID: conv.ConvID,
			Type:           conv.Type,
			PeerID:         conv.PeerID,
			GroupID:        conv.GroupID,
			RecvOpt:        conv.RecvOpt,
			IsPinned:       conv.IsPinned,
			LastMsgID:      conv.LastMsgID,
			LastMsgSeq:     conv.LastMsgSeq,
			LastMsgAt:      conv.LastMsgAt,
			LastMsgBrief:   conv.LastMsgBrief,
			ReadSeq:        readSeq,
			MaxSeq:         maxSeq,
			Unread:         unread,
			UpdatedAt:      conv.UpdatedAt,
		})
	}
	return items, nil
}

// Get returns one conversation owned by ownerID.
func (s *Service) Get(ctx context.Context, ownerID int64, convID string) (*model.Conversation, error) {
	conv, err := s.repo.Get(ctx, ownerID, convID)
	if err != nil {
		return nil, errs.ErrConversationNotExist.WithCause(err)
	}
	return conv, nil
}

// VisibleRange returns the inclusive seq window uid may read in convID.
// It enforces conversation ownership and the user-level min_seq watermark,
// so a member who left a group cannot read messages after the leave point.
func (s *Service) VisibleRange(ctx context.Context, uid int64, convID string) (int64, int64, error) {
	conv, err := s.repo.Get(ctx, uid, convID)
	if err != nil {
		if strings.HasPrefix(convID, "g_") {
			return 0, 0, errs.ErrNotGroupMember.WithCause(err)
		}
		return 0, 0, errs.ErrConversationNotExist.WithCause(err)
	}
	state, err := s.repo.GetSeqUser(ctx, uid, convID)
	if err != nil {
		return 0, 0, errs.ErrInternal.WithCause(err)
	}
	min := state.MinSeq
	if min < 1 {
		min = 1
	}
	max := conv.LastMsgSeq
	if max < 0 {
		max = 0
	}
	return min, max, nil
}

// Delete marks a conversation deleted.
func (s *Service) Delete(ctx context.Context, ownerID int64, convID string) error {
	if err := s.repo.Delete(ctx, ownerID, convID); err != nil {
		return errs.ErrConversationNotExist.WithCause(err)
	}
	s.record(ctx, ownerID, convID, model.ChangeDelete)
	return nil
}

// Pin updates the pin flag.
func (s *Service) Pin(ctx context.Context, ownerID int64, convID string, pinned bool) error {
	flag := int16(0)
	if pinned {
		flag = 1
	}
	if err := s.repo.SetPin(ctx, ownerID, convID, flag); err != nil {
		return errs.ErrConversationNotExist.WithCause(err)
	}
	s.record(ctx, ownerID, convID, model.ChangeUpdate)
	return nil
}

// SetRecvOpt updates the receive option.
func (s *Service) SetRecvOpt(ctx context.Context, ownerID int64, convID string, opt int16) error {
	if err := s.repo.SetRecvOpt(ctx, ownerID, convID, opt); err != nil {
		return errs.ErrConversationNotExist.WithCause(err)
	}
	s.record(ctx, ownerID, convID, model.ChangeUpdate)
	return nil
}

// Name implements due's component interface.
func (s *Service) Name() string { return "conversation-readseq" }

// Init implements due's component lifecycle.
func (s *Service) Init() {}

// Destroy implements due's component lifecycle.
func (s *Service) Destroy() { s.Close() }

// Start starts the async read_seq flusher.
func (s *Service) Start() {
	if s.cache == nil {
		return
	}
	s.startOnce.Do(func() {
		s.stop = make(chan struct{})
		go s.loop()
	})
}

// Close flushes pending read_seq updates and stops the flusher.
func (s *Service) Close() {
	if s.cache == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Flush(ctx)
	})
}

func (s *Service) loop() {
	ticker := time.NewTicker(readFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			s.Flush(ctx)
			cancel()
		case <-s.flushCh:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			s.Flush(ctx)
			cancel()
		case <-s.stop:
			return
		}
	}
}

// Flush 将待落库的已读水位按多用户合并成一批写入，避免高 QPS 下逐条 UPDATE。
func (s *Service) Flush(ctx context.Context) {
	if s.cache == nil {
		return
	}
	updates := make([]*model.ReadMark, 0, readFlushBatch)
	keys := make([]string, 0, readFlushBatch)
	s.pending.Range(func(key, value any) bool {
		if len(updates) >= readFlushBatch {
			return false
		}
		s.pending.Delete(key)
		update := value.(readUpdate)
		keys = append(keys, key.(string))
		updates = append(updates, &model.ReadMark{UID: update.uid, ConvID: update.convID, Seq: update.seq})
		return true
	})
	if len(updates) == 0 {
		return
	}
	if err := s.repo.SetReadBatch(ctx, updates); err != nil {
		metrics.Default.Inc("zim_read_flush_total", map[string]string{"result": "error"})
		for index, key := range keys {
			s.pending.Store(key, readUpdate{uid: updates[index].UID, convID: updates[index].ConvID, seq: updates[index].Seq})
		}
		log.Warnf("flush read_seq batch failed: count = %d, err = %v", len(updates), err)
		return
	}
	metrics.Default.Observe("zim_read_flush_batch_size", nil, int64(len(updates)))
	metrics.Default.Inc("zim_read_flush_total", map[string]string{"result": "ok"})
	s.dirty.Add(-int64(len(updates)))
}

// Read 将 read_seq 前进到 seq，并以会话最大 seq 截断，防止越界。
func (s *Service) Read(ctx context.Context, uid int64, convID string, seq int64) (int64, error) {
	if seq < 0 {
		seq = 0
	}
	conv, err := s.repo.Get(ctx, uid, convID)
	if err != nil {
		return 0, errs.ErrConversationNotExist.WithCause(err)
	}
	maxSeq := conv.LastMsgSeq
	if seq > maxSeq {
		seq = maxSeq
	}
	if s.cache == nil {
		if err = s.repo.SetReadSeq(ctx, uid, convID, seq); err != nil {
			return 0, errs.ErrInternal.WithCause(err)
		}
		return seq, nil
	}
	if err = s.cache.Set(ctx, convID, uid, seq); err != nil {
		// 缓存是快确认路径；缓存不可用时同步落库，避免刷新后已读回退。
		if err = s.repo.SetReadSeq(ctx, uid, convID, seq); err != nil {
			return 0, errs.ErrInternal.WithCause(err)
		}
		return seq, nil
	}
	key := readSeqKey(convID, uid)
	s.pending.Store(key, readUpdate{uid: uid, convID: convID, seq: seq})
	if s.dirty.Add(1) >= readFlushBatch {
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
	}
	return seq, nil
}

// Unread returns max_seq - read_seq.
func (s *Service) Unread(ctx context.Context, uid int64, convID string) (int64, error) {
	readSeq, err := s.readSeq(ctx, uid, convID)
	if err != nil {
		return 0, errs.ErrInternal.WithCause(err)
	}
	conv, err := s.repo.Get(ctx, uid, convID)
	if err != nil {
		return 0, errs.ErrConversationNotExist.WithCause(err)
	}
	if conv.ReadSeq > readSeq {
		readSeq = conv.ReadSeq
	}
	maxSeq := conv.LastMsgSeq
	unread := maxSeq - readSeq
	if unread < 0 {
		unread = 0
	}
	return unread, nil
}

// CreateDirect creates both owners' single-chat conversation rows.
func (s *Service) CreateDirect(ctx context.Context, a, b int64, convID string) error {
	now := s.now().UnixMilli()
	for _, pair := range [][2]int64{{a, b}, {b, a}} {
		if err := s.repo.Create(ctx, &model.Conversation{
			OwnerID: pair[0], ConvID: convID, Type: model.ConversationSingle,
			PeerID: pair[1], UpdatedAt: now,
		}); err != nil {
			return errs.ErrInternal.WithCause(err)
		}
		s.record(ctx, pair[0], convID, model.ChangeInsert)
	}
	return nil
}

// Touch updates a conversation preview after a message.
func (s *Service) Touch(ctx context.Context, conv *model.Conversation) error {
	if err := s.repo.TouchLastMessage(ctx, conv); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	s.record(ctx, conv.OwnerID, conv.ConvID, model.ChangeUpdate)
	return nil
}

// PreviewSeq returns the shared preview sequence.
func (s *Service) PreviewSeq(ctx context.Context, convID string) int64 {
	seq, err := s.repo.PreviewSeq(ctx, convID)
	if err != nil {
		return 0
	}
	return seq
}

// TouchPreviewBatch writes many shared preview rows in one statement.
func (s *Service) TouchPreviewBatch(ctx context.Context, conversations []*model.Conversation) error {
	if err := s.repo.TouchPreviewBatch(ctx, conversations); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// TouchPreview writes the one shared preview row for a conversation.
func (s *Service) TouchPreview(ctx context.Context, conv *model.Conversation) error {
	if err := s.repo.TouchPreview(ctx, conv); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// TouchBatch creates per-owner rows that do not exist yet. It is called once
// per conversation/member, not on every message.
func (s *Service) TouchBatch(ctx context.Context, conversations []*model.Conversation) error {
	if err := s.repo.TouchLastMessageBatch(ctx, conversations); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// SetGroupNotify updates a member's group notification option.
func (s *Service) SetGroupNotify(ctx context.Context, uid, gid int64, opt int16) error {
	convID := conv.Group(gid)
	if err := s.repo.Create(ctx, &model.Conversation{
		OwnerID: uid, ConvID: convID, Type: model.ConversationGroup, GroupID: gid,
		UpdatedAt: s.now().UnixMilli(),
	}); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return s.SetRecvOpt(ctx, uid, convID, opt)
}

// LastSeq returns the owner's last message sequence, or zero when absent.
func (s *Service) LastSeq(ctx context.Context, ownerID int64, convID string) int64 {
	conv, err := s.repo.Get(ctx, ownerID, convID)
	if err != nil {
		return 0
	}
	return conv.LastMsgSeq
}

// RaiseMin moves the visible lower bound, used when leaving/being kicked.
func (s *Service) RaiseMin(ctx context.Context, uid int64, convID string, seq int64) error {
	if err := s.repo.RaiseMin(ctx, uid, convID, seq); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	if s.cache != nil {
		if err := s.cache.Set(ctx, convID, uid, seq); err != nil {
			return errs.ErrInternal.WithCause(err)
		}
	}
	return nil
}

// Clear raises the visible lower bound to the latest message.
func (s *Service) Clear(ctx context.Context, uid int64, convID string) (int64, error) {
	conv, err := s.repo.Get(ctx, uid, convID)
	if err != nil {
		return 0, errs.ErrConversationNotExist.WithCause(err)
	}
	if conv.LastMsgSeq <= 0 {
		return 0, nil
	}
	if err = s.repo.RaiseMin(ctx, uid, convID, conv.LastMsgSeq); err != nil {
		return 0, errs.ErrInternal.WithCause(err)
	}
	s.record(ctx, uid, convID, model.ChangeUpdate)
	return conv.LastMsgSeq, nil
}
