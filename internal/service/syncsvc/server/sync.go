// Package syncsvc implements the multi-device sync protocol (ADR-0023):
// login SyncState, 1601 SyncCheck and 1514 SyncMissing.
package syncsvc

import (
	"context"
	"sort"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/conversation/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// New returns a sync service.
func New(conv Conversations, history History, versions Versions) *Service {
	return &Service{
		conv:             conv,
		history:          history,
		versions:         versions,
		now:              time.Now,
		maxConversations: 200,
		maxDiffs:         500,
		maxMissing:       200,
		pageSize:         100,
	}
}

// BuildState returns the SyncState sent in the login response.
func (s *Service) BuildState(ctx context.Context, uid int64) (State, error) {
	state := State{MaxSeqs: make(map[string]int64)}

	if s.versions != nil {
		version, err := s.versions.Current(ctx, uid)
		if err != nil {
			return state, err
		}
		state.ConversationVersion = version
		state.FriendVersion = version
	}

	items, truncated, err := s.listConversations(ctx, uid)
	if err != nil {
		return state, err
	}
	state.Truncated = truncated
	for _, item := range items {
		if item.ConversationID == "" {
			continue
		}
		state.MaxSeqs[item.ConversationID] = item.LastMsgSeq
	}
	return state, nil
}

// Check compares the client state with the server and returns the missing windows.
func (s *Service) Check(ctx context.Context, uid int64, req Request) (CheckResult, error) {
	state, err := s.BuildState(ctx, uid)
	if err != nil {
		return CheckResult{}, err
	}

	result := CheckResult{
		ConversationVersion: state.ConversationVersion,
		FriendVersion:       state.FriendVersion,
		ServerTime:          s.now().UnixMilli(),
	}
	if req.ConversationVersion == 0 && len(req.MaxSeqs) == 0 {
		result.FullSyncRequired = true
	}
	if req.ConversationVersion > state.ConversationVersion {
		// Client is ahead of the server (state reset or clock skew): full sync.
		result.FullSyncRequired = true
	}
	if result.FullSyncRequired {
		return result, nil
	}
	if req.FriendVersion > 0 && req.FriendVersion < state.FriendVersion {
		result.FriendChanged = true
	}

	diffs := make([]Diff, 0, len(req.MaxSeqs)+len(state.MaxSeqs))
	for conversationID, clientSeq := range req.MaxSeqs {
		serverSeq, ok := state.MaxSeqs[conversationID]
		if !ok || serverSeq <= clientSeq {
			continue
		}
		if clientSeq < 0 {
			clientSeq = 0
		}
		diffs = append(diffs, Diff{ConversationID: conversationID, FromSeq: clientSeq + 1, ToSeq: serverSeq})
	}
	for conversationID, serverSeq := range state.MaxSeqs {
		if _, ok := req.MaxSeqs[conversationID]; ok || serverSeq <= 0 {
			continue
		}
		diffs = append(diffs, Diff{ConversationID: conversationID, FromSeq: 1, ToSeq: serverSeq})
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].ConversationID < diffs[j].ConversationID })
	if len(diffs) > s.maxDiffs {
		diffs = diffs[:s.maxDiffs]
		result.Truncated = true
	}
	result.Diffs = diffs
	return result, nil
}

// Missing returns the requested messages that still exist and the seqs that do not.
func (s *Service) Missing(ctx context.Context, uid int64, conversationID string, seqs []int64,
	limit int) ([]*model.Message, []int64, error) {
	if conversationID == "" || len(seqs) == 0 {
		return nil, nil, errs.ErrMessageEmpty
	}
	if limit <= 0 || limit > s.maxMissing {
		limit = s.maxMissing
	}
	if len(seqs) > limit {
		seqs = seqs[:limit]
	}

	minSeq, maxSeq, err := s.conv.VisibleRange(ctx, uid, conversationID)
	if err != nil {
		return nil, nil, err
	}

	requested := make([]int64, 0, len(seqs))
	missing := make([]int64, 0)
	for _, seq := range seqs {
		if seq <= 0 || seq < minSeq || seq > maxSeq {
			missing = append(missing, seq)
			continue
		}
		requested = append(requested, seq)
	}
	requested = uniqueSorted(requested)

	found := make(map[int64]*model.Message, len(requested))
	for _, window := range mergeWindows(requested) {
		for begin := window[0]; begin <= window[1]; begin += int64(s.pageSize) {
			end := begin + int64(s.pageSize) - 1
			if end > window[1] {
				end = window[1]
			}
			messages, _, pullErr := s.history.Pull(ctx, conversationID, begin, end, s.pageSize)
			if pullErr != nil {
				return nil, nil, pullErr
			}
			for _, message := range messages {
				found[message.Seq] = message
			}
		}
	}

	messages := make([]*model.Message, 0, len(requested))
	for _, seq := range requested {
		if message, ok := found[seq]; ok {
			messages = append(messages, message)
			continue
		}
		missing = append(missing, seq)
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return messages, missing, nil
}

func (s *Service) listConversations(ctx context.Context, uid int64) ([]*conversation.Item, bool, error) {
	items := make([]*conversation.Item, 0, s.maxConversations)
	for len(items) < s.maxConversations {
		page, err := s.conv.List(ctx, uid, len(items), s.pageSize)
		if err != nil {
			return nil, false, err
		}
		items = append(items, page...)
		if len(page) < s.pageSize {
			return items, false, nil
		}
	}
	return items, true, nil
}

func uniqueSorted(seqs []int64) []int64 {
	sorted := append([]int64(nil), seqs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	out := sorted[:0]
	for _, seq := range sorted {
		if len(out) == 0 || out[len(out)-1] != seq {
			out = append(out, seq)
		}
	}
	return out
}

func mergeWindows(seqs []int64) [][2]int64 {
	windows := make([][2]int64, 0, len(seqs))
	for _, seq := range seqs {
		if len(windows) > 0 && seq == windows[len(windows)-1][1]+1 {
			windows[len(windows)-1][1] = seq
			continue
		}
		windows = append(windows, [2]int64{seq, seq})
	}
	return windows
}
