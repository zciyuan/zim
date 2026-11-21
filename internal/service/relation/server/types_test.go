package server

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
	webhookserver "github.com/zciyuan/ZIM/internal/service/webhook/server"
)

// recordingNotifier captures webhook events emitted by the relation service.
type recordingNotifier struct {
	mu     sync.Mutex
	events []webhookserver.Event
}

// Notify records one webhook event.
func (n *recordingNotifier) Notify(event webhookserver.Event) {
	n.mu.Lock()
	n.events = append(n.events, event)
	n.mu.Unlock()
}

// list returns a copy of the recorded events.
func (n *recordingNotifier) list() []webhookserver.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]webhookserver.Event(nil), n.events...)
}

type fakeRepo struct {
	mu       sync.Mutex
	requests map[int64]*model.FriendRequest
	friends  map[[2]int64]*model.Friend
	black    map[[2]int64]bool
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		requests: make(map[int64]*model.FriendRequest),
		friends:  make(map[[2]int64]*model.Friend),
		black:    make(map[[2]int64]bool),
	}
}

func (f *fakeRepo) CreateRequest(_ context.Context, req *model.FriendRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy := *req
	f.requests[req.ID] = &copy
	return nil
}

func (f *fakeRepo) GetRequest(_ context.Context, id int64) (*model.FriendRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req, ok := f.requests[id]; ok {
		return req, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeRepo) AcceptRequest(_ context.Context, fromID, toID, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasInboundLocked(fromID, toID) {
		return pgx.ErrNoRows
	}
	for _, req := range f.requests {
		if req.Status != model.FriendRequestPending {
			continue
		}
		if (req.FromID == fromID && req.ToID == toID) || (req.FromID == toID && req.ToID == fromID) {
			req.Status = model.FriendRequestAccepted
		}
	}
	f.friends[[2]int64{fromID, toID}] = &model.Friend{UID: fromID, FriendID: toID, CreatedAt: now}
	f.friends[[2]int64{toID, fromID}] = &model.Friend{UID: toID, FriendID: fromID, CreatedAt: now}
	return nil
}

func (f *fakeRepo) HandleRequest(_ context.Context, fromID, toID int64, status int16, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasInboundLocked(fromID, toID) {
		return pgx.ErrNoRows
	}
	for _, req := range f.requests {
		if req.Status != model.FriendRequestPending {
			continue
		}
		if (req.FromID == fromID && req.ToID == toID) || (req.FromID == toID && req.ToID == fromID) {
			req.Status = status
		}
	}
	return nil
}

// hasInboundLocked reports whether a pending fromID -> toID row exists.
func (f *fakeRepo) hasInboundLocked(fromID, toID int64) bool {
	for _, req := range f.requests {
		if req.FromID == fromID && req.ToID == toID && req.Status == model.FriendRequestPending {
			return true
		}
	}
	return false
}

func (f *fakeRepo) ListRequests(_ context.Context, uid int64, inbound bool) ([]*model.FriendRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*model.FriendRequest, 0)
	for _, req := range f.requests {
		if (inbound && req.ToID == uid) || (!inbound && req.FromID == uid) {
			result = append(result, req)
		}
	}
	return result, nil
}

func (f *fakeRepo) AddFriends(_ context.Context, a, b, now int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.friends[[2]int64{a, b}] = &model.Friend{UID: a, FriendID: b, CreatedAt: now}
	f.friends[[2]int64{b, a}] = &model.Friend{UID: b, FriendID: a, CreatedAt: now}
	return nil
}

func (f *fakeRepo) DeleteFriend(_ context.Context, a, b int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.friends, [2]int64{a, b})
	delete(f.friends, [2]int64{b, a})
	return nil
}

func (f *fakeRepo) List(_ context.Context, uid int64) ([]*model.Friend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*model.Friend, 0)
	for key, friend := range f.friends {
		if key[0] == uid {
			result = append(result, friend)
		}
	}
	return result, nil
}

func (f *fakeRepo) UpdateRemark(_ context.Context, uid, friendID int64, remark string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	friend, ok := f.friends[[2]int64{uid, friendID}]
	if !ok {
		return pgx.ErrNoRows
	}
	friend.Remark = remark
	return nil
}

func (f *fakeRepo) UpdatePin(_ context.Context, uid, friendID int64, pinned int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	friend, ok := f.friends[[2]int64{uid, friendID}]
	if !ok {
		return pgx.ErrNoRows
	}
	friend.Pinned = pinned
	return nil
}

func (f *fakeRepo) AddBlack(_ context.Context, uid, blackID, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.black[[2]int64{uid, blackID}] = true
	return nil
}

func (f *fakeRepo) RemoveBlack(_ context.Context, uid, blackID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.black, [2]int64{uid, blackID})
	return nil
}

func (f *fakeRepo) Blacklist(_ context.Context, uid int64) ([]*model.BlacklistEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]*model.BlacklistEntry, 0)
	for key := range f.black {
		if key[0] == uid {
			result = append(result, &model.BlacklistEntry{UID: uid, BlackID: key[1]})
		}
	}
	return result, nil
}

func (f *fakeRepo) IsFriend(_ context.Context, a, b int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.friends[[2]int64{a, b}]
	return ok, nil
}

func (f *fakeRepo) IsBlack(_ context.Context, uid, target int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.black[[2]int64{uid, target}], nil
}

func (f *fakeRepo) Relation(_ context.Context, a, b int64) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, friend := f.friends[[2]int64{a, b}]
	return friend, f.black[[2]int64{b, a}], nil
}

func (f *fakeRepo) CanAddFriend(context.Context, int64) (bool, error) { return true, nil }
