package server

import (
	"context"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

// fakeRepo is the in-memory VoiceRoomRepo.
type fakeRepo struct {
	mu    sync.Mutex
	rooms map[int64]*model.VoiceRoom
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{rooms: make(map[int64]*model.VoiceRoom)}
}

func (f *fakeRepo) Create(_ context.Context, room *model.VoiceRoom) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy := *room
	f.rooms[room.RoomID] = &copy
	return nil
}

func (f *fakeRepo) Get(_ context.Context, roomID int64) (*model.VoiceRoom, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if room, ok := f.rooms[roomID]; ok {
		return room, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeRepo) UpdateStatus(_ context.Context, roomID int64, status int16, closedAt int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if room, ok := f.rooms[roomID]; ok {
		room.Status = status
		room.ClosedAt = closedAt
		return nil
	}
	return pgx.ErrNoRows
}

func (f *fakeRepo) ListByGroup(_ context.Context, groupID int64, limit int, _ int64) ([]*model.VoiceRoom, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rooms := make([]*model.VoiceRoom, 0)
	for _, room := range f.rooms {
		if room.GroupID == groupID && len(rooms) < limit {
			rooms = append(rooms, room)
		}
	}
	return rooms, nil
}

func (f *fakeRepo) ListPublic(_ context.Context, limit int, _ int64) ([]*model.VoiceRoom, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rooms := make([]*model.VoiceRoom, 0)
	for _, room := range f.rooms {
		if room.RoomType == model.VoiceRoomTypePublic && room.Status == model.VoiceRoomStatusLive && len(rooms) < limit {
			rooms = append(rooms, room)
		}
	}
	return rooms, nil
}

// fakeProvider is the in-memory rtc.Provider; it records publish-permission
// grants/revocations so tests can assert the LiveKit sync calls.
type fakeProvider struct {
	mu      sync.Mutex
	rooms   map[string]bool
	permits map[string]bool
	events  chan rtc.RoomEvent
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		rooms:   make(map[string]bool),
		permits: make(map[string]bool),
		events:  make(chan rtc.RoomEvent, 16),
	}
}

func (f *fakeProvider) CreateRoom(_ context.Context, req rtc.RoomReq) (*rtc.Room, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rooms[req.Room] = true
	return &rtc.Room{ID: req.Room, Name: req.Room}, nil
}

func (f *fakeProvider) IssueToken(_ context.Context, room string, uid int64, role rtc.Role) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rooms[room] {
		return "", rtc.ErrRoomNotFound
	}
	f.permits[room+":"+strconv.FormatInt(uid, 10)] = role >= rtc.RoleParticipant
	return room + ":" + strconv.FormatInt(uid, 10), nil
}

func (f *fakeProvider) JoinRoom(_ context.Context, room string, uid int64, role rtc.Role) (string, error) {
	return f.IssueToken(context.Background(), room, uid, role)
}

func (f *fakeProvider) UpdateParticipantPermission(_ context.Context, room string, uid int64, canPublish bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rooms[room] {
		return rtc.ErrRoomNotFound
	}
	f.permits[room+":"+strconv.FormatInt(uid, 10)] = canPublish
	return nil
}

func (f *fakeProvider) RemoveParticipant(_ context.Context, room string, uid int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.rooms[room] {
		return rtc.ErrRoomNotFound
	}
	return nil
}

func (f *fakeProvider) DeleteRoom(_ context.Context, room string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.rooms, room)
	return nil
}

func (f *fakeProvider) MuteTrack(_ context.Context, room, track string, muted bool) error {
	f.events <- rtc.RoomEvent{Type: "track_muted", Room: room, Track: track, Muted: muted}
	return nil
}

func (f *fakeProvider) Events() <-chan rtc.RoomEvent { return f.events }

func (f *fakeProvider) permit(room string, uid int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.permits[room+":"+strconv.FormatInt(uid, 10)]
}

func (f *fakeProvider) roomGone(room string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.rooms[room]
}

// fakePusher records room pushes.
type fakePusher struct {
	mu     sync.Mutex
	pushes []pushRecord
}

type pushRecord struct {
	roomID int64
	opcode int32
	uids   []int64
}

func (f *fakePusher) PushRoom(_ context.Context, room *Room, opcode int32, _ []byte, uids []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushes = append(f.pushes, pushRecord{roomID: room.ID, opcode: opcode, uids: uids})
}

func (f *fakePusher) count(opcode int32) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, push := range f.pushes {
		if push.opcode == opcode {
			count++
		}
	}
	return count
}
