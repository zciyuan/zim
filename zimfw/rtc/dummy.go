package rtc

import (
	"context"
	"strconv"
	"sync"

	"github.com/dobyte/due/v2/errors"
)

// ErrRoomNotFound is returned when the room does not exist.
var ErrRoomNotFound = errors.New("rtc room not found")

// Dummy is a deterministic in-memory provider for tests and P-RTC-0.
type Dummy struct {
	mu     sync.Mutex
	rooms  map[string]*Room
	tokens map[string]string
	events chan RoomEvent
}

// NewDummy returns a dummy provider.
func NewDummy() *Dummy {
	return &Dummy{
		rooms: make(map[string]*Room), tokens: make(map[string]string),
		events: make(chan RoomEvent, 16),
	}
}

// CreateRoom creates a room once; repeated calls return the same room.
func (d *Dummy) CreateRoom(_ context.Context, req RoomReq) (*Room, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if room := d.rooms[req.Room]; room != nil {
		return room, nil
	}
	room := &Room{ID: req.Room, Name: req.Room, Node: "dummy"}
	d.rooms[req.Room] = room
	return room, nil
}

// IssueToken returns a deterministic dummy token.
func (d *Dummy) IssueToken(_ context.Context, room string, uid int64, _ Role) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.rooms[room] == nil {
		return "", ErrRoomNotFound
	}
	token := room + ":" + strconv.FormatInt(uid, 10)
	d.tokens[token] = room
	return token, nil
}

// RemoveParticipant removes a participant token (no-op if absent).
func (d *Dummy) RemoveParticipant(_ context.Context, room string, uid int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.rooms[room] == nil {
		return ErrRoomNotFound
	}
	for token := range d.tokens {
		if token == room+":"+strconv.FormatInt(uid, 10) {
			delete(d.tokens, token)
		}
	}
	return nil
}

// DeleteRoom removes a room and its tokens; a missing room is a no-op.
func (d *Dummy) DeleteRoom(_ context.Context, room string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.rooms, room)
	for token, tokenRoom := range d.tokens {
		if tokenRoom == room {
			delete(d.tokens, token)
		}
	}
	return nil
}

// MuteTrack records a mute event.
func (d *Dummy) MuteTrack(_ context.Context, room, track string, muted bool) error {
	d.mu.Lock()
	_, ok := d.rooms[room]
	d.mu.Unlock()
	if !ok {
		return ErrRoomNotFound
	}
	d.events <- RoomEvent{Type: "track_muted", Room: room, Track: track, Muted: muted}
	return nil
}

// Events returns the dummy event channel.
func (d *Dummy) Events() <-chan RoomEvent { return d.events }
