// Package rtc defines the media-provider boundary. LiveKit is the default
// implementation; business code only depends on this interface.
package rtc

import "context"

// Role is a participant role inside a room.
type Role int32

// Participant roles.
const (
	RoleParticipant Role = 1
	RolePublisher   Role = 2
)

// RoomReq describes an idempotent room creation request.
type RoomReq struct {
	Room            string
	CallID          string
	GroupID         int64
	OwnerID         int64
	MaxParticipants int
}

// Room is the provider-side room identity.
type Room struct {
	ID       string
	Name     string
	Node     string
	ExpireAt int64
}

// RoomEvent is a provider webhook event.
type RoomEvent struct {
	Type  string
	Room  string
	UID   int64
	Track string
	Muted bool
}

// Provider is the five-action RTC boundary.
type Provider interface {
	CreateRoom(ctx context.Context, req RoomReq) (*Room, error)
	IssueToken(ctx context.Context, room string, uid int64, role Role) (string, error)
	RemoveParticipant(ctx context.Context, room string, uid int64) error
	DeleteRoom(ctx context.Context, room string) error
	MuteTrack(ctx context.Context, room, track string, muted bool) error
	Events() <-chan RoomEvent
}
