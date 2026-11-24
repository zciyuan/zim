// Package server implements the voice-room lifecycle and seat state machine
// (ADR-0043, op 1808-1819). Rooms live in LiveKit plus PostgreSQL
// (zim_voice_room); the authoritative seat state is a Redis hash driven by
// atomic Lua scripts in lua.go. Webhook events keep the seat state aligned
// with the media plane (reconnect grace / empty-room GC).
package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

// Room status (persisted in zim_voice_room.status and the room hash).
const (
	RoomLive   int16 = 1
	RoomClosed int16 = 2
)

// Seat status values (zim:vr:seats:{room_id} hash values are "uid:status").
const (
	SeatEmpty        int32 = 0
	SeatSpeaking     int32 = 1
	SeatSelfMuted    int32 = 2
	SeatForceMuted   int32 = 3
	SeatLocked       int32 = 4
	SeatReconnecting int32 = 5
)

// Close reasons for the 1913 push.
const (
	CloseReasonEmptyTimeout int32 = 1
	CloseReasonOwner        int32 = 2
	CloseReasonAdmin        int32 = 3
)

// Config carries runtime tunables, read from etc.zim.voiceroom.*.
type Config struct {
	SeatCount        int32
	ReconnectTimeout time.Duration
	EmptyTimeout     time.Duration
	RoomTTL          time.Duration
	TokenTTL         time.Duration
	ReapInterval     time.Duration
	SFUURL           string
	WebhookSecret    string
}

// DefaultConfig returns the ADR-0043 baseline values.
func DefaultConfig() Config {
	return Config{
		SeatCount:        int32(model.VoiceRoomSeatCountDefault),
		ReconnectTimeout: 30 * time.Second,
		EmptyTimeout:     300 * time.Second,
		RoomTTL:          7 * 24 * time.Hour,
		TokenTTL:         time.Hour,
		ReapInterval:     time.Second,
	}
}

// VoiceRoomRepo is the small PostgreSQL boundary the service needs; the
// production implementation is internal/dao/voiceroom, tests use a fake.
type VoiceRoomRepo interface {
	Create(ctx context.Context, room *model.VoiceRoom) error
	Get(ctx context.Context, roomID int64) (*model.VoiceRoom, error)
	UpdateStatus(ctx context.Context, roomID int64, status int16, closedAt int64) error
	ListByGroup(ctx context.Context, groupID int64, limit int, offset int64) ([]*model.VoiceRoom, error)
	ListPublic(ctx context.Context, limit int, offset int64) ([]*model.VoiceRoom, error)
}

// Pusher delivers S->C pushes. room carries the group_id so a group-room
// implementation may fan out to group members when uids is empty.
type Pusher interface {
	PushRoom(ctx context.Context, room *Room, opcode int32, payload []byte, uids []int64)
}

// Room is the runtime room state cached in zim:vr:room:{room_id}.
type Room struct {
	ID           int64
	RoomType     int16
	GroupID      int64
	OwnerUID     int64
	Title        string
	Announcement string
	SeatCount    int32
	JoinSeatMode int16
	Status       int16
	CreatedAt    int64
}

// Live reports whether the room accepts joins and seat operations.
func (r *Room) Live() bool { return r != nil && r.Status == RoomLive }

// Service is the voiceroom mesh service provider and due component. The
// reaper goroutine starts in Start() and stops in Close().
type Service struct {
	pb.UnimplementedVoiceroomServer
	proxy  *mesh.Proxy
	repo   VoiceRoomRepo
	client redis.UniversalClient
	media  rtc.Provider
	pusher Pusher
	ids    *snowflake.Generator
	cfg    Config
	now    func() time.Time

	initOnce sync.Once
	once     sync.Once
	stop     chan struct{}
}

type createRoomArgs struct {
	OwnerUID     int64
	RoomType     int32
	GroupID      int64
	Title        string
	Password     string
	SeatCount    int32
	JoinSeatMode int32
}

type listRoomsArgs struct {
	GroupID int64
	Offset  int64
	Limit   int32
}

type joinRoomArgs struct {
	RoomID   int64
	UID      int64
	Password string
}

type refreshTokenArgs struct {
	RoomID int64
	UID    int64
}

type takeSeatArgs struct {
	RoomID    int64
	UID       int64
	SeatIndex int32
}

type leaveSeatArgs struct {
	RoomID int64
	UID    int64
}

type kickSeatArgs struct {
	RoomID      int64
	OperatorUID int64
	UID         int64
	SeatIndex   int32
}

type forceMuteSeatArgs struct {
	RoomID      int64
	OperatorUID int64
	SeatIndex   int32
	Muted       bool
}

type lockSeatArgs struct {
	RoomID      int64
	OperatorUID int64
	SeatIndex   int32
	Locked      bool
}

type handUpArgs struct {
	RoomID int64
	UID    int64
}

type approveHandUpArgs struct {
	RoomID      int64
	OperatorUID int64
	UID         int64
	SeatIndex   int32
	Accept      bool
}

type inviteToSeatArgs struct {
	RoomID      int64
	OperatorUID int64
	UID         int64
	SeatIndex   int32
}

type getRoomArgs struct {
	RoomID int64
}

type closeRoomArgs struct {
	RoomID      int64
	OperatorUID int64
	Reason      int32
}

// Webhook serves the LiveKit webhook endpoint on its own net/http listener;
// the mesh process has no http component, so the listener is a plain
// goroutine (smallest change, see Start/Close).
type Webhook struct {
	addr   string
	svc    *Service
	server *http.Server
	mu     sync.Mutex
}
