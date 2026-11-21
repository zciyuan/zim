package rtc

import (
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"
)

// Call states.
const (
	StateRinging  int32 = 1
	StateAnswered int32 = 2
	StateRejected int32 = 3
	StateBusy     int32 = 4
	StateTimeout  int32 = 5
	StateCanceled int32 = 6
	StateEnded    int32 = 7
)

// MaxGroupVideoParticipants is the frozen group-video grid limit (DL-086).
const MaxGroupVideoParticipants int32 = 9

// MaxGroupVoiceParticipants matches ZIM-02 R3 (2-50 members).
const MaxGroupVoiceParticipants int32 = 50

// MaxFamilyParticipants matches the family group member cap.
const MaxFamilyParticipants int32 = 20

// Call is the Redis-backed signaling state.
//
// target_type=2 (ordinary group) is a persistent room that starts in
// StateAnswered. target_type=3 (family) still starts in StateRinging and keeps
// ring/auto-answer, but uses the same participant set and group index: it ends
// only when the last participant leaves.
type Call struct {
	ID              string
	FromUID         int64
	ToUID           int64
	TargetType      int32
	Media           int32
	Room            string
	State           int32
	CreatedAt       int64
	UpdatedAt       int64
	ExpireAt        int64
	MaxParticipants int32
	Version         int64
	Seq             int64
}

// Event is one replayed call state transition.
type Event struct {
	Seq   int64
	State int32
	At    int64
}

// Service implements the RTC call state machine.
type Service struct {
	client redis.UniversalClient
	ttl    time.Duration
	now    func() time.Time
}

// Errors.
var (
	ErrInvalidCall  = errors.New("invalid call")
	ErrBusy         = errors.New("call busy")
	ErrCallNotFound = errors.New("call not found")
	ErrForbidden    = errors.New("call forbidden")
	ErrInvalidState = errors.New("invalid call state")
	ErrRoomFull     = errors.New("rtc room full")
)
