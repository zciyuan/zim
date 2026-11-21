// Package rtc owns the call state machine; media never flows through IM.
package rtc

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"
)

// New returns a call service.
func New(client redis.UniversalClient) *Service {
	return &Service{client: client, ttl: 2 * time.Hour, now: time.Now}
}

// Invite creates a ringing call.
func (s *Service) Invite(ctx context.Context, id string, from, to int64, media int32, room string) (*Call, error) {
	return s.InviteTo(ctx, id, from, to, 1, media, room)
}

// InviteTo creates a ringing call. Ordinary groups use OpenGroup and families
// use OpenFamily: those are rooms, not plain invitations.
func (s *Service) InviteTo(ctx context.Context, id string, from, to int64, targetType, media int32, room string) (*Call, error) {
	if id == "" || from <= 0 || to <= 0 || from == to || targetType == 2 {
		return nil, ErrInvalidCall
	}
	if ok, err := s.client.Exists(ctx, key(id)).Result(); err != nil {
		return nil, err
	} else if ok > 0 {
		return nil, ErrBusy
	}
	now := s.now().UnixMilli()
	expireAt := now + 30_000
	call := &Call{ID: id, FromUID: from, ToUID: to, TargetType: targetType, Media: media, Room: room,
		State: StateRinging, CreatedAt: now, UpdatedAt: now, ExpireAt: expireAt}
	if err := s.client.HSet(ctx, key(id), map[string]any{
		"from": from, "to": to, "target_type": targetType, "media": media, "room": room,
		"state": StateRinging, "created_at": now, "updated_at": now, "expire_at": expireAt,
		"version": 1, "seq": 1,
	}).Err(); err != nil {
		return nil, err
	}
	if err := s.client.Expire(ctx, key(id), s.ttl).Err(); err != nil {
		return nil, err
	}
	if err := s.addDeadline(ctx, id, expireAt); err != nil {
		return nil, err
	}
	if err := s.bindUserCall(ctx, call); err != nil {
		return nil, err
	}
	return call, nil
}

// OpenGroup creates a persistent group-call room.
//
// The call starts in StateAnswered so no member is rung; the creator is the
// first participant. One active call per group is enforced through
// zim:rtc:group:{gid}; the call hash is written before that index so an index
// never points at a half-created call.
func (s *Service) OpenGroup(ctx context.Context, id string, from, groupID int64,
	media int32, room string, max int32) (*Call, error) {
	if id == "" || from <= 0 || groupID <= 0 {
		return nil, ErrInvalidCall
	}
	if max <= 0 {
		max = MaxGroupVideoParticipants
	}
	now := s.now().UnixMilli()
	call := &Call{ID: id, FromUID: from, ToUID: groupID, TargetType: 2, Media: media, Room: room,
		State: StateAnswered, CreatedAt: now, UpdatedAt: now, ExpireAt: now + s.ttl.Milliseconds(),
		MaxParticipants: max}
	return s.openCall(ctx, call)
}

// OpenFamily creates a family call that still rings, but is also a room.
//
// Ringing and auto-answer remain (ADR-0033); the difference is that the call
// gets a group index, so family members can join/leave it via 1805/1806/1803
// while it is live, and it ends only when the last participant leaves.
func (s *Service) OpenFamily(ctx context.Context, id string, from, groupID int64,
	media int32, room string, max int32) (*Call, error) {
	if id == "" || from <= 0 || groupID <= 0 {
		return nil, ErrInvalidCall
	}
	if max <= 0 {
		max = MaxFamilyParticipants
	}
	now := s.now().UnixMilli()
	call := &Call{ID: id, FromUID: from, ToUID: groupID, TargetType: 3, Media: media, Room: room,
		State: StateRinging, CreatedAt: now, UpdatedAt: now, ExpireAt: now + 30_000,
		MaxParticipants: max}
	return s.openCall(ctx, call)
}

// openCall writes the call hash and claims zim:rtc:group:{gid}.
func (s *Service) openCall(ctx context.Context, call *Call) (*Call, error) {
	if err := s.createGroupCall(ctx, call); err != nil {
		if !errors.Is(err, ErrBusy) {
			s.cleanupCall(ctx, call)
		}
		return nil, err
	}
	ok, err := s.client.SetNX(ctx, groupKey(call.ToUID), call.ID, s.ttl).Result()
	if err != nil {
		s.cleanupCall(ctx, call)
		return nil, err
	}
	if !ok {
		// Capture the index value before probing it: ActiveGroupCall may clear
		// a stale index, and a concurrent call could claim the group right
		// after. The compare-and-delete below must use the value we observed,
		// never a freshly read (possibly newer) one.
		staleID, getErr := s.client.Get(ctx, groupKey(call.ToUID)).Result()
		if getErr != nil && !errors.Is(getErr, redis.Nil) {
			s.cleanupCall(ctx, call)
			return nil, getErr
		}
		// A previous room may have expired without clearing its index. Only
		// remove the index when ActiveGroupCall proves it is stale.
		_, activeErr := s.ActiveGroupCall(ctx, call.ToUID)
		if activeErr == nil {
			s.cleanupCall(ctx, call)
			return nil, ErrBusy
		}
		if !errors.Is(activeErr, ErrCallNotFound) {
			s.cleanupCall(ctx, call)
			return nil, activeErr
		}
		if getErr == nil {
			if _, delErr := runDeleteGroupIndexScript(ctx, s.client, groupKey(call.ToUID), staleID); delErr != nil {
				s.cleanupCall(ctx, call)
				return nil, delErr
			}
		}
		ok, err = s.client.SetNX(ctx, groupKey(call.ToUID), call.ID, s.ttl).Result()
		if err != nil || !ok {
			s.cleanupCall(ctx, call)
			if err != nil {
				return nil, err
			}
			return nil, ErrBusy
		}
	}
	if call.State == StateRinging {
		if err := s.addDeadline(ctx, call.ID, call.ExpireAt); err != nil {
			s.cleanupCall(ctx, call)
			return nil, err
		}
	}
	if err := s.bindUserCall(ctx, call); err != nil {
		s.cleanupCall(ctx, call)
		return nil, err
	}
	return call, nil
}

// JoinGroup adds uid to a group/family call.
//
// Joining is idempotent: a reconnecting client that is already a participant
// still gets a token from the caller.
func (s *Service) JoinGroup(ctx context.Context, id string, uid int64) (*Call, error) {
	call, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.joinGroup(ctx, call, uid)
}

// LeaveGroup removes uid from a group/family call.
//
// Ordinary group calls and answered family calls end only when the last
// participant leaves. participants is the snapshot before the removal, so the
// end card can list everyone who was in the room.
func (s *Service) LeaveGroup(ctx context.Context, id string, uid int64) (*Call, []int64, bool, error) {
	call, err := s.Get(ctx, id)
	if err != nil {
		return nil, nil, false, err
	}
	if call.TargetType <= 1 {
		return nil, nil, false, ErrForbidden
	}
	if isTerminal(call.State) {
		return nil, nil, false, ErrInvalidState
	}
	participants, err := s.Participants(ctx, id)
	if err != nil {
		return nil, nil, false, err
	}
	_, left, err := runLeaveGroupMemberScript(ctx, s.client, membersKey(id), uid)
	if err != nil {
		return nil, nil, false, err
	}
	if left == 0 {
		if err = s.endCall(ctx, call); err != nil {
			return nil, nil, false, err
		}
		_ = s.client.Del(ctx, userCallKey(uid)).Err()
		call.State = StateEnded
		return call, participants, true, nil
	}
	_ = s.client.Del(ctx, userCallKey(uid)).Err()
	if err = s.touch(ctx, call); err != nil {
		return nil, nil, false, err
	}
	return call, participants, false, nil
}

// ActiveGroupCall returns the live call of one group.
func (s *Service) ActiveGroupCall(ctx context.Context, groupID int64) (*Call, error) {
	id, err := s.client.Get(ctx, groupKey(groupID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCallNotFound
		}
		return nil, err
	}
	call, err := s.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrCallNotFound) {
			_, _ = runDeleteGroupIndexScript(ctx, s.client, groupKey(groupID), id)
		}
		return nil, err
	}
	if call.ToUID != groupID || (call.TargetType != 2 && call.TargetType != 3) || isTerminal(call.State) {
		_, _ = runDeleteGroupIndexScript(ctx, s.client, groupKey(groupID), id)
		return nil, ErrCallNotFound
	}
	// Ordinary group rooms are active immediately; family calls are joinable
	// while ringing (joining answers the call).
	if call.TargetType == 2 && call.State != StateAnswered {
		_, _ = runDeleteGroupIndexScript(ctx, s.client, groupKey(groupID), id)
		return nil, ErrCallNotFound
	}
	return call, nil
}

// End ends a call immediately (used to roll back a failed group/family start).
func (s *Service) End(ctx context.Context, id string) error {
	call, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if err = s.endCall(ctx, call); err != nil {
		return err
	}
	call.State = StateEnded
	return nil
}

// Touch refreshes the call TTL (participant heartbeat).
func (s *Service) Touch(ctx context.Context, id string) error {
	call, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if isTerminal(call.State) {
		return ErrInvalidState
	}
	return s.touch(ctx, call)
}

// Answer accepts or rejects a ringing call.
//
// Direct calls: only ToUID may answer, once. Group/family calls: any member
// except the caller may join at any time while the call is live, so the state
// becomes answered on the first acceptance and later joiners are recorded in the
// participants set.
func (s *Service) Answer(ctx context.Context, id string, uid int64, accept bool) (*Call, error) {
	call, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if call.TargetType > 1 {
		return s.answerGroup(ctx, call, uid, accept)
	}
	if call.ToUID != uid {
		return nil, ErrForbidden
	}
	// Retries after a lost response are idempotent: an accepted call stays
	// answered instead of surfacing ErrInvalidState to the client.
	if accept && call.State == StateAnswered {
		return call, nil
	}
	if call.State != StateRinging {
		return nil, ErrInvalidState
	}
	state := StateRejected
	if accept {
		state = StateAnswered
	}
	if err = s.setState(ctx, id, state); err != nil {
		return nil, err
	}
	call.State = state
	if accept {
		if err = s.touch(ctx, call); err != nil {
			return nil, err
		}
		if fresh, getErr := s.Get(ctx, id); getErr == nil {
			call.Version, call.Seq = fresh.Version, fresh.Seq
		}
	}
	return call, nil
}

// Hangup ends a 1:1 or family call.
func (s *Service) Hangup(ctx context.Context, id string, uid int64) error {
	call, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if isTerminal(call.State) {
		return nil
	}
	if call.FromUID != uid && call.ToUID != uid {
		joined, err := s.Joined(ctx, id, uid)
		if err != nil {
			return err
		}
		if !joined {
			return ErrForbidden
		}
	}
	if err := s.setState(ctx, id, StateEnded); err != nil {
		return err
	}
	s.clearUserCall(ctx, call)
	return nil
}

// Cancel cancels a ringing call. Only the caller may cancel it.
func (s *Service) Cancel(ctx context.Context, id string, uid int64) error {
	call, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if call.FromUID != uid {
		return ErrForbidden
	}
	if call.State != StateRinging {
		return ErrInvalidState
	}
	if err = s.setState(ctx, id, StateCanceled); err != nil {
		return err
	}
	s.clearUserCall(ctx, call)
	if call.TargetType > 1 {
		if _, err = runDeleteGroupIndexScript(ctx, s.client, groupKey(call.ToUID), call.ID); err != nil {
			return err
		}
	}
	return nil
}

// Get returns one call.
func (s *Service) Get(ctx context.Context, id string) (*Call, error) {
	values, err := s.client.HGetAll(ctx, key(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrCallNotFound
	}
	call := &Call{ID: id}
	call.FromUID = parseInt(values["from"])
	call.ToUID = parseInt(values["to"])
	call.TargetType = int32(parseInt(values["target_type"]))
	call.Media = int32(parseInt(values["media"]))
	call.Room = values["room"]
	call.State = int32(parseInt(values["state"]))
	call.CreatedAt = parseInt(values["created_at"])
	call.UpdatedAt = parseInt(values["updated_at"])
	call.ExpireAt = parseInt(values["expire_at"])
	call.MaxParticipants = int32(parseInt(values["max"]))
	call.Version = parseInt(values["version"])
	call.Seq = parseInt(values["seq"])
	if call.ExpireAt == 0 {
		if call.TargetType > 1 {
			call.ExpireAt = call.CreatedAt + s.ttl.Milliseconds()
		} else {
			call.ExpireAt = call.CreatedAt + 30_000
		}
	}
	return call, nil
}

// TimeoutCalls marks and returns ringing calls older than cutoff.
//
// The deadline index (ZSET) is the only scan: KEYS is unsafe on Redis Cluster
// and blocks the server. A stored score is the ringing deadline (created+30s),
// while cutoff is "now-30s", so the query bound is cutoff+30s.
func (s *Service) TimeoutCalls(ctx context.Context, cutoff int64) ([]*Call, error) {
	ids, err := s.client.ZRangeByScore(ctx, deadlinesKey(), &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(cutoff+30_000, 10), Count: 1000,
	}).Result()
	if err != nil {
		return nil, err
	}
	calls := make([]*Call, 0, len(ids))
	for _, id := range ids {
		call, getErr := s.Get(ctx, id)
		if getErr != nil {
			if errors.Is(getErr, ErrCallNotFound) {
				_ = s.removeDeadline(ctx, id)
			}
			continue
		}
		if call.State != StateRinging {
			_ = s.removeDeadline(ctx, id)
			continue
		}
		if call.CreatedAt > cutoff {
			continue
		}
		call.State = StateTimeout
		if setErr := s.setState(ctx, id, StateTimeout); setErr != nil {
			return calls, setErr
		}
		s.clearUserCall(ctx, call)
		if call.TargetType > 1 {
			if _, delErr := runDeleteGroupIndexScript(ctx, s.client, groupKey(call.ToUID), id); delErr != nil {
				return calls, delErr
			}
		}
		calls = append(calls, call)
	}
	return calls, nil
}

// Timeout marks ringing calls older than cutoff and returns the count.
func (s *Service) Timeout(ctx context.Context, cutoff int64) (int, error) {
	calls, err := s.TimeoutCalls(ctx, cutoff)
	return len(calls), err
}

// answerGroup lets one member join a group/family call.
func (s *Service) answerGroup(ctx context.Context, call *Call, uid int64, accept bool) (*Call, error) {
	if uid == call.FromUID {
		return nil, ErrForbidden
	}
	if isTerminal(call.State) {
		return nil, ErrInvalidState
	}
	if !accept {
		return call, nil
	}
	return s.joinGroup(ctx, call, uid)
}

// joinGroup adds uid to an existing group/family call.
func (s *Service) joinGroup(ctx context.Context, call *Call, uid int64) (*Call, error) {
	if call.TargetType <= 1 {
		return nil, ErrForbidden
	}
	if isTerminal(call.State) {
		return nil, ErrInvalidState
	}
	max := call.MaxParticipants
	if max <= 0 {
		if call.TargetType == 3 {
			max = MaxFamilyParticipants
			if call.Media == 2 {
				max = MaxGroupVideoParticipants
			}
		} else {
			max = MaxGroupVideoParticipants
		}
	}
	joined, err := runJoinGroupMemberScript(ctx, s.client, membersKey(call.ID), uid, max)
	if err != nil {
		return nil, err
	}
	if !joined {
		return nil, ErrRoomFull
	}
	if call.State == StateRinging {
		if err = s.setState(ctx, call.ID, StateAnswered); err != nil {
			return nil, err
		}
		call.State = StateAnswered
	}
	if err = s.touch(ctx, call); err != nil {
		return nil, err
	}
	// Every group participant gets a user index so a later disconnect can find
	// and leave the room without relying on a signaling push.
	if err = s.client.Set(ctx, userCallKey(uid), call.ID, s.ttl).Err(); err != nil {
		return nil, err
	}
	return call, nil
}

// createGroupCall writes the call hash and its first participant.
//
// HSETNX claims the call id first, so two concurrent starts that happen to use
// the same client-generated id cannot delete each other's state.
func (s *Service) createGroupCall(ctx context.Context, call *Call) error {
	claimed, err := s.client.HSetNX(ctx, key(call.ID), "from", call.FromUID).Result()
	if err != nil {
		return err
	}
	if !claimed {
		return ErrBusy
	}
	if err = s.client.HSet(ctx, key(call.ID), map[string]any{
		"from": call.FromUID, "to": call.ToUID, "target_type": call.TargetType,
		"media": call.Media, "room": call.Room, "state": call.State,
		"created_at": call.CreatedAt, "updated_at": call.UpdatedAt,
		"expire_at": call.ExpireAt, "max": call.MaxParticipants,
		"version": 1, "seq": 1,
	}).Err(); err != nil {
		return err
	}
	if err = s.client.Expire(ctx, key(call.ID), s.ttl).Err(); err != nil {
		return err
	}
	if err = s.client.SAdd(ctx, membersKey(call.ID), call.FromUID).Err(); err != nil {
		return err
	}
	return s.client.Expire(ctx, membersKey(call.ID), s.ttl).Err()
}

// cleanupCall releases a call whose group index could not be claimed.
func (s *Service) cleanupCall(ctx context.Context, call *Call) {
	_ = s.client.Del(ctx, key(call.ID)).Err()
	_ = s.client.Del(ctx, membersKey(call.ID)).Err()
}

// endCall marks the call ended and clears the group index.
func (s *Service) endCall(ctx context.Context, call *Call) error {
	if err := s.setState(ctx, call.ID, StateEnded); err != nil {
		return err
	}
	s.clearUserCall(ctx, call)
	if call.TargetType > 1 {
		// Compare-and-delete: never remove an index that already points at a
		// newer room in the same group.
		if _, err := runDeleteGroupIndexScript(ctx, s.client, groupKey(call.ToUID), call.ID); err != nil {
			return err
		}
	}
	return nil
}

// touch refreshes the call, participants and group-index TTLs. A call that has
// left ringing is removed from the deadline index: only ringing calls expire.
func (s *Service) touch(ctx context.Context, call *Call) error {
	now := s.now().UnixMilli()
	expireAt := now + s.ttl.Milliseconds()
	if err := s.client.HSet(ctx, key(call.ID), "updated_at", now, "expire_at", expireAt).Err(); err != nil {
		return err
	}
	if err := s.removeDeadline(ctx, call.ID); err != nil {
		return err
	}
	if err := s.client.Expire(ctx, key(call.ID), s.ttl).Err(); err != nil {
		return err
	}
	if err := s.client.Expire(ctx, membersKey(call.ID), s.ttl).Err(); err != nil {
		return err
	}
	if call.TargetType == 2 {
		if err := s.client.Expire(ctx, groupKey(call.ToUID), s.ttl).Err(); err != nil {
			return err
		}
	}
	call.UpdatedAt = now
	call.ExpireAt = expireAt
	return nil
}

func (s *Service) setState(ctx context.Context, id string, state int32) error {
	now := s.now().UnixMilli()
	if err := s.client.HSet(ctx, key(id), "state", state, "updated_at", now).Err(); err != nil {
		return err
	}
	version, err := s.client.HIncrBy(ctx, key(id), "version", 1).Result()
	if err != nil {
		return err
	}
	seq, err := s.client.HIncrBy(ctx, key(id), "seq", 1).Result()
	if err != nil {
		return err
	}
	if err = s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: eventsKey(id), MaxLen: 200, Approx: true,
		Values: map[string]any{"seq": seq, "state": state, "at": now, "version": version},
	}).Err(); err != nil {
		return err
	}
	_ = s.client.Expire(ctx, eventsKey(id), s.ttl).Err()
	if isTerminal(state) {
		return s.removeDeadline(ctx, id)
	}
	return nil
}

// Events replays state transitions after sinceSeq for reconnect reconciliation.
func (s *Service) Events(ctx context.Context, id string, sinceSeq int64) ([]Event, error) {
	if sinceSeq < 0 {
		sinceSeq = 0
	}
	values, err := s.client.XRange(ctx, eventsKey(id),
		strconv.FormatInt(sinceSeq+1, 10), "+").Result()
	if err != nil {
		return nil, err
	}
	events := make([]Event, 0, len(values))
	for _, value := range values {
		events = append(events, Event{
			Seq:   parseInt(fmt.Sprint(value.Values["seq"])),
			State: int32(parseInt(fmt.Sprint(value.Values["state"]))),
			At:    parseInt(fmt.Sprint(value.Values["at"])),
		})
	}
	return events, nil
}

// Ack records the highest event seq a device has applied.
func (s *Service) Ack(ctx context.Context, id string, uid, seq int64) error {
	if seq <= 0 {
		return nil
	}
	return s.client.HSet(ctx, key(id), "ack:"+strconv.FormatInt(uid, 10), seq).Err()
}

// MarkLeg records the per-device call leg state.
func (s *Service) MarkLeg(ctx context.Context, id string, deviceKey int64, state string) error {
	return s.client.HSet(ctx, legsKey(id), strconv.FormatInt(deviceKey, 10), state).Err()
}

// addDeadline records a ringing call in the deadline index. The index replaces
// KEYS scans: zim-crontask reads only the calls that are due.
func (s *Service) addDeadline(ctx context.Context, id string, expireAt int64) error {
	return s.client.ZAdd(ctx, deadlinesKey(), redis.Z{Score: float64(expireAt), Member: id}).Err()
}

func (s *Service) removeDeadline(ctx context.Context, id string) error {
	return s.client.ZRem(ctx, deadlinesKey(), id).Err()
}

// Participants returns the uids that joined a group/family call.
func (s *Service) Participants(ctx context.Context, id string) ([]int64, error) {
	values, err := s.client.SMembers(ctx, membersKey(id)).Result()
	if err != nil {
		return nil, err
	}
	members := make([]int64, 0, len(values))
	for _, value := range values {
		if uid := parseInt(value); uid > 0 {
			members = append(members, uid)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	return members, nil
}

// Joined reports whether uid has already joined the call.
func (s *Service) Joined(ctx context.Context, id string, uid int64) (bool, error) {
	return s.client.SIsMember(ctx, membersKey(id), uid).Result()
}

func isTerminal(state int32) bool {
	return state == StateEnded || state == StateCanceled || state == StateRejected ||
		state == StateTimeout
}

// ActiveCall returns the live call of a user from the per-user index. It is the
// login snapshot path: a reconnecting client does not depend on a push having
// survived the disconnect.
func (s *Service) ActiveCall(ctx context.Context, uid int64) (*Call, error) {
	id, err := s.client.Get(ctx, userCallKey(uid)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCallNotFound
		}
		return nil, err
	}
	call, err := s.Get(ctx, id)
	if err != nil {
		_ = s.client.Del(ctx, userCallKey(uid)).Err()
		return nil, err
	}
	if isTerminal(call.State) || (call.TargetType <= 1 && call.FromUID != uid && call.ToUID != uid) {
		_ = s.client.Del(ctx, userCallKey(uid)).Err()
		return nil, ErrCallNotFound
	}
	return call, nil
}

func (s *Service) bindUserCall(ctx context.Context, call *Call) error {
	if err := s.client.Set(ctx, userCallKey(call.FromUID), call.ID, s.ttl).Err(); err != nil {
		return err
	}
	if call.TargetType <= 1 && call.ToUID > 0 {
		return s.client.Set(ctx, userCallKey(call.ToUID), call.ID, s.ttl).Err()
	}
	return nil
}

func (s *Service) clearUserCall(ctx context.Context, call *Call) {
	if call == nil {
		return
	}
	_ = s.client.Del(ctx, userCallKey(call.FromUID)).Err()
	if call.TargetType <= 1 && call.ToUID > 0 {
		_ = s.client.Del(ctx, userCallKey(call.ToUID)).Err()
	}
}

func userCallKey(uid int64) string {
	return "zim:rtc:user:" + strconv.FormatInt(uid, 10)
}

func key(id string) string { return "zim:rtc:call:" + id }

func eventsKey(id string) string { return key(id) + ":events" }

func legsKey(id string) string { return key(id) + ":legs" }

func membersKey(id string) string { return "zim:rtc:call:" + id + ":members" }

// deadlinesKey indexes ringing calls by their expiry so the timeout sweep never
// scans keys. Members are call IDs; scores are BIGINT milliseconds.
func deadlinesKey() string { return "zim:rtc:deadlines" }

func groupKey(groupID int64) string {
	return "zim:rtc:group:" + strconv.FormatInt(groupID, 10)
}

func parseInt(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}
