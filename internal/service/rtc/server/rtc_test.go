package rtc

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dobyte/due/v2/errors"
	"github.com/redis/go-redis/v9"
)

func newTestService(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), server
}

func TestCallLifecycle(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	call, err := service.Invite(ctx, "call-1", 1, 2, 2, "room-1")
	if err != nil || call.State != StateRinging {
		t.Fatalf("call = %+v, err = %v", call, err)
	}
	if call.ExpireAt <= call.CreatedAt {
		t.Fatalf("expire_at = %d, created_at = %d", call.ExpireAt, call.CreatedAt)
	}
	if _, err = service.Invite(ctx, "call-1", 1, 3, 2, "room-1"); err != ErrBusy {
		t.Fatalf("duplicate err = %v", err)
	}
	if _, err = service.Answer(ctx, "call-1", 3, true); err != ErrForbidden {
		t.Fatalf("wrong user err = %v", err)
	}
	answered, err := service.Answer(ctx, "call-1", 2, true)
	if err != nil || answered.State != StateAnswered {
		t.Fatalf("answered = %+v, err = %v", answered, err)
	}
	if err = service.Hangup(ctx, "call-1", 1); err != nil {
		t.Fatal(err)
	}
	ended, err := service.Get(ctx, "call-1")
	if err != nil || ended.State != StateEnded {
		t.Fatalf("ended = %+v, err = %v", ended, err)
	}
}

func TestCancelRinging(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	if _, err := service.Invite(ctx, "call-2", 1, 2, 1, "room-2"); err != nil {
		t.Fatal(err)
	}
	if err := service.Cancel(ctx, "call-2", 2); err != ErrForbidden {
		t.Fatalf("non-caller cancel err = %v", err)
	}
	if err := service.Cancel(ctx, "call-2", 1); err != nil {
		t.Fatal(err)
	}
	call, err := service.Get(ctx, "call-2")
	if err != nil || call.State != StateCanceled {
		t.Fatalf("call = %+v, err = %v", call, err)
	}
}

func TestTimeoutRingingCall(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.Invite(ctx, "call-3", 1, 2, 1, "room-3")
	if err != nil {
		t.Fatal(err)
	}
	// Participant sets share the key prefix but must be skipped by the sweep.
	if err = service.client.SAdd(ctx, membersKey("call-3"), 2).Err(); err != nil {
		t.Fatal(err)
	}
	if count, err := service.Timeout(ctx, call.CreatedAt-1); err != nil || count != 0 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	if count, err := service.Timeout(ctx, call.CreatedAt+1); err != nil || count != 1 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	timedOut, err := service.Get(ctx, "call-3")
	if err != nil || timedOut.State != StateTimeout {
		t.Fatalf("call = %+v, err = %v", timedOut, err)
	}
}

func TestOpenGroupRoom(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", MaxGroupVoiceParticipants)
	if err != nil {
		t.Fatal(err)
	}
	if call.State != StateAnswered || call.TargetType != 2 || call.ToUID != 5001 {
		t.Fatalf("call = %+v", call)
	}
	if call.MaxParticipants != MaxGroupVoiceParticipants {
		t.Fatalf("max = %d", call.MaxParticipants)
	}
	participants, err := service.Participants(ctx, call.ID)
	if err != nil || len(participants) != 1 || participants[0] != 1 {
		t.Fatalf("participants = %v, err = %v", participants, err)
	}
	active, err := service.ActiveGroupCall(ctx, 5001)
	if err != nil || active.ID != call.ID {
		t.Fatalf("active = %+v, err = %v", active, err)
	}
	if _, err = service.OpenGroup(ctx, "group-call-2", 2, 5001, 1, "room-g2", 9); err != ErrBusy {
		t.Fatalf("second room err = %v", err)
	}
	if _, err = service.Get(ctx, "group-call-2"); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("losing room must be cleaned up: err = %v", err)
	}
}

func TestOpenGroupStaleIndex(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	first, err := service.OpenGroup(ctx, "group-call-1", 1, 5001, 1, "room-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a call hash that expired before its group index.
	if err = service.client.Del(ctx, key(first.ID)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ActiveGroupCall(ctx, 5001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("stale index err = %v", err)
	}
	if _, err = service.client.Get(ctx, groupKey(5001)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("stale index must be removed: err = %v", err)
	}
	if _, err = service.OpenGroup(ctx, "group-call-2", 1, 5001, 1, "room-2", 9); err != nil {
		t.Fatalf("new room after stale index: %v", err)
	}
}

func TestJoinAndLeaveGroup(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	// The creator may call RoomToken/JoinGroup idempotently.
	if _, err = service.JoinGroup(ctx, call.ID, 1); err != nil {
		t.Fatalf("creator join: %v", err)
	}
	joined, err := service.JoinGroup(ctx, call.ID, 2)
	if err != nil || joined.State != StateAnswered {
		t.Fatalf("member join = %+v, err = %v", joined, err)
	}
	participants, err := service.Participants(ctx, call.ID)
	if err != nil || len(participants) != 2 {
		t.Fatalf("participants = %v, err = %v", participants, err)
	}
	// One member leaving must not end the room.
	if _, snapshot, ended, err := service.LeaveGroup(ctx, call.ID, 2); err != nil || ended {
		t.Fatalf("partial leave = ended %v, err = %v", ended, err)
	} else if len(snapshot) != 2 {
		t.Fatalf("snapshot = %v", snapshot)
	}
	active, err := service.ActiveGroupCall(ctx, 5001)
	if err != nil || active.State != StateAnswered {
		t.Fatalf("room after partial leave = %+v, err = %v", active, err)
	}
	if left, err := service.Joined(ctx, call.ID, 2); err != nil || left {
		t.Fatalf("member2 still joined = %v, err = %v", left, err)
	}
	// Last participant leaving ends the room and clears the index.
	_, snapshot, ended, err := service.LeaveGroup(ctx, call.ID, 1)
	if err != nil || !ended {
		t.Fatalf("last leave = ended %v, err = %v", ended, err)
	}
	if len(snapshot) != 1 || snapshot[0] != 1 {
		t.Fatalf("last snapshot = %v", snapshot)
	}
	endedCall, err := service.Get(ctx, call.ID)
	if err != nil || endedCall.State != StateEnded {
		t.Fatalf("ended call = %+v, err = %v", endedCall, err)
	}
	if _, err = service.ActiveGroupCall(ctx, 5001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("index after end = %v", err)
	}
	if _, err = service.JoinGroup(ctx, call.ID, 3); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("join ended room err = %v", err)
	}
	if _, _, _, err = service.LeaveGroup(ctx, call.ID, 3); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("leave ended room err = %v", err)
	}
}

func TestGroupRoomCapacity(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 2, "room-g", 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.JoinGroup(ctx, call.ID, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = service.JoinGroup(ctx, call.ID, 3); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("third member err = %v", err)
	}
	// Re-joining an existing participant is idempotent even when full.
	if _, err = service.JoinGroup(ctx, call.ID, 2); err != nil {
		t.Fatalf("idempotent join err = %v", err)
	}
}

func TestGroupLeaveIsIdempotent(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.JoinGroup(ctx, call.ID, 2); err != nil {
		t.Fatal(err)
	}
	if _, _, ended, err := service.LeaveGroup(ctx, call.ID, 2); err != nil || ended {
		t.Fatalf("first leave = ended %v, err = %v", ended, err)
	}
	if _, _, ended, err := service.LeaveGroup(ctx, call.ID, 2); err != nil || ended {
		t.Fatalf("second leave = ended %v, err = %v", ended, err)
	}
}

func TestGroupCallCancelRequiresRinging(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Cancel(ctx, call.ID, 1); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("cancel active room err = %v", err)
	}
}

func TestFamilyCallKeepsRingButUsesRoomLifecycle(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenFamily(ctx, "family-call", 1, 9001, 1, "room-f", MaxFamilyParticipants)
	if err != nil {
		t.Fatal(err)
	}
	if call.State != StateRinging || call.TargetType != 3 {
		t.Fatalf("family call = %+v", call)
	}
	active, err := service.ActiveGroupCall(ctx, 9001)
	if err != nil || active.ID != call.ID {
		t.Fatalf("family index while ringing = %+v, err = %v", active, err)
	}
	participants, err := service.Participants(ctx, call.ID)
	if err != nil || len(participants) != 1 || participants[0] != 1 {
		t.Fatalf("family participants = %v, err = %v", participants, err)
	}
	// Joining via the room path answers the family call.
	joined, err := service.JoinGroup(ctx, call.ID, 2)
	if err != nil || joined.State != StateAnswered {
		t.Fatalf("family join = %+v, err = %v", joined, err)
	}
	// One member leaving does not end the call.
	if _, _, ended, err := service.LeaveGroup(ctx, call.ID, 2); err != nil || ended {
		t.Fatalf("family partial leave = ended %v, err = %v", ended, err)
	}
	if active, err = service.ActiveGroupCall(ctx, 9001); err != nil || active.State != StateAnswered {
		t.Fatalf("family after partial leave = %+v, err = %v", active, err)
	}
	// Last participant leaving ends it and clears the index.
	if _, _, ended, err := service.LeaveGroup(ctx, call.ID, 1); err != nil || !ended {
		t.Fatalf("family last leave = ended %v, err = %v", ended, err)
	}
	endedCall, err := service.Get(ctx, call.ID)
	if err != nil || endedCall.State != StateEnded {
		t.Fatalf("family ended = %+v, err = %v", endedCall, err)
	}
	if _, err = service.ActiveGroupCall(ctx, 9001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("family index after end = %v", err)
	}
}

func TestFamilyTimeoutAndCancelClearIndex(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()

	timed, err := service.OpenFamily(ctx, "family-timeout", 1, 9001, 1, "room-1", MaxFamilyParticipants)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := service.Timeout(ctx, timed.CreatedAt+1); err != nil || count != 1 {
		t.Fatalf("timeout count = %d, err = %v", count, err)
	}
	if _, err = service.ActiveGroupCall(ctx, 9001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("timeout index = %v", err)
	}

	canceled, err := service.OpenFamily(ctx, "family-cancel", 1, 9001, 1, "room-2", MaxFamilyParticipants)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Cancel(ctx, canceled.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ActiveGroupCall(ctx, 9001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("cancel index = %v", err)
	}
}

func TestEndRollback(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.End(ctx, call.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ActiveGroupCall(ctx, 5001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("active after end = %v", err)
	}
	if err = service.End(ctx, "missing"); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("end missing err = %v", err)
	}
}

func TestActiveGroupCallClearsEndedIndex(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 1, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.setState(ctx, call.ID, StateEnded); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ActiveGroupCall(ctx, 5001); !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("active err = %v", err)
	}
	if _, err = service.client.Get(ctx, groupKey(5001)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("index still present: %v", err)
	}
}

func TestOpenGroupDuplicateCallIDKeepsOwner(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "same-id", 1, 5001, 1, "room-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.OpenGroup(ctx, "same-id", 2, 5002, 1, "room-2", 9); !errors.Is(err, ErrBusy) {
		t.Fatalf("duplicate call id err = %v", err)
	}
	kept, err := service.Get(ctx, call.ID)
	if err != nil || kept.FromUID != 1 || kept.ToUID != 5001 {
		t.Fatalf("call = %+v, err = %v", kept, err)
	}
}

func TestDeleteGroupIndexOnlyWhenCurrent(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	if err := service.client.Set(ctx, groupKey(5001), "old-call", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	deleted, err := runDeleteGroupIndexScript(ctx, service.client, groupKey(5001), "new-call")
	if err != nil || deleted {
		t.Fatalf("deleted = %v, err = %v", deleted, err)
	}
	if got, _ := service.client.Get(ctx, groupKey(5001)).Result(); got != "old-call" {
		t.Fatalf("index = %q", got)
	}
	deleted, err = runDeleteGroupIndexScript(ctx, service.client, groupKey(5001), "old-call")
	if err != nil || !deleted {
		t.Fatalf("deleted = %v, err = %v", deleted, err)
	}
	if _, err = service.client.Get(ctx, groupKey(5001)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("index still present: %v", err)
	}
}

func TestOpenGroupRejectsInvalid(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	cases := []struct {
		id      string
		from    int64
		groupID int64
	}{
		{"", 1, 1},
		{"x", 0, 1},
		{"x", 1, 0},
	}
	for _, item := range cases {
		if _, err := service.OpenGroup(ctx, item.id, item.from, item.groupID, 1, "r", 9); !errors.Is(err, ErrInvalidCall) {
			t.Fatalf("case %+v err = %v", item, err)
		}
	}
}

func TestParticipantsSorted(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.OpenGroup(ctx, "group-call", 9, 5001, 1, "room-g", 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []int64{3, 2} {
		if _, err = service.JoinGroup(ctx, call.ID, uid); err != nil {
			t.Fatal(err)
		}
	}
	participants, err := service.Participants(ctx, call.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{2, 3, 9}
	if len(participants) != len(want) {
		t.Fatalf("participants = %v", participants)
	}
	for i := range want {
		if participants[i] != want[i] {
			t.Fatalf("participants = %v, want %v", participants, want)
		}
	}
}

func TestAnsweredCallLeavesDeadlineIndex(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.Invite(ctx, "call-4", 1, 2, 1, "room-4")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Answer(ctx, "call-4", 2, true); err != nil {
		t.Fatal(err)
	}
	if count, err := service.Timeout(ctx, call.CreatedAt+60_000); err != nil || count != 0 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	answered, err := service.Get(ctx, "call-4")
	if err != nil || answered.State != StateAnswered {
		t.Fatalf("call = %+v, err = %v", answered, err)
	}
}

func TestEndRemovesDeadline(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	call, err := service.Invite(ctx, "call-5", 1, 2, 1, "room-5")
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Hangup(ctx, "call-5", 1); err != nil {
		t.Fatal(err)
	}
	if count, err := service.Timeout(ctx, call.CreatedAt+60_000); err != nil || count != 0 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
}

func TestAnswerAndHangupAreIdempotent(t *testing.T) {
	service, _ := newTestService(t)
	ctx := context.Background()
	if _, err := service.Invite(ctx, "call-6", 1, 2, 2, "room-6"); err != nil {
		t.Fatal(err)
	}
	first, err := service.Answer(ctx, "call-6", 2, true)
	if err != nil || first.State != StateAnswered {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	second, err := service.Answer(ctx, "call-6", 2, true)
	if err != nil || second.State != StateAnswered {
		t.Fatalf("second = %+v, err = %v", second, err)
	}
	if err = service.Hangup(ctx, "call-6", 1); err != nil {
		t.Fatal(err)
	}
	if err = service.Hangup(ctx, "call-6", 2); err != nil {
		t.Fatalf("repeat hangup err = %v", err)
	}
}
