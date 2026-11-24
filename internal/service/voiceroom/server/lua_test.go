package server

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// luaHarness prepares a minimal room hash plus seat hash for direct script
// tests against miniredis.
type luaHarness struct {
	client redis.UniversalClient
	roomID int64
	ctx    context.Context
}

func newLuaHarness(t *testing.T, mode, seatCount int64) *luaHarness {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	roomID := int64(42)
	ctx := context.Background()
	if err := client.HSet(ctx, roomKey(roomID), map[string]any{
		"status": 1, "owner_uid": 1001, "join_seat_mode": mode, "seat_count": seatCount,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := initSeatsScript.Run(ctx, client, []string{seatsKey(roomID)}, seatCount).Err(); err != nil {
		t.Fatal(err)
	}
	return &luaHarness{client: client, roomID: roomID, ctx: ctx}
}

func (h *luaHarness) seat(index int64) string {
	value, err := h.client.HGet(h.ctx, seatsKey(h.roomID), strconv.FormatInt(index, 10)).Result()
	if err != nil {
		return ""
	}
	return value
}

func TestTakeSeatScript(t *testing.T) {
	h := newLuaHarness(t, 1, 8)

	// A member may sit in free mode.
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	result, err := takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 1, 10, 0).Int64()
	if err != nil || result != 1 {
		t.Fatalf("result = %d, err = %v", result, err)
	}
	if h.seat(1) != "1002:1" {
		t.Fatalf("seat = %q", h.seat(1))
	}
	// Occupied seat.
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1003, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1003, 1, 10, 0).Int64(); result != luaErrSeatOccupied {
		t.Fatalf("occupied result = %d", result)
	}
	// Already seated elsewhere.
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 2, 10, 0).Int64(); result != luaErrAlreadySeated {
		t.Fatalf("double seat result = %d", result)
	}
	// Not a member.
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1004, 2, 10, 0).Int64(); result != luaErrNotRoomMember {
		t.Fatalf("non-member result = %d", result)
	}
	// Bad index.
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1004, 9, 10, 0).Int64(); result != luaErrBadSeatIndex {
		t.Fatalf("bad index result = %d", result)
	}
	// Locked seat.
	if err := h.client.HSet(h.ctx, seatsKey(h.roomID), "3", "0:4").Err(); err != nil {
		t.Fatal(err)
	}
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1003, 3, 10, 0).Int64(); result != luaErrSeatLocked {
		t.Fatalf("locked result = %d", result)
	}
	// Closed room.
	if err := h.client.HSet(h.ctx, roomKey(h.roomID), "status", 2).Err(); err != nil {
		t.Fatal(err)
	}
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1003, 2, 10, 0).Int64(); result != luaErrRoomClosed {
		t.Fatalf("closed result = %d", result)
	}
	// Missing room.
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(77), seatsKey(77), handupKey(77)}, 1003, 0, 10, 0).Int64(); result != luaErrRoomMissing {
		t.Fatalf("missing result = %d", result)
	}
}

func TestTakeSeatHandUpMode(t *testing.T) {
	h := newLuaHarness(t, 2, 8)
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1001, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	// Without approval a member is denied.
	result, err := takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 1, 10, 0).Int64()
	if err != nil || result != luaErrNoPermission {
		t.Fatalf("unapproved result = %d, err = %v", result, err)
	}
	// The owner bypasses the queue.
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1001, 1, 10, 0).Int64(); result != 1 {
		t.Fatalf("owner result = %d", result)
	}
	// Approval with a queue entry seats the member and clears the queue.
	if _, err := handUpScript.Run(h.ctx, h.client, []string{handupKey(h.roomID)}, 1002, 10).Int64(); err != nil {
		t.Fatal(err)
	}
	if result, _ = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 2, 10, 1).Int64(); result != 2 {
		t.Fatalf("approved result = %d", result)
	}
	if exists, _ := h.client.ZScore(h.ctx, handupKey(h.roomID), "1002").Result(); exists > 0 {
		t.Fatal("queue entry not cleared after approval")
	}
}

func TestLeaveKickMuteLockScripts(t *testing.T) {
	h := newLuaHarness(t, 1, 8)
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	if _, err := takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 1, 10, 0).Int64(); err != nil {
		t.Fatal(err)
	}
	// Force mute by owner.
	result, err := forceMuteSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1001, 1, 1).Int64()
	if err != nil || result != 1 || h.seat(1) != "1002:3" {
		t.Fatalf("mute result = %d, seat = %q, err = %v", result, h.seat(1), err)
	}
	// Non-owner denied.
	if result, _ = forceMuteSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1002, 1, 1).Int64(); result != luaErrNoPermission {
		t.Fatalf("non-owner mute result = %d", result)
	}
	// Unmute.
	if result, _ = forceMuteSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1001, 1, 0).Int64(); result != 1 || h.seat(1) != "1002:1" {
		t.Fatalf("unmute result = %d, seat = %q", result, h.seat(1))
	}
	// Kick by owner with matching uid.
	if result, _ = kickSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1001, 1002, 1).Int64(); result != 1 || h.seat(1) != "0:0" {
		t.Fatalf("kick result = %d, seat = %q", result, h.seat(1))
	}
	// Lock empty seat, then unlock.
	if result, _ = lockSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1001, 2, 1).Int64(); result != 2 || h.seat(2) != "0:4" {
		t.Fatalf("lock result = %d, seat = %q", result, h.seat(2))
	}
	if result, _ = lockSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID)}, 1001, 2, 0).Int64(); result != 2 || h.seat(2) != "0:0" {
		t.Fatalf("unlock result = %d, seat = %q", result, h.seat(2))
	}
	// Leave by the member frees the seat.
	if _, err = takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 3, 10, 0).Int64(); err != nil {
		t.Fatal(err)
	}
	if result, _ = leaveSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1002).Int64(); result != 3 || h.seat(3) != "0:0" {
		t.Fatalf("leave result = %d, seat = %q", result, h.seat(3))
	}
	if result, _ = leaveSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1002).Int64(); result != luaErrSeatNotFound {
		t.Fatalf("double leave result = %d", result)
	}
}

func TestReconnectScripts(t *testing.T) {
	h := newLuaHarness(t, 1, 8)
	if _, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 1).Int64(); err != nil {
		t.Fatal(err)
	}
	if _, err := takeSeatScript.Run(h.ctx, h.client,
		[]string{roomKey(h.roomID), seatsKey(h.roomID), handupKey(h.roomID)}, 1002, 1, 10, 0).Int64(); err != nil {
		t.Fatal(err)
	}
	// Disconnect marks RECONNECTING.
	result, err := markReconnectScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1002).Int64()
	if err != nil || result != 1 || h.seat(1) != "1002:5" {
		t.Fatalf("mark result = %d, seat = %q, err = %v", result, h.seat(1), err)
	}
	// Not seated → -7.
	if result, _ = markReconnectScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1003).Int64(); result != luaErrSeatNotFound {
		t.Fatalf("mark empty result = %d", result)
	}
	// Restore after rejoin.
	if result, _ = restoreSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1002).Int64(); result != 1 || h.seat(1) != "1002:1" {
		t.Fatalf("restore result = %d, seat = %q", result, h.seat(1))
	}
	// Reaping an already-restored seat is a no-op (returns 0).
	if result, _ = reapSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1, 1002).Int64(); result != 0 {
		t.Fatalf("reap restored result = %d", result)
	}
	// Reconnect again, then reap.
	if _, err = markReconnectScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1002).Int64(); err != nil {
		t.Fatal(err)
	}
	if result, _ = reapSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 1, 1002).Int64(); result != 1 || h.seat(1) != "0:0" {
		t.Fatalf("reap result = %d, seat = %q", result, h.seat(1))
	}
	// Reaping with a mismatched uid is a no-op.
	if err := h.client.HSet(h.ctx, seatsKey(h.roomID), "2", "1003:5").Err(); err != nil {
		t.Fatal(err)
	}
	if result, _ = reapSeatScript.Run(h.ctx, h.client, []string{seatsKey(h.roomID)}, 2, 1004).Int64(); result != 0 {
		t.Fatalf("reap mismatch result = %d", result)
	}
}

func TestMemberScripts(t *testing.T) {
	h := newLuaHarness(t, 1, 8)
	online, err := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 1).Int64()
	if err != nil || online != 1 {
		t.Fatalf("join online = %d, err = %v", online, err)
	}
	// Idempotent join.
	online, _ = memberJoinScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002, 2).Int64()
	if online != 1 {
		t.Fatalf("idempotent join online = %d", online)
	}
	online, _ = memberLeaveScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002).Int64()
	if online != 0 {
		t.Fatalf("leave online = %d", online)
	}
	// Idempotent leave.
	online, _ = memberLeaveScript.Run(h.ctx, h.client, []string{roomKey(h.roomID)}, 1002).Int64()
	if online != 0 {
		t.Fatalf("idempotent leave online = %d", online)
	}
	// Missing room.
	if result, _ := memberJoinScript.Run(h.ctx, h.client, []string{roomKey(77)}, 1, 1).Int64(); result != luaErrRoomMissing {
		t.Fatalf("missing room result = %d", result)
	}
}

func TestHandUpScript(t *testing.T) {
	h := newLuaHarness(t, 1, 8)
	result, err := handUpScript.Run(h.ctx, h.client, []string{handupKey(h.roomID)}, 1002, 10).Int64()
	if err != nil || result != 1 {
		t.Fatalf("hand up result = %d, err = %v", result, err)
	}
	if result, _ = handUpScript.Run(h.ctx, h.client, []string{handupKey(h.roomID)}, 1002, 20).Int64(); result != -1 {
		t.Fatalf("double hand up result = %d", result)
	}
	// FIFO: first member pops first.
	if _, err = handUpScript.Run(h.ctx, h.client, []string{handupKey(h.roomID)}, 1003, 5).Int64(); err != nil {
		t.Fatal(err)
	}
	member, err := h.client.ZRange(h.ctx, handupKey(h.roomID), 0, 0).Result()
	if err != nil || len(member) != 1 || member[0] != "1003" {
		t.Fatalf("oldest hand up = %v, err = %v", member, err)
	}
}
