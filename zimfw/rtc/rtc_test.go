package rtc

import (
	"context"
	"testing"
)

func TestDummyProvider(t *testing.T) {
	provider := NewDummy()
	room, err := provider.CreateRoom(context.Background(), RoomReq{Room: "zim-call-1", CallID: "1"})
	if err != nil || room.ID != "zim-call-1" {
		t.Fatalf("room = %+v, err = %v", room, err)
	}
	again, err := provider.CreateRoom(context.Background(), RoomReq{Room: "zim-call-1"})
	if err != nil || again != room {
		t.Fatal("CreateRoom must be idempotent")
	}
	token, err := provider.IssueToken(context.Background(), room.ID, 10001, RolePublisher)
	if err != nil || token == "" {
		t.Fatalf("token = %q, err = %v", token, err)
	}
	if err = provider.MuteTrack(context.Background(), room.ID, "audio", true); err != nil {
		t.Fatal(err)
	}
	event := <-provider.Events()
	if event.Room != room.ID || event.Type != "track_muted" || !event.Muted {
		t.Fatalf("event = %+v", event)
	}
	if err = provider.RemoveParticipant(context.Background(), room.ID, 10001); err != nil {
		t.Fatal(err)
	}
	if _, err = provider.IssueToken(context.Background(), "missing", 1, RoleParticipant); err != ErrRoomNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestDummyDeleteRoom(t *testing.T) {
	provider := NewDummy()
	if _, err := provider.CreateRoom(context.Background(), RoomReq{Room: "room-del"}); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.IssueToken(context.Background(), "room-del", 1, RolePublisher); err != nil {
		t.Fatal(err)
	}
	if err := provider.DeleteRoom(context.Background(), "room-del"); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.IssueToken(context.Background(), "room-del", 1, RolePublisher); err != ErrRoomNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestDummyJoinAndPermission(t *testing.T) {
	provider := NewDummy()
	if _, err := provider.CreateRoom(context.Background(), RoomReq{Room: "vr-1"}); err != nil {
		t.Fatal(err)
	}
	// A listener joins without publish permission.
	if _, err := provider.JoinRoom(context.Background(), "vr-1", 10002, RoleListener); err != nil {
		t.Fatal(err)
	}
	if permit, ok := provider.Permit("vr-1", 10002); !ok || permit {
		t.Fatalf("listener permit = %v, ok = %v", permit, ok)
	}
	// Taking a seat grants publish through the server API.
	if err := provider.UpdateParticipantPermission(context.Background(), "vr-1", 10002, true); err != nil {
		t.Fatal(err)
	}
	if permit, ok := provider.Permit("vr-1", 10002); !ok || !permit {
		t.Fatalf("granted permit = %v, ok = %v", permit, ok)
	}
	// Revoking drops it again.
	if err := provider.UpdateParticipantPermission(context.Background(), "vr-1", 10002, false); err != nil {
		t.Fatal(err)
	}
	if permit, _ := provider.Permit("vr-1", 10002); permit {
		t.Fatal("permit must be revoked")
	}
	// A missing room is rejected.
	if err := provider.UpdateParticipantPermission(context.Background(), "missing", 1, true); err != ErrRoomNotFound {
		t.Fatalf("err = %v", err)
	}
	if _, err := provider.JoinRoom(context.Background(), "missing", 1, RoleParticipant); err != ErrRoomNotFound {
		t.Fatalf("join err = %v", err)
	}
	// Deleting the room clears the permission bookkeeping.
	if err := provider.DeleteRoom(context.Background(), "vr-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := provider.Permit("vr-1", 10002); ok {
		t.Fatal("permits must be cleared on delete")
	}
}
