package server

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

type recordingMedia struct {
	grants  map[int64]bool // uid -> canPublish
	removed []int64
}

func (r *recordingMedia) CreateRoom(context.Context, rtc.RoomReq) (*rtc.Room, error) { return nil, nil }
func (r *recordingMedia) IssueToken(context.Context, string, int64, rtc.Role) (string, error) {
	return "", nil
}
func (r *recordingMedia) JoinRoom(context.Context, string, int64, rtc.Role) (string, error) {
	return "", nil
}
func (r *recordingMedia) RemoveParticipant(_ context.Context, _ string, uid int64) error {
	r.removed = append(r.removed, uid)
	return nil
}
func (r *recordingMedia) DeleteRoom(context.Context, string) error              { return nil }
func (r *recordingMedia) MuteTrack(context.Context, string, string, bool) error { return nil }
func (r *recordingMedia) UpdateParticipantPermission(_ context.Context, _ string, uid int64, canPublish bool) error {
	if r.grants == nil {
		r.grants = make(map[int64]bool)
	}
	r.grants[uid] = canPublish
	return nil
}
func (r *recordingMedia) Events() <-chan rtc.RoomEvent { return nil }

type memberStatusRepo struct {
	rooms []*model.VoiceRoom
}

func (m *memberStatusRepo) Create(context.Context, *model.VoiceRoom) error { return nil }
func (m *memberStatusRepo) Get(context.Context, int64) (*model.VoiceRoom, error) {
	return m.rooms[0], nil
}
func (m *memberStatusRepo) UpdateStatus(context.Context, int64, int16, int64) error { return nil }
func (m *memberStatusRepo) ListByGroup(context.Context, int64, int, int64) ([]*model.VoiceRoom, error) {
	return m.rooms, nil
}
func (m *memberStatusRepo) ListPublic(context.Context, int, int64) ([]*model.VoiceRoom, error) {
	return nil, nil
}

func TestHandleMemberStatusRevokesPublish(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	repo := &memberStatusRepo{rooms: []*model.VoiceRoom{
		{RoomID: 7001, RoomType: 1, GroupID: 5001, OwnerUID: 1001, SeatCount: 8, Status: model.VoiceRoomStatusLive},
	}}
	media := &recordingMedia{}
	service := New(nil, repo, client, media, nil, nil, DefaultConfig())
	// Prime the room hash so loadRoom finds it in Redis.
	if err := service.saveRoom(context.Background(), &Room{
		ID: 7001, RoomType: 1, GroupID: 5001, OwnerUID: 1001, SeatCount: 8, Status: RoomLive,
	}); err != nil {
		t.Fatal(err)
	}
	// Mute event: publish revoked for uid 1002.
	if err := service.HandleMemberStatus(context.Background(), &MemberStatusEvent{
		Type: "mute", GroupID: 5001, UID: 1002, MuteUntil: -1,
	}); err != nil {
		t.Fatalf("handle mute: err = %v", err)
	}
	if got, ok := media.grants[1002]; !ok || got {
		t.Fatalf("mute must revoke publish: grants = %v", media.grants)
	}
	// Kick event: participant removed.
	if err := service.HandleMemberStatus(context.Background(), &MemberStatusEvent{
		Type: "kick", GroupID: 5001, UID: 1002,
	}); err != nil {
		t.Fatalf("handle kick: err = %v", err)
	}
	if len(media.removed) != 1 || media.removed[0] != 1002 {
		t.Fatalf("kick must remove participant: removed = %v", media.removed)
	}
}
