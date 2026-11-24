package server

import (
	"context"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/model"
)

// MemberStatusEvent is one record of zim.pipe.groupMemberStatus, published by
// the group service after mute/unmute/kick lands (ADR-0043 G-10).
type MemberStatusEvent struct {
	Type        string `json:"type"` // mute / unmute / kick
	GroupID     int64  `json:"group_id"`
	UID         int64  `json:"uid"`
	MuteFlags   int32  `json:"mute_flags,omitempty"`
	MuteUntil   int64  `json:"mute_until,omitempty"`
	OperatorUID int64  `json:"operator_uid,omitempty"`
	At          int64  `json:"at"`
}

// DecodeMemberStatus parses one Kafka record.
func DecodeMemberStatus(value []byte) (*MemberStatusEvent, error) {
	event := &MemberStatusEvent{}
	if err := json.Unmarshal(value, event); err != nil {
		return nil, err
	}
	return event, nil
}

// HandleMemberStatus applies a group mute/kick to every live room of that
// group: muted members lose LiveKit publish rights, kicked members are
// evicted. unmute restores publish only when the member still holds a seat
// and is not force-muted (ADR-0043 §3.1.5, G-10).
func (s *Service) HandleMemberStatus(ctx context.Context, event *MemberStatusEvent) error {
	if event == nil || event.GroupID <= 0 {
		return nil
	}
	rooms, err := s.repo.ListByGroup(ctx, event.GroupID, 20, 0)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	for _, stored := range rooms {
		if stored.Status != model.VoiceRoomStatusLive {
			continue
		}
		room, loadErr := s.loadRoom(ctx, stored.RoomID)
		if loadErr != nil {
			log.Warnf("voiceroom member status load failed: room_id = %d, err = %v", stored.RoomID, loadErr)
			continue
		}
		switch event.Type {
		case "kick":
			s.revokePublish(ctx, room, event.UID)
			if s.media != nil {
				if err := s.media.RemoveParticipant(ctx, roomNameOf(room.ID), event.UID); err != nil {
					log.Warnf("voiceroom kick participant failed: room_id = %d, uid = %d, err = %v",
						room.ID, event.UID, err)
				}
			}
		case "mute":
			if muted := event.MuteUntil == -1 || event.MuteUntil > now; muted {
				s.revokePublish(ctx, room, event.UID)
			} else {
				s.restorePublishIfSeated(ctx, room, event.UID)
			}
		case "unmute":
			s.restorePublishIfSeated(ctx, room, event.UID)
		}
		s.pushSeats(ctx, room.ID)
	}
	return nil
}

// revokePublish strips a member's LiveKit publish grant. Even a malicious
// client that ignores the UI cannot publish afterwards: the SFU enforces it.
func (s *Service) revokePublish(ctx context.Context, room *Room, uid int64) {
	if s.media == nil || uid <= 0 {
		return
	}
	if err := s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), uid, false); err != nil {
		log.Warnf("voiceroom revoke publish failed: room_id = %d, uid = %d, err = %v", room.ID, uid, err)
	}
}

// restorePublishIfSeated re-grants publish after an unmute, but only for a
// member who still owns a seat and is not force-muted/locked.
func (s *Service) restorePublishIfSeated(ctx context.Context, room *Room, uid int64) {
	if s.media == nil || uid <= 0 {
		return
	}
	states, err := s.seats(ctx, room.ID)
	if err != nil {
		return
	}
	for _, seat := range states {
		if seat.Uid == uid && seat.Status == SeatSpeaking {
			if err := s.media.UpdateParticipantPermission(ctx, roomNameOf(room.ID), uid, true); err != nil {
				log.Warnf("voiceroom restore publish failed: room_id = %d, uid = %d, err = %v", room.ID, uid, err)
			}
			return
		}
	}
}
