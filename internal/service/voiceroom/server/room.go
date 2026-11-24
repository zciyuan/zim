package server

import (
	"context"
	"strconv"
	"strings"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

// roomNameOf maps a snowflake room id to the LiveKit room name.
func roomNameOf(roomID int64) string { return "zim-vr-" + strconv.FormatInt(roomID, 10) }

// parseRoomName extracts the room id from a LiveKit room name.
func parseRoomName(name string) (int64, bool) {
	id, found := strings.CutPrefix(name, "zim-vr-")
	if !found || id == "" {
		return 0, false
	}
	roomID, err := strconv.ParseInt(id, 10, 64)
	if err != nil || roomID <= 0 {
		return 0, false
	}
	return roomID, true
}

// doCreateRoom creates the LiveKit room, persists it and seats the owner on
// seat 0.
func (s *Service) doCreateRoom(ctx context.Context, args createRoomArgs) (*pb.CreateRoomReply, error) {
	title := strings.TrimSpace(args.Title)
	if args.OwnerUID <= 0 || title == "" || len([]rune(title)) > 128 {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	roomType := int16(args.RoomType)
	if roomType != model.VoiceRoomTypeGroup && roomType != model.VoiceRoomTypePublic && roomType != model.VoiceRoomTypePassword {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	if roomType == model.VoiceRoomTypeGroup && args.GroupID <= 0 {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	if roomType == model.VoiceRoomTypePassword && args.Password == "" {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	seatCount := args.SeatCount
	if seatCount <= 0 {
		seatCount = s.cfg.SeatCount
	}
	if seatCount > int32(model.VoiceRoomSeatCountMax) {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	joinMode := int16(args.JoinSeatMode)
	if joinMode <= 0 {
		joinMode = model.VoiceRoomJoinFree
	}
	if joinMode != model.VoiceRoomJoinFree && joinMode != model.VoiceRoomJoinHandUp {
		return nil, errs.ErrVoiceRoomParamInvalid
	}

	passwordHash := ""
	if roomType == model.VoiceRoomTypePassword {
		var err error
		if passwordHash, err = hashPassword(args.Password); err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
	}

	now := s.now().UnixMilli()
	roomID := s.ids.Next()
	roomName := roomNameOf(roomID)
	if _, err := s.media.CreateRoom(ctx, rtc.RoomReq{Room: roomName, OwnerID: args.OwnerUID}); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	room := &Room{
		ID: roomID, RoomType: roomType, GroupID: args.GroupID, OwnerUID: args.OwnerUID,
		Title: title, SeatCount: seatCount, JoinSeatMode: joinMode,
		Status: RoomLive, CreatedAt: now,
	}
	if err := s.repo.Create(ctx, &model.VoiceRoom{
		RoomID: roomID, RoomType: roomType, GroupID: args.GroupID, OwnerUID: args.OwnerUID,
		Title: title, SeatCount: int16(seatCount), JoinSeatMode: joinMode,
		PasswordHash: passwordHash, Status: RoomLive, CreatedAt: now,
	}); err != nil {
		_ = s.media.DeleteRoom(context.Background(), roomName)
		return nil, errs.ErrInternal.WithCause(err)
	}
	if err := s.saveRoom(ctx, room); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if err := initSeatsScript.Run(ctx, s.client, []string{seatsKey(roomID)}, seatCount).Err(); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if _, err := s.addMember(ctx, room, args.OwnerUID); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	s.touch(ctx, roomID)
	// The owner sits on seat 0 from the start.
	if _, err := takeSeatScript.Run(ctx, s.client, []string{roomKey(roomID), seatsKey(roomID), handupKey(roomID)},
		args.OwnerUID, 0, now, 1).Int64(); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if err := s.media.UpdateParticipantPermission(ctx, roomName, args.OwnerUID, true); err != nil {
		log.Warnf("grant owner publish failed: uid = %d, room_id = %d, err = %v", args.OwnerUID, roomID, err)
	}
	token, err := s.media.JoinRoom(ctx, roomName, args.OwnerUID, rtc.RolePublisher)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &pb.CreateRoomReply{
		Room:     roomInfo(room),
		Token:    token,
		SfuUrl:   s.cfg.SFUURL,
		ExpireAt: now + s.cfg.TokenTTL.Milliseconds(),
	}, nil
}

// doJoinRoom validates the password and issues a listener token. Listeners
// join without publish permission; publish is granted by the seat machine.
func (s *Service) doJoinRoom(ctx context.Context, args joinRoomArgs) (*pb.JoinRoomReply, error) {
	if args.UID <= 0 {
		return nil, errs.ErrVoiceRoomParamInvalid
	}
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	if room.RoomType == model.VoiceRoomTypePassword {
		stored, getErr := s.repo.Get(ctx, room.ID)
		if getErr != nil || !verifyPassword(stored.PasswordHash, args.Password) {
			return nil, errs.ErrVoiceRoomPasswordWrong
		}
	}
	roomName := roomNameOf(room.ID)
	token, err := s.media.JoinRoom(ctx, roomName, args.UID, rtc.RoleListener)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if _, err = s.addMember(ctx, room, args.UID); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	_ = s.client.ZRem(ctx, emptyTimeoutKey, strconv.FormatInt(room.ID, 10)).Err()
	s.touch(ctx, room.ID)
	seats, err := s.seats(ctx, room.ID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &pb.JoinRoomReply{
		Token:    token,
		SfuUrl:   s.cfg.SFUURL,
		ExpireAt: s.now().UnixMilli() + s.cfg.TokenTTL.Milliseconds(),
		Seats:    seatInfos(seats),
	}, nil
}

// doRefreshToken renews a member token after re-checking the member is still
// in the room (not kicked) and the room is live.
func (s *Service) doRefreshToken(ctx context.Context, args refreshTokenArgs) (*pb.RefreshTokenReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	if !room.Live() {
		return nil, errs.ErrVoiceRoomClosed
	}
	exists, err := s.client.HExists(ctx, roomKey(room.ID), "m:"+strconv.FormatInt(args.UID, 10)).Result()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if !exists {
		return nil, errs.ErrNotInVoiceRoom
	}
	token, err := s.media.JoinRoom(ctx, roomNameOf(room.ID), args.UID, rtc.RoleListener)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &pb.RefreshTokenReply{
		Token:    token,
		ExpireAt: s.now().UnixMilli() + s.cfg.TokenTTL.Milliseconds(),
	}, nil
}

// doGetRoom returns the runtime room snapshot.
func (s *Service) doGetRoom(ctx context.Context, args getRoomArgs) (*pb.RoomInfo, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		return nil, err
	}
	return roomInfo(room), nil
}

// doCloseRoom closes a room and broadcasts 1913.
func (s *Service) doCloseRoom(ctx context.Context, args closeRoomArgs) (*pb.SeatOperationReply, error) {
	room, err := s.loadRoom(ctx, args.RoomID)
	if err != nil {
		// A room whose Redis state is gone may already be closed in PG.
		if errs.IsCode(err, 180100) || errs.IsCode(err, 180107) {
			stored, getErr := s.repo.Get(ctx, args.RoomID)
			if getErr == nil && stored.Status == model.VoiceRoomStatusClosed {
				return &pb.SeatOperationReply{Ok: true}, nil
			}
		}
		return nil, err
	}
	if args.Reason == CloseReasonOwner && args.OperatorUID != room.OwnerUID {
		return nil, errs.ErrNoSeatPermission
	}
	uids, err := s.memberUIDs(ctx, room)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	roomName := roomNameOf(room.ID)
	if err = s.media.DeleteRoom(ctx, roomName); err != nil {
		log.Warnf("delete livekit room failed: room_id = %d, err = %v", room.ID, err)
	}
	now := s.now().UnixMilli()
	if err = s.repo.UpdateStatus(ctx, room.ID, model.VoiceRoomStatusClosed, now); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	s.deleteRoomKeys(ctx, room.ID)
	s.pushClosed(ctx, room, args.Reason, uids)
	return &pb.SeatOperationReply{Ok: true}, nil
}

// doListRooms pages group rooms or live public rooms.
func (s *Service) doListRooms(ctx context.Context, args listRoomsArgs) (*pb.ListRoomsReply, error) {
	limit := int(args.Limit)
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var (
		rooms []*model.VoiceRoom
		err   error
	)
	if args.GroupID > 0 {
		rooms, err = s.repo.ListByGroup(ctx, args.GroupID, limit+1, args.Offset)
	} else {
		rooms, err = s.repo.ListPublic(ctx, limit+1, args.Offset)
	}
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	hasMore := len(rooms) > limit
	if hasMore {
		rooms = rooms[:limit]
	}
	reply := &pb.ListRoomsReply{Rooms: make([]*pb.RoomInfo, 0, len(rooms)), HasMore: hasMore}
	for _, room := range rooms {
		reply.Rooms = append(reply.Rooms, roomInfo(&Room{
			ID: room.RoomID, RoomType: room.RoomType, GroupID: room.GroupID,
			OwnerUID: room.OwnerUID, Title: room.Title, Announcement: room.Announcement,
			SeatCount: int32(room.SeatCount), JoinSeatMode: room.JoinSeatMode,
			Status: room.Status, CreatedAt: room.CreatedAt,
		}))
	}
	return reply, nil
}

// ReapReconnects frees seats whose reconnect grace expired and revokes the
// publish permission on the media plane.
func (s *Service) ReapReconnects(ctx context.Context) (int64, error) {
	if s.client == nil {
		return 0, nil
	}
	now := s.now().UnixMilli()
	entries, err := s.client.ZRangeByScore(ctx, reconnectTimeoutKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		return 0, err
	}
	var reclaimed int64
	for _, member := range entries {
		parts := strings.Split(member, ":")
		if len(parts) != 3 {
			_ = s.client.ZRem(ctx, reconnectTimeoutKey, member).Err()
			continue
		}
		roomID, parseErr := strconv.ParseInt(parts[0], 10, 64)
		seatIndex, seatErr := strconv.ParseInt(parts[1], 10, 64)
		uid, uidErr := strconv.ParseInt(parts[2], 10, 64)
		if parseErr != nil || seatErr != nil || uidErr != nil {
			_ = s.client.ZRem(ctx, reconnectTimeoutKey, member).Err()
			continue
		}
		result, runErr := reapSeatScript.Run(ctx, s.client, []string{seatsKey(roomID)}, seatIndex, uid).Int64()
		if runErr != nil {
			log.Warnf("reap seat script failed: room_id = %d, seat = %d, err = %v", roomID, seatIndex, runErr)
			_ = s.client.ZRem(ctx, reconnectTimeoutKey, member).Err()
			continue
		}
		if result == 1 {
			reclaimed++
			if err = s.media.UpdateParticipantPermission(ctx, roomNameOf(roomID), uid, false); err != nil {
				log.Warnf("revoke publish on reconnect timeout failed: uid = %d, room_id = %d, err = %v", uid, roomID, err)
			}
			s.pushSeats(ctx, roomID)
		}
		_ = s.client.ZRem(ctx, reconnectTimeoutKey, member).Err()
	}
	return reclaimed, nil
}

// ReapEmptyRooms closes rooms whose last member left more than
// emptyTimeout ago and broadcasts 1913.
func (s *Service) ReapEmptyRooms(ctx context.Context) (int64, error) {
	if s.client == nil {
		return 0, nil
	}
	now := s.now().UnixMilli()
	entries, err := s.client.ZRangeByScore(ctx, emptyTimeoutKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		return 0, err
	}
	var closed int64
	for _, member := range entries {
		roomID, parseErr := strconv.ParseInt(member, 10, 64)
		if parseErr != nil {
			_ = s.client.ZRem(ctx, emptyTimeoutKey, member).Err()
			continue
		}
		room, loadErr := s.loadRoom(ctx, roomID)
		if loadErr != nil {
			// Redis state is gone: nothing left to guard, only stale keys.
			s.deleteRoomKeys(ctx, roomID)
			_ = s.client.ZRem(ctx, emptyTimeoutKey, member).Err()
			continue
		}
		online, onlineErr := s.client.HGet(ctx, roomKey(roomID), "online").Int64()
		if onlineErr != nil || online > 0 {
			// Someone rejoined before the timeout fired.
			_ = s.client.ZRem(ctx, emptyTimeoutKey, member).Err()
			continue
		}
		if err = s.media.DeleteRoom(ctx, roomNameOf(roomID)); err != nil {
			log.Warnf("delete livekit room on empty timeout failed: room_id = %d, err = %v", roomID, err)
		}
		if err = s.repo.UpdateStatus(ctx, roomID, model.VoiceRoomStatusClosed, now); err != nil {
			log.Warnf("close empty room status update failed: room_id = %d, err = %v", roomID, err)
		}
		s.deleteRoomKeys(ctx, roomID)
		_ = s.client.ZRem(ctx, emptyTimeoutKey, member).Err()
		s.pushClosed(ctx, room, CloseReasonEmptyTimeout, nil)
		closed++
	}
	return closed, nil
}

// loadRoom reads the room hash, re-hydrating from PostgreSQL when the Redis
// key is missing but the room is still live.
func (s *Service) loadRoom(ctx context.Context, roomID int64) (*Room, error) {
	values, err := s.client.HGetAll(ctx, roomKey(roomID)).Result()
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if len(values) > 0 {
		room, parseErr := roomFromHash(values)
		if parseErr != nil {
			return nil, errs.ErrInternal.WithCause(parseErr)
		}
		return room, nil
	}
	stored, err := s.repo.Get(ctx, roomID)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrVoiceRoomNotExist
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	room := &Room{
		ID: stored.RoomID, RoomType: stored.RoomType, GroupID: stored.GroupID,
		OwnerUID: stored.OwnerUID, Title: stored.Title, Announcement: stored.Announcement,
		SeatCount: int32(stored.SeatCount), JoinSeatMode: stored.JoinSeatMode,
		Status: stored.Status, CreatedAt: stored.CreatedAt,
	}
	if room.Status != RoomLive {
		return nil, errs.ErrVoiceRoomClosed
	}
	if err = s.saveRoom(ctx, room); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if err = initSeatsScript.Run(ctx, s.client, []string{seatsKey(roomID)}, room.SeatCount).Err(); err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return room, nil
}

func (s *Service) saveRoom(ctx context.Context, room *Room) error {
	err := s.client.HSet(ctx, roomKey(room.ID), map[string]any{
		"id": room.ID, "room_type": room.RoomType, "group_id": room.GroupID,
		"owner_uid": room.OwnerUID, "title": room.Title, "announcement": room.Announcement,
		"seat_count": room.SeatCount, "join_seat_mode": room.JoinSeatMode,
		"status": room.Status, "created_at": room.CreatedAt, "online": 0,
	}).Err()
	if err != nil {
		return err
	}
	return s.client.Expire(ctx, roomKey(room.ID), s.cfg.RoomTTL).Err()
}

func roomFromHash(values map[string]string) (*Room, error) {
	room := &Room{}
	room.ID, _ = strconv.ParseInt(values["id"], 10, 64)
	room.RoomType = int16(parseInt(values["room_type"]))
	room.GroupID, _ = strconv.ParseInt(values["group_id"], 10, 64)
	room.OwnerUID, _ = strconv.ParseInt(values["owner_uid"], 10, 64)
	room.Title = values["title"]
	room.Announcement = values["announcement"]
	room.SeatCount = int32(parseInt(values["seat_count"]))
	room.JoinSeatMode = int16(parseInt(values["join_seat_mode"]))
	room.Status = int16(parseInt(values["status"]))
	room.CreatedAt, _ = strconv.ParseInt(values["created_at"], 10, 64)
	if room.ID <= 0 {
		return nil, dueerrors.ErrInvalidArgument
	}
	return room, nil
}

func parseInt(value string) int64 {
	n, _ := strconv.ParseInt(value, 10, 64)
	return n
}

// addMember registers presence and returns the new online count.
func (s *Service) addMember(ctx context.Context, room *Room, uid int64) (int64, error) {
	return memberJoinScript.Run(ctx, s.client, []string{roomKey(room.ID)}, uid, s.now().UnixMilli()).Int64()
}

// removeMember unregisters presence and returns the new online count; when it
// drops to zero the empty timeout is armed.
func (s *Service) removeMember(ctx context.Context, room *Room, uid int64) (int64, error) {
	online, err := memberLeaveScript.Run(ctx, s.client, []string{roomKey(room.ID)}, uid).Int64()
	if err != nil {
		return 0, err
	}
	if online == 0 {
		now := s.now().UnixMilli()
		_ = s.client.ZAdd(ctx, emptyTimeoutKey, redis.Z{
			Score:  float64(now + s.cfg.EmptyTimeout.Milliseconds()),
			Member: strconv.FormatInt(room.ID, 10),
		}).Err()
	}
	return online, nil
}

func (s *Service) touch(ctx context.Context, roomID int64) {
	ttl := s.cfg.RoomTTL
	_ = s.client.Expire(ctx, roomKey(roomID), ttl).Err()
	_ = s.client.Expire(ctx, seatsKey(roomID), ttl).Err()
	_ = s.client.Expire(ctx, handupKey(roomID), ttl).Err()
}

func (s *Service) deleteRoomKeys(ctx context.Context, roomID int64) {
	_ = s.client.Del(ctx, roomKey(roomID), seatsKey(roomID), handupKey(roomID)).Err()
}

func (s *Service) memberUIDs(ctx context.Context, room *Room) ([]int64, error) {
	keys, err := s.client.HKeys(ctx, roomKey(room.ID)).Result()
	if err != nil {
		return nil, err
	}
	uids := make([]int64, 0, len(keys))
	for _, key := range keys {
		if uid, found := strings.CutPrefix(key, "m:"); found {
			if n, parseErr := strconv.ParseInt(uid, 10, 64); parseErr == nil && n > 0 {
				uids = append(uids, n)
			}
		}
	}
	return uids, nil
}

func roomInfo(room *Room) *pb.RoomInfo {
	return &pb.RoomInfo{
		RoomId: room.ID, RoomType: int32(room.RoomType), GroupId: room.GroupID,
		OwnerUid: room.OwnerUID, Title: room.Title, Announcement: room.Announcement,
		SeatCount: room.SeatCount, JoinSeatMode: int32(room.JoinSeatMode),
		Status: int32(room.Status), CreatedAt: room.CreatedAt,
	}
}
