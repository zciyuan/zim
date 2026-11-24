package logic

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	rtcwire "github.com/zciyuan/ZIM/internal/protocol/rtc"
	vrclient "github.com/zciyuan/ZIM/internal/service/voiceroom/client"
	vrpb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// voiceroom handlers forward ops 1808-1819 to the zim-voiceroom mesh service
// (ADR-0043). The node stays a thin protocol layer; all seat state machine
// logic lives in the voiceroom service.
func (c *core) voiceRoomCreate(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomCreate, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.VoiceRoomCreateReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpVoiceRoomCreate, nil, errs.ErrInternal)
			return
		}
		client, err := vrclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomCreate, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.CreateRoom(ctx.Context(), &vrpb.CreateRoomArgs{
			OwnerUid: uid, RoomType: req.GetRoomType(), GroupId: req.GetGroupId(),
			Title: req.GetTitle(), Password: req.GetPassword(),
			SeatCount: req.GetSeatCount(), JoinSeatMode: req.GetJoinSeatMode(),
		})
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomCreate, nil, err)
			return
		}
		payload, _ := proto.Marshal(&rtcwire.VoiceRoomCreateRes{
			RoomId: strconv.FormatInt(reply.GetRoom().GetRoomId(), 10), RoomType: reply.GetRoom().GetRoomType(),
			GroupId: reply.GetRoom().GetGroupId(), OwnerUid: reply.GetRoom().GetOwnerUid(),
			Title: reply.GetRoom().GetTitle(), SeatCount: reply.GetRoom().GetSeatCount(),
			JoinSeatMode: reply.GetRoom().GetJoinSeatMode(), Token: reply.GetToken(),
			SfuUrl: reply.GetSfuUrl(), ExpireAt: reply.GetExpireAt(),
		})
		c.respond(ctx, contract.OpVoiceRoomCreate, payload, nil)
	})
}

func (c *core) voiceRoomList(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		if _, _, err := multi.Parse(ctx.UID()); err != nil {
			c.respond(ctx, contract.OpVoiceRoomList, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.VoiceRoomListReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpVoiceRoomList, nil, errs.ErrInternal)
			return
		}
		client, err := vrclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomList, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.ListRooms(ctx.Context(), &vrpb.ListRoomsArgs{
			GroupId: req.GetGroupId(), Offset: int64(req.GetOffset()), Limit: req.GetLimit(),
		})
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomList, nil, err)
			return
		}
		res := &rtcwire.VoiceRoomListRes{Rooms: make([]*rtcwire.VoiceRoomItem, 0, len(reply.GetRooms())), HasMore: reply.GetHasMore()}
		for _, room := range reply.GetRooms() {
			res.Rooms = append(res.Rooms, &rtcwire.VoiceRoomItem{
				RoomId: strconv.FormatInt(room.GetRoomId(), 10), RoomType: room.GetRoomType(),
				GroupId: room.GetGroupId(), OwnerUid: room.GetOwnerUid(), Title: room.GetTitle(),
				SeatCount: room.GetSeatCount(),
			})
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpVoiceRoomList, payload, nil)
	})
}

func (c *core) voiceRoomJoin(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomJoin, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.VoiceRoomJoinReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpVoiceRoomJoin, nil, errs.ErrInternal)
			return
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			c.respond(ctx, contract.OpVoiceRoomJoin, nil, errs.ErrVoiceRoomNotExist)
			return
		}
		client, err := vrclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomJoin, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.JoinRoom(ctx.Context(), &vrpb.JoinRoomArgs{RoomId: roomID, Uid: uid, Password: req.GetPassword()})
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomJoin, nil, err)
			return
		}
		seats := make([]*rtcwire.SeatState, 0, len(reply.GetSeats()))
		for _, seat := range reply.GetSeats() {
			seats = append(seats, &rtcwire.SeatState{SeatIndex: seat.GetSeatIndex(), Uid: seat.GetUid(), Status: seat.GetStatus()})
		}
		payload, _ := proto.Marshal(&rtcwire.VoiceRoomJoinRes{
			RoomId: req.GetRoomId(), Token: reply.GetToken(), SfuUrl: reply.GetSfuUrl(),
			ExpireAt: reply.GetExpireAt(), Seats: seats,
		})
		c.respond(ctx, contract.OpVoiceRoomJoin, payload, nil)
	})
}

func (c *core) voiceRoomRefreshToken(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomRefreshToken, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.VoiceRoomRefreshTokenReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpVoiceRoomRefreshToken, nil, errs.ErrInternal)
			return
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			c.respond(ctx, contract.OpVoiceRoomRefreshToken, nil, errs.ErrVoiceRoomNotExist)
			return
		}
		client, err := vrclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomRefreshToken, nil, errs.ErrServerBusy)
			return
		}
		reply, err := client.RefreshToken(ctx.Context(), &vrpb.RefreshTokenArgs{RoomId: roomID, Uid: uid})
		if err != nil {
			c.respond(ctx, contract.OpVoiceRoomRefreshToken, nil, err)
			return
		}
		payload, _ := proto.Marshal(&rtcwire.VoiceRoomRefreshTokenRes{Token: reply.GetToken(), ExpireAt: reply.GetExpireAt()})
		c.respond(ctx, contract.OpVoiceRoomRefreshToken, payload, nil)
	})
}

// seatOperation is the shared voiceroom seat-op request path. On success the
// node broadcasts the 1912 seat update to every room member (the mesh binary
// cannot reach the node cluster, ADR-0043 §5.4).
func (c *core) seatOperation(ctx node.Context, opcode int32, invoke func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error)) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, opcode, nil, errs.ErrUnauthorized)
			return
		}
		client, err := vrclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, opcode, nil, errs.ErrServerBusy)
			return
		}
		roomID, reply, err := invoke(client, uid)
		if err != nil {
			c.respond(ctx, opcode, nil, err)
			return
		}
		seats := make([]*rtcwire.SeatState, 0, len(reply.GetSeats()))
		for _, seat := range reply.GetSeats() {
			seats = append(seats, &rtcwire.SeatState{SeatIndex: seat.GetSeatIndex(), Uid: seat.GetUid(), Status: seat.GetStatus()})
		}
		payload, _ := proto.Marshal(&rtcwire.SeatOperationRes{Ok: reply.GetOk(), Seats: seats})
		c.respond(ctx, opcode, payload, nil)
		// Room-wide 1912 after every state-changing seat op.
		if roomID > 0 {
			c.pushVoiceRoomSeats(ctx.Context(), roomID, seats)
		}
	})
}

// pushVoiceRoomSeats broadcasts the 1912 seat snapshot to every member present
// in the room hash (zim:vr:room:{id}, m:* fields).
func (c *core) pushVoiceRoomSeats(ctx context.Context, roomID int64, seats []*rtcwire.SeatState) {
	if c.cache == nil {
		return
	}
	fields, err := c.cache.HGetAll(ctx, "zim:vr:room:"+strconv.FormatInt(roomID, 10)).Result()
	if err != nil {
		return
	}
	uids := make([]int64, 0, len(fields))
	for field := range fields {
		if len(field) < 2 || field[:2] != "m:" {
			continue
		}
		if memberUID, parseErr := strconv.ParseInt(field[2:], 10, 64); parseErr == nil && memberUID > 0 {
			uids = append(uids, memberUID)
		}
	}
	if len(uids) == 0 {
		return
	}
	payload, err := proto.Marshal(&rtcwire.SeatUpdatePush{RoomId: strconv.FormatInt(roomID, 10), Seats: seats})
	if err != nil {
		return
	}
	seen := make(map[int64]struct{}, len(uids)*2)
	for _, memberUID := range uids {
		for _, key := range c.deviceKeys(ctx, memberUID) {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			c.pushDeviceKeys(ctx, []int64{key}, contract.OpPushSeatUpdate, payload, false)
		}
	}
}

func (c *core) takeSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpTakeSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.TakeSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.TakeSeat(ctx.Context(), &vrpb.TakeSeatArgs{RoomId: roomID, Uid: uid, SeatIndex: req.GetSeatIndex()})
		return roomID, reply, err
	})
}

func (c *core) leaveSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpLeaveSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.LeaveSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.LeaveSeat(ctx.Context(), &vrpb.LeaveSeatArgs{RoomId: roomID, Uid: uid})
		return roomID, reply, err
	})
}

func (c *core) kickSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpKickSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.KickSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.KickSeat(ctx.Context(), &vrpb.KickSeatArgs{
			RoomId: roomID, OperatorUid: uid, Uid: req.GetUid(), SeatIndex: req.GetSeatIndex(),
		})
		return roomID, reply, err
	})
}

func (c *core) forceMuteSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpForceMuteSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.ForceMuteSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.ForceMuteSeat(ctx.Context(), &vrpb.ForceMuteSeatArgs{
			RoomId: roomID, OperatorUid: uid, SeatIndex: req.GetSeatIndex(), Muted: req.GetMuted(),
		})
		return roomID, reply, err
	})
}

func (c *core) lockSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpLockSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.LockSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.LockSeat(ctx.Context(), &vrpb.LockSeatArgs{
			RoomId: roomID, OperatorUid: uid, SeatIndex: req.GetSeatIndex(), Locked: req.GetLocked(),
		})
		return roomID, reply, err
	})
}

func (c *core) handUp(ctx node.Context) {
	c.seatOperation(ctx, contract.OpHandUp, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.HandUpReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.HandUp(ctx.Context(), &vrpb.HandUpArgs{RoomId: roomID, Uid: uid})
		return roomID, reply, err
	})
}

func (c *core) approveHandUp(ctx node.Context) {
	c.seatOperation(ctx, contract.OpApproveHandUp, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.ApproveHandUpReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.ApproveHandUp(ctx.Context(), &vrpb.ApproveHandUpArgs{
			RoomId: roomID, OperatorUid: uid, Uid: req.GetUid(),
			SeatIndex: req.GetSeatIndex(), Accept: req.GetAccept(),
		})
		return roomID, reply, err
	})
}

func (c *core) inviteToSeat(ctx node.Context) {
	c.seatOperation(ctx, contract.OpInviteToSeat, func(client *vrclient.Client, uid int64) (int64, *vrpb.SeatOperationReply, error) {
		req := &rtcwire.InviteToSeatReq{}
		if _, err := parseMessage(ctx, req); err != nil {
			return 0, nil, errs.ErrInternal
		}
		roomID, err := strconv.ParseInt(req.GetRoomId(), 10, 64)
		if err != nil || roomID <= 0 {
			return 0, nil, errs.ErrVoiceRoomNotExist
		}
		reply, err := client.InviteToSeat(ctx.Context(), &vrpb.InviteToSeatArgs{
			RoomId: roomID, OperatorUid: uid, Uid: req.GetUid(), SeatIndex: req.GetSeatIndex(),
		})
		return roomID, reply, err
	})
}
