package logic

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	rtcwire "github.com/zciyuan/ZIM/internal/protocol/rtc"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

func (c *core) callInvite(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, platform, slot, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.CallInviteReq{}
		if _, err = parseMessage(ctx, req); err != nil || req.GetTarget() == nil ||
			req.GetCallId() == "" || req.GetTarget().GetId() <= 0 ||
			(req.GetMedia() != 1 && req.GetMedia() != 2) {
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrInternal)
			return
		}
		target := req.GetTarget()
		targetType := target.GetType()
		if targetType < 1 || targetType > 3 {
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrRTCCallNotExist)
			return
		}
		if targetType == 1 && target.GetId() == uid {
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrRTCCallSelf)
			return
		}
		if targetType > 1 {
			if _, err = c.msg.GroupMembers(ctx.Context(), target.GetId(), uid); err != nil {
				c.respond(ctx, contract.OpCallInvite, nil, callMemberError(err))
				return
			}
		}
		if targetType == 1 && !c.anyOnline(ctx.Context(), target.GetId()) {
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrRTCPeerOffline)
			return
		}

		room := "zim-call-" + req.GetCallId()
		max := groupCallMax(req.GetMedia())
		if targetType == 3 {
			max = familyCallMax(req.GetMedia())
		}
		var call *rtcsvc.Call
		if targetType == 2 || targetType == 3 {
			if targetType == 2 {
				call, err = c.rtcCalls.OpenGroup(ctx.Context(), req.GetCallId(), uid,
					target.GetId(), req.GetMedia(), room, max)
			} else {
				call, err = c.rtcCalls.OpenFamily(ctx.Context(), req.GetCallId(), uid,
					target.GetId(), req.GetMedia(), room, max)
			}
			if err == nil {
				// Reserve the group first: a group that is already in a call must
				// not create an orphan SFU room.
				if _, err = c.rtcProvider.CreateRoom(ctx.Context(), rtcfw.RoomReq{
					Room: room, CallID: req.GetCallId(), GroupID: target.GetId(),
					OwnerID: uid, MaxParticipants: int(max),
				}); err != nil {
					_ = c.rtcCalls.End(ctx.Context(), call.ID)
				}
			}
		} else {
			if _, err = c.rtcProvider.CreateRoom(ctx.Context(), rtcfw.RoomReq{
				Room: room, CallID: req.GetCallId(), GroupID: target.GetId(),
				OwnerID: uid, MaxParticipants: int(max),
			}); err != nil {
				c.respond(ctx, contract.OpCallInvite, nil, errs.ErrServerBusy)
				return
			}
			call, err = c.rtcCalls.InviteTo(ctx.Context(), req.GetCallId(), uid,
				target.GetId(), targetType, req.GetMedia(), room)
		}
		if err != nil {
			if targetType == 2 {
				countGroupCall("start", "error")
			}
			if targetType > 1 && errors.Is(err, rtcsvc.ErrBusy) {
				c.respond(ctx, contract.OpCallInvite, nil, errs.ErrRTCGroupBusy)
				return
			}
			if targetType > 1 {
				c.respond(ctx, contract.OpCallInvite, nil, errs.ErrServerBusy)
				return
			}
			c.respond(ctx, contract.OpCallInvite, nil, rtcError(err))
			return
		}

		res := &rtcwire.CallInviteRes{CallId: call.ID, Room: call.Room, ExpireAt: call.ExpireAt,
			Media: call.Media, TargetType: call.TargetType, Version: call.Version, Seq: call.Seq}
		// Every caller needs a join token: for 1:1 the invite response is the
		// only frame that reaches the caller before the callee answers.
		token, tokenErr := c.rtcProvider.IssueToken(ctx.Context(), call.Room, uid, rtcfw.RolePublisher)
		if tokenErr != nil {
			_ = c.rtcCalls.End(ctx.Context(), call.ID)
			_ = c.rtcProvider.DeleteRoom(ctx.Context(), call.Room)
			if targetType == 2 {
				countGroupCall("start", "error")
			}
			c.respond(ctx, contract.OpCallInvite, nil, errs.ErrServerBusy)
			return
		}
		res.Token = token
		res.SfuUrl = c.rtcClientURL
		if targetType == 2 {
			res.Participants, _ = c.rtcCalls.Participants(ctx.Context(), call.ID)
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpCallInvite, payload, nil)

		if targetType == 2 {
			c.pushGroupCallState(ctx.Context(), call, res.Participants, uid)
			c.sendGroupCallCard(ctx.Context(), call, uid, "ongoing", 0, res.Participants)
			countGroupCall("start", "ok")
			c.traceCall(uid, platform, slot, call.ID, "ok",
				fmt.Sprintf("event=start,media=%d,group=%d", call.Media, call.ToUID))
			return
		}
		if targetType == 3 {
			if answeredUID, ok := c.tryAutoAnswer(ctx.Context(), call); ok {
				if refreshed, getErr := c.rtcCalls.Get(ctx.Context(), call.ID); getErr == nil {
					call = refreshed
				}
				c.pushFamilyAnswered(ctx.Context(), call, answeredUID)
				c.onFamilyAnswered(ctx.Context(), call)
			} else {
				c.pushRinging(ctx.Context(), call)
			}
			c.traceCall(uid, platform, slot, call.ID, "ok",
				fmt.Sprintf("event=start,media=%d,group=%d", call.Media, call.ToUID))
			return
		}
		c.pushRinging(ctx.Context(), call)
	})
}

func (c *core) callAnswer(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, platform, slot, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCallAnswer, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.CallAnswerReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpCallAnswer, nil, errs.ErrInternal)
			return
		}
		call, err := c.rtcCalls.Get(ctx.Context(), req.GetCallId())
		if err != nil {
			c.respond(ctx, contract.OpCallAnswer, nil, rtcError(err))
			return
		}
		if err = c.checkCallMember(ctx.Context(), call, uid); err != nil {
			c.respond(ctx, contract.OpCallAnswer, nil, err)
			return
		}
		wasRinging := call.State == rtcsvc.StateRinging

		// Mint the callee token before the state transition: a token failure
		// must not leave an "answered" call that the caller was never told about.
		var joinToken string
		if req.GetAccept() {
			joinToken, err = c.rtcProvider.IssueToken(ctx.Context(), call.Room, uid, rtcfw.RolePublisher)
			if err != nil {
				c.respond(ctx, contract.OpCallAnswer, nil, errs.ErrServerBusy)
				return
			}
		}

		var answered *rtcsvc.Call
		err = c.rtcActors.Do(ctx.Context(), req.GetCallId(), func() error {
			var answerErr error
			answered, answerErr = c.rtcCalls.Answer(ctx.Context(), req.GetCallId(), uid, req.GetAccept())
			return answerErr
		})
		if err != nil {
			c.respond(ctx, contract.OpCallAnswer, nil, rtcError(err))
			return
		}

		res := &rtcwire.CallAnswerRes{CallId: answered.ID, Room: answered.Room,
			Phase: answered.State, TargetType: answered.TargetType, Media: answered.Media,
			ExpireAt: answered.ExpireAt, Version: answered.Version, Seq: answered.Seq}
		if req.GetAccept() {
			res.Token = joinToken
			res.SfuUrl = c.rtcClientURL
			_ = c.rtcCalls.MarkLeg(ctx.Context(), answered.ID, multi.Key(uid, platform, slot), "joined")
			if answered.TargetType <= 1 {
				c.cancelOtherDevices(ctx.Context(), answered, uid, multi.Key(uid, platform, slot))
			}
		} else if answered.TargetType == 1 || wasRinging {
			c.recordCall(ctx.Context(), answered.FromUID, answered, 0, "rejected")
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpCallAnswer, payload, nil)

		if !req.GetAccept() {
			if answered.TargetType == 1 || wasRinging {
				pushPayload, _ := proto.Marshal(&rtcwire.CallAnswerRes{
					CallId: answered.ID, Phase: 3, Media: answered.Media,
				})
				c.pushCallFrame(ctx.Context(), answered.FromUID, contract.OpCallAnswer, pushPayload)
			}
			return
		}
		c.traceCall(uid, platform, slot, answered.ID, "ok",
			fmt.Sprintf("event=join,media=%d,group=%d", answered.Media, answered.ToUID))
		if answered.TargetType == 2 {
			countGroupCall("join", "ok")
			// Group rooms are not ringing: joining is a silent state change.
			return
		}
		if answered.TargetType == 3 {
			if wasRinging {
				c.pushFamilyAnswered(ctx.Context(), answered, uid)
				c.onFamilyAnswered(ctx.Context(), answered)
			} else if participants, err := c.rtcCalls.Participants(ctx.Context(), answered.ID); err == nil {
				c.pushGroupCallState(ctx.Context(), answered, participants, uid)
			}
			// Late joiners update the room state; they must not re-trigger the
			// "answered" notification for everyone else.
			return
		}
		// The caller already holds a token from 1801; include a fresh one so a
		// client that only consumes the answer frame can still join.
		callerToken, _ := c.rtcProvider.IssueToken(ctx.Context(), answered.Room, answered.FromUID, rtcfw.RolePublisher)
		pushPayload, _ := proto.Marshal(&rtcwire.CallAnswerRes{
			CallId: answered.ID, Phase: 2, FromUid: uid, Media: answered.Media, Room: answered.Room,
			TargetType: answered.TargetType, Token: callerToken, SfuUrl: c.rtcClientURL,
		})
		c.pushCallFrame(ctx.Context(), answered.FromUID, contract.OpCallAnswer, pushPayload)
	})
}

func (c *core) callHangup(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, platform, slot, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCallHangup, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.CallHangupReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpCallHangup, nil, errs.ErrInternal)
			return
		}

		var call *rtcsvc.Call
		var participants []int64
		ended := false
		wasRinging := false
		err = c.rtcActors.Do(ctx.Context(), req.GetCallId(), func() error {
			var hangupErr error
			call, hangupErr = c.rtcCalls.Get(ctx.Context(), req.GetCallId())
			if hangupErr != nil {
				return hangupErr
			}
			wasRinging = call.State == rtcsvc.StateRinging
			if call.TargetType > 1 {
				call, participants, ended, hangupErr = c.rtcCalls.LeaveGroup(ctx.Context(), req.GetCallId(), uid)
				return hangupErr
			}
			return c.rtcCalls.Hangup(ctx.Context(), req.GetCallId(), uid)
		})
		if err != nil {
			if call != nil && call.TargetType == 2 {
				countGroupCall("leave", "error")
			}
			c.respond(ctx, contract.OpCallHangup, nil, rtcError(err))
			return
		}

		if call.TargetType > 1 {
			duration := callDuration(call)
			// Remove the SFU participant before responding: a fast rejoin
			// after the client receives the response must not be kicked by
			// this leave's cleanup.
			if !ended {
				c.removeParticipant(ctx.Context(), call, uid)
			}
			payload, _ := proto.Marshal(&rtcwire.CallHangupRes{CallId: call.ID, Ended: ended})
			c.respond(ctx, contract.OpCallHangup, payload, nil)
			if ended {
				if call.TargetType == 3 && wasRinging {
					// Nobody answered; this is a missed family call, not a room
					// that ever had an ongoing card.
					c.recordCall(ctx.Context(), call.FromUID, call, 0, "missed")
					for _, member := range c.callMembers(ctx.Context(), call) {
						if member != uid {
							c.pushCallFrame(ctx.Context(), member, contract.OpCallHangup, payload)
						}
					}
					c.traceCall(uid, platform, slot, call.ID, "ok",
						fmt.Sprintf("event=end,media=%d,group=%d,duration=%d", call.Media, call.ToUID, duration))
					return
				}
				c.pushGroupCallState(ctx.Context(), call, participants, uid)
				c.sendGroupCallCard(ctx.Context(), call, uid, "ended", duration, participants)
				if call.TargetType == 3 {
					for _, member := range c.callMembers(ctx.Context(), call) {
						if member != uid {
							c.pushCallFrame(ctx.Context(), member, contract.OpCallHangup, payload)
						}
					}
				}
				if call.TargetType == 2 {
					countGroupCall("end", "ok")
				}
				c.cleanupRoom(ctx.Context(), call)
				c.traceCall(uid, platform, slot, call.ID, "ok",
					fmt.Sprintf("event=end,media=%d,group=%d,duration=%d", call.Media, call.ToUID, duration))
			} else {
				if call.TargetType == 2 {
					countGroupCall("leave", "ok")
				} else if current, listErr := c.rtcCalls.Participants(ctx.Context(), call.ID); listErr == nil {
					c.pushGroupCallState(ctx.Context(), call, current, uid)
				}
				c.traceCall(uid, platform, slot, call.ID, "ok",
					fmt.Sprintf("event=leave,media=%d,group=%d", call.Media, call.ToUID))
			}
			return
		}

		duration := int64(0)
		switch call.State {
		case rtcsvc.StateAnswered:
			duration = callDuration(call)
			c.recordCall(ctx.Context(), uid, call, duration, "ended")
		case rtcsvc.StateRinging:
			c.recordCall(ctx.Context(), call.FromUID, call, 0, "missed")
		}
		payload, _ := proto.Marshal(&rtcwire.CallHangupRes{CallId: req.GetCallId()})
		c.respond(ctx, contract.OpCallHangup, payload, nil)
		if call.TargetType > 1 {
			for _, member := range c.callMembers(ctx.Context(), call) {
				if member != uid {
					c.pushCallFrame(ctx.Context(), member, contract.OpCallHangup, payload)
				}
			}
			return
		}
		peer := call.ToUID
		if peer == uid {
			peer = call.FromUID
		}
		c.pushCallFrame(ctx.Context(), peer, contract.OpCallHangup, payload)
		c.cleanupRoom(ctx.Context(), call)
	})
}

func (c *core) callCancel(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, _, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCallCancel, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.CallCancelReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpCallCancel, nil, errs.ErrInternal)
			return
		}
		var call *rtcsvc.Call
		err = c.rtcActors.Do(ctx.Context(), req.GetCallId(), func() error {
			var getErr error
			call, getErr = c.rtcCalls.Get(ctx.Context(), req.GetCallId())
			if getErr != nil {
				return getErr
			}
			return c.rtcCalls.Cancel(ctx.Context(), req.GetCallId(), uid)
		})
		if err != nil {
			c.respond(ctx, contract.OpCallCancel, nil, rtcError(err))
			return
		}
		c.recordCall(ctx.Context(), call.FromUID, call, 0, "canceled")
		payload, _ := proto.Marshal(&rtcwire.CallCancelRes{CallId: req.GetCallId()})
		c.respond(ctx, contract.OpCallCancel, payload, nil)
		if call.TargetType > 1 {
			for _, member := range c.callMembers(ctx.Context(), call) {
				if member == call.FromUID {
					continue
				}
				c.pushCallFrame(ctx.Context(), member, contract.OpCallCancel, payload)
			}
			return
		}
		c.pushCallFrame(ctx.Context(), call.ToUID, contract.OpCallCancel, payload)
		c.cleanupRoom(ctx.Context(), call)
	})
}

func (c *core) roomToken(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, platform, slot, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpRoomToken, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.RoomTokenReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpRoomToken, nil, errs.ErrInternal)
			return
		}
		if req.GetCallId() == "" && req.GetGroupId() <= 0 {
			c.respond(ctx, contract.OpRoomToken, nil, errs.ErrRTCCallNotExist)
			return
		}

		var call *rtcsvc.Call
		if req.GetCallId() != "" {
			call, err = c.rtcCalls.Get(ctx.Context(), req.GetCallId())
			if err != nil {
				c.respond(ctx, contract.OpRoomToken, nil, rtcError(err))
				return
			}
		} else {
			call, err = c.rtcCalls.ActiveGroupCall(ctx.Context(), req.GetGroupId())
			if err != nil {
				// The group card is stale: tell the client the call is over.
				c.respond(ctx, contract.OpRoomToken, nil, errs.ErrRTCCallEnded)
				return
			}
		}
		if err = c.checkCallMember(ctx.Context(), call, uid); err != nil {
			c.respond(ctx, contract.OpRoomToken, nil, err)
			return
		}
		wasRinging := call.State == rtcsvc.StateRinging

		joined := call
		if call.TargetType > 1 {
			err = c.rtcActors.Do(ctx.Context(), call.ID, func() error {
				var joinErr error
				joined, joinErr = c.rtcCalls.JoinGroup(ctx.Context(), call.ID, uid)
				return joinErr
			})
			if err != nil {
				if call.TargetType == 2 {
					countGroupCall("join", "error")
				}
				c.respond(ctx, contract.OpRoomToken, nil, rtcError(err))
				return
			}
		} else if call.State != rtcsvc.StateAnswered {
			c.respond(ctx, contract.OpRoomToken, nil, errs.ErrRTCCallEnded)
			return
		}

		token, err := c.rtcProvider.IssueToken(ctx.Context(), joined.Room, uid, rtcfw.RolePublisher)
		if err != nil {
			c.respond(ctx, contract.OpRoomToken, nil, errs.ErrServerBusy)
			return
		}
		res := &rtcwire.RoomTokenRes{
			CallId: joined.ID, Room: joined.Room, Token: token, SfuUrl: c.rtcClientURL,
			ExpireAt: joined.ExpireAt, Media: joined.Media, TargetType: joined.TargetType,
		}
		if joined.TargetType > 1 {
			res.Participants, _ = c.rtcCalls.Participants(ctx.Context(), joined.ID)
		} else {
			res.Participants = []int64{joined.FromUID, joined.ToUID}
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpRoomToken, payload, nil)
		if joined.TargetType == 2 {
			countGroupCall("join", "ok")
		}
		if joined.TargetType == 3 {
			if wasRinging {
				c.pushFamilyAnswered(ctx.Context(), joined, uid)
				c.onFamilyAnswered(ctx.Context(), joined)
			} else {
				c.pushGroupCallState(ctx.Context(), joined, res.Participants, uid)
			}
		}
		c.traceCall(uid, platform, slot, joined.ID, "ok",
			fmt.Sprintf("event=join,media=%d,group=%d", joined.Media, joined.ToUID))
	})
}

func (c *core) callState(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, _, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpCallState, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.CallStateReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpCallState, nil, errs.ErrInternal)
			return
		}
		if req.GetCallId() == "" && req.GetGroupId() <= 0 {
			c.respond(ctx, contract.OpCallState, nil, errs.ErrRTCCallNotExist)
			return
		}

		if req.GetCallId() != "" {
			call, getErr := c.rtcCalls.Get(ctx.Context(), req.GetCallId())
			if getErr != nil {
				c.respond(ctx, contract.OpCallState, nil, rtcError(getErr))
				return
			}
			if getErr = c.checkCallMember(ctx.Context(), call, uid); getErr != nil {
				c.respond(ctx, contract.OpCallState, nil, getErr)
				return
			}
			participants := []int64{call.FromUID, call.ToUID}
			if call.TargetType > 1 {
				participants, _ = c.rtcCalls.Participants(ctx.Context(), call.ID)
			}
			payload, _ := proto.Marshal(callStateRes(call, participants))
			c.respond(ctx, contract.OpCallState, payload, nil)
			return
		}

		// Query by group id: the group screen calls this when it opens.
		if _, getErr := c.msg.GroupMembers(ctx.Context(), req.GetGroupId(), uid); getErr != nil {
			c.respond(ctx, contract.OpCallState, nil, callMemberError(getErr))
			return
		}
		call, getErr := c.rtcCalls.ActiveGroupCall(ctx.Context(), req.GetGroupId())
		if getErr != nil {
			if errors.Is(getErr, rtcsvc.ErrCallNotFound) {
				payload, _ := proto.Marshal(&rtcwire.CallStateRes{
					GroupId: req.GetGroupId(), State: 1, Active: false,
				})
				c.respond(ctx, contract.OpCallState, payload, nil)
				return
			}
			c.respond(ctx, contract.OpCallState, nil, rtcError(getErr))
			return
		}
		participants, _ := c.rtcCalls.Participants(ctx.Context(), call.ID)
		res := callStateRes(call, participants)
		if req.GetSinceSeq() > 0 {
			if events, listErr := c.rtcCalls.Events(ctx.Context(), call.ID, req.GetSinceSeq()); listErr == nil {
				for _, event := range events {
					res.Events = append(res.Events, &rtcwire.CallEvent{
						Seq: event.Seq, State: event.State, At: event.At,
					})
				}
			}
		}
		payload, _ := proto.Marshal(res)
		c.respond(ctx, contract.OpCallState, payload, nil)
	})
}

func (c *core) participantState(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, _, err := multi.ParseSlot(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpParticipantState, nil, errs.ErrUnauthorized)
			return
		}
		req := &rtcwire.ParticipantStateReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpParticipantState, nil, errs.ErrInternal)
			return
		}
		call, err := c.rtcCalls.Get(ctx.Context(), req.GetCallId())
		if err != nil {
			c.respond(ctx, contract.OpParticipantState, nil, rtcError(err))
			return
		}
		if err = c.checkCallMember(ctx.Context(), call, uid); err != nil {
			c.respond(ctx, contract.OpParticipantState, nil, err)
			return
		}
		// Participant reports double as a liveness heartbeat for the room TTL.
		if err = c.rtcCalls.Touch(ctx.Context(), call.ID); err != nil {
			log.Warnf("rtc touch failed: call_id=%s uid=%d err=%v", call.ID, uid, err)
		}
		if req.GetAckSeq() > 0 {
			if ackErr := c.rtcCalls.Ack(ctx.Context(), call.ID, uid, req.GetAckSeq()); ackErr != nil {
				log.Warnf("rtc ack failed: call_id=%s uid=%d seq=%d err=%v",
					call.ID, uid, req.GetAckSeq(), ackErr)
			}
		}
		payload, _ := proto.Marshal(&rtcwire.ParticipantStateRes{CallId: req.GetCallId()})
		c.respond(ctx, contract.OpParticipantState, payload, nil)
	})
}

// checkCallMember rejects call queries from users that are not in the call.
func (c *core) checkCallMember(ctx context.Context, call *rtcsvc.Call, uid int64) error {
	if call == nil {
		return errs.ErrRTCCallNotExist
	}
	if call.TargetType <= 1 {
		if uid != call.FromUID && uid != call.ToUID {
			return errs.ErrRTCCallNotExist
		}
		return nil
	}
	if _, err := c.msg.GroupMembers(ctx, call.ToUID, uid); err != nil {
		return callMemberError(err)
	}
	return nil
}

// callMemberError keeps membership failures as 130002 and everything else as
// an internal error.
func callMemberError(err error) error {
	if errors.Is(err, errs.ErrNotGroupMember) {
		return errs.ErrNotGroupMember
	}
	return errs.ErrInternal.WithCause(err)
}

func (c *core) recordCall(ctx context.Context, uid int64, call *rtcsvc.Call, duration int64, result string) {
	participants := []int64{call.FromUID, call.ToUID}
	if call.TargetType > 1 {
		// ToUID is a group id; never put it in the participant list.
		participants = []int64{call.FromUID}
		if list, err := c.rtcCalls.Participants(ctx, call.ID); err == nil && len(list) > 0 {
			participants = list
		}
	}
	msgType := contract.MsgTypeVoiceCall
	if call.Media == 2 {
		msgType = contract.MsgTypeVideoCall
	}
	if call.TargetType > 1 {
		msgType = contract.MsgTypeGroupVoice
	}
	content := fmt.Sprintf(`{"call_id":%q,"duration":%d,"result":%q,"participants":[%s],"target_type":%d}`,
		call.ID, duration, result, joinUIDs(participants), call.TargetType)
	req := &message.SendRequest{
		ClientMsgID: "call-" + call.ID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		MsgType:     msgType, Content: content,
	}
	var err error
	if call.TargetType > 1 {
		req.GroupID = call.ToUID
		if c.pipeline != nil {
			_, err = c.pipeline.SendGroup(ctx, uid, req)
		} else {
			_, err = c.msg.SendGroup(ctx, uid, req)
		}
	} else {
		peer := call.ToUID
		if peer == uid {
			peer = call.FromUID
		}
		req.ReceiverID = peer
		if c.pipeline != nil {
			_, err = c.pipeline.SendSingle(ctx, uid, req)
		} else {
			_, err = c.msg.SendSingle(ctx, uid, req)
		}
	}
	if err != nil {
		log.Warnf("record call message failed: call_id=%s target_type=%d media=%d err=%v",
			call.ID, call.TargetType, call.Media, err)
	}
}

// pushGroupCallState broadcasts the 1805 state frame to every online group
// member. It is the real-time "this group is in a call" signal; the persisted
// msg_type=22 card is the offline/history source of truth.
func (c *core) pushGroupCallState(ctx context.Context, call *rtcsvc.Call, participants []int64, memberUID int64) {
	members, err := c.msg.GroupMembers(ctx, call.ToUID, memberUID)
	if err != nil {
		log.Warnf("group call push members failed: call_id=%s group=%d err=%v", call.ID, call.ToUID, err)
		metrics.Default.Inc("zim_rtc_group_state_push_total", map[string]string{"result": "error"})
		return
	}
	payload, err := proto.Marshal(callStateRes(call, participants))
	if err != nil {
		return
	}
	keys := c.deviceKeysBatch(ctx, members)
	if len(keys) == 0 {
		return
	}
	const batch = 500
	for start := 0; start < len(keys); start += batch {
		end := start + batch
		if end > len(keys) {
			end = len(keys)
		}
		c.pushDeviceKeys(ctx, keys[start:end], contract.OpCallState, payload, false)
	}
	metrics.Default.Inc("zim_rtc_group_state_push_total", map[string]string{"result": "ok"})
}

// sendGroupCallCard writes the persistent "call in progress/ended" card.
//
// It is a normal msg_type=22 group message, so online members get the usual
// 1901 push and offline members see it when they sync history. The card is the
// history/offline source of truth; 1805 remains the authoritative live state.
func (c *core) sendGroupCallCard(ctx context.Context, call *rtcsvc.Call, sender int64,
	status string, duration int64, participants []int64) {
	if len(participants) == 0 {
		if list, err := c.rtcCalls.Participants(ctx, call.ID); err == nil {
			participants = list
		} else {
			log.Warnf("group call participants failed: call_id=%s err=%v", call.ID, err)
		}
	}
	req := &message.SendRequest{
		ClientMsgID: "group-call-" + status + "-" + call.ID,
		GroupID:     call.ToUID,
		MsgType:     contract.MsgTypeGroupVoice,
		Content:     groupCallContent(call, status, duration, participants),
	}
	var err error
	if c.pipeline != nil {
		// The Kafka path also publishes the 1901 push; writing through the
		// service directly would only persist the card.
		_, err = c.pipeline.SendGroup(ctx, sender, req)
	} else {
		_, err = c.msg.SendGroup(ctx, sender, req)
	}
	if err != nil {
		log.Warnf("group call card failed: call_id=%s status=%s err=%v", call.ID, status, err)
		metrics.Default.Inc("zim_rtc_group_card_total", map[string]string{"status": status, "result": "error"})
		return
	}
	metrics.Default.Inc("zim_rtc_group_card_total", map[string]string{"status": status, "result": "ok"})
}

// groupCallContent is the msg_type=22 payload understood by the client.
func groupCallContent(call *rtcsvc.Call, status string, duration int64, participants []int64) string {
	result := ""
	if status == "ended" {
		result = "ended"
	}
	return fmt.Sprintf(`{"call_id":%q,"room":%q,"media":%d,"status":%q,"result":%q,"target_type":%d,"initiator":%d,"started_at":%d,"duration":%d,"participants":[%s]}`,
		call.ID, call.Room, call.Media, status, result, call.TargetType, call.FromUID,
		call.CreatedAt, duration, joinUIDs(participants))
}

// callStateRes builds the 1805 payload shared by queries and pushes.
func callStateRes(call *rtcsvc.Call, participants []int64) *rtcwire.CallStateRes {
	res := &rtcwire.CallStateRes{
		CallId: call.ID, State: call.State, Active: call.State == rtcsvc.StateRinging ||
			call.State == rtcsvc.StateAnswered,
		Media: call.Media, Room: call.Room, TargetType: call.TargetType,
		Participants: participants, StartedAt: call.CreatedAt, ExpireAt: call.ExpireAt,
		Version: call.Version, Seq: call.Seq,
	}
	if call.TargetType > 1 {
		res.GroupId = call.ToUID
	}
	return res
}

// callDuration returns whole seconds since the call started.
func callDuration(call *rtcsvc.Call) int64 {
	duration := (time.Now().UnixMilli() - call.CreatedAt) / 1000
	if duration < 0 {
		return 0
	}
	return duration
}

// groupCallMax is the room cap: 50 voice (R3), 9 video (DL-086).
func groupCallMax(media int32) int32 {
	if media == 2 {
		return rtcsvc.MaxGroupVideoParticipants
	}
	return rtcsvc.MaxGroupVoiceParticipants
}

// familyCallMax is the family room cap: 20 members, 9 video tiles.
func familyCallMax(media int32) int32 {
	if media == 2 {
		return rtcsvc.MaxGroupVideoParticipants
	}
	return rtcsvc.MaxFamilyParticipants
}

// onFamilyAnswered announces a family call that just became answered.
//
// Family calls still ring and auto-answer (ADR-0033); once someone is in the
// room they also get the ordinary group-card + 1805 state so members who
// missed the ring can join later (ADR-0039).
func (c *core) onFamilyAnswered(ctx context.Context, call *rtcsvc.Call) {
	if call == nil || call.State != rtcsvc.StateAnswered {
		return
	}
	participants, err := c.rtcCalls.Participants(ctx, call.ID)
	if err != nil {
		log.Warnf("family call participants failed: call_id=%s err=%v", call.ID, err)
	}
	c.pushGroupCallState(ctx, call, participants, call.FromUID)
	c.sendGroupCallCard(ctx, call, call.FromUID, "ongoing", 0, participants)
}

// pushFamilyAnswered tells the caller and the other family members that the
// call has been answered. Family calls still ring, so this closes the incoming
// call page on every other device/member.
func (c *core) pushFamilyAnswered(ctx context.Context, call *rtcsvc.Call, answeredUID int64) {
	payload, _ := proto.Marshal(&rtcwire.CallAnswerRes{
		CallId: call.ID, Phase: 2, FromUid: answeredUID, Media: call.Media,
		Room: call.Room, TargetType: 3,
	})
	c.pushCallFrame(ctx, call.FromUID, contract.OpCallAnswer, payload)
	for _, member := range c.callMembers(ctx, call) {
		if member == answeredUID || member == call.FromUID {
			continue
		}
		c.pushCallFrame(ctx, member, contract.OpCallAnswer, payload)
	}
}

// countGroupCall increments the group-call lifecycle counter.
func countGroupCall(action, result string) {
	metrics.Default.Inc("zim_rtc_group_call_total", map[string]string{"action": action, "result": result})
}

// traceCall writes one [trace] line for a group-call state change.
func (c *core) traceCall(uid int64, platform, slot int32, callID, result, detail string) {
	device := multi.Key(uid, platform, slot)
	line := "[trace] module=nodemsg action=call uid=%d device=%d platform=%d biz=%s req=- result=%s detail=%s"
	if result == "ok" {
		log.Infof(line, uid, device, platform, callID, result, detail)
		return
	}
	log.Warnf(line, uid, device, platform, callID, result, detail)
}

// joinUIDs renders participant ids for JSON content without encoding/json.
func joinUIDs(uids []int64) string {
	values := make([]string, 0, len(uids))
	for _, uid := range uids {
		if uid > 0 {
			values = append(values, strconv.FormatInt(uid, 10))
		}
	}
	return strings.Join(values, ",")
}

// removeParticipant evicts a leaving member from the SFU room.
func (c *core) removeParticipant(ctx context.Context, call *rtcsvc.Call, uid int64) {
	removeCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := c.rtcProvider.RemoveParticipant(removeCtx, call.Room, uid)
	cancel()
	if err != nil {
		log.Warnf("rtc remove participant failed: call_id=%s uid=%d err=%v", call.ID, uid, err)
	}
}

// cleanupRoom evicts the remaining participants and deletes the SFU room on a
// terminal call. Cleanup is idempotent: a missing room is not an error.
func (c *core) cleanupRoom(ctx context.Context, call *rtcsvc.Call) {
	if call == nil || call.Room == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	uids := []int64{call.FromUID}
	if call.ToUID > 0 && call.TargetType <= 1 {
		uids = append(uids, call.ToUID)
	}
	if call.TargetType > 1 {
		if participants, err := c.rtcCalls.Participants(cleanupCtx, call.ID); err == nil && len(participants) > 0 {
			uids = participants
		}
	}
	for _, uid := range uids {
		if uid > 0 {
			_ = c.rtcProvider.RemoveParticipant(cleanupCtx, call.Room, uid)
		}
	}
	if err := c.rtcProvider.DeleteRoom(cleanupCtx, call.Room); err != nil {
		log.Warnf("rtc delete room failed: call_id=%s room=%s err=%v", call.ID, call.Room, err)
	}
}

func rtcError(err error) error {
	if _, ok := err.(*errs.Error); ok {
		return err
	}
	switch {
	case errors.Is(err, rtcsvc.ErrBusy):
		return errs.ErrRTCPeerBusy
	case errors.Is(err, rtcsvc.ErrRoomFull):
		return errs.ErrRTCRoomFull
	case errors.Is(err, rtcsvc.ErrCallNotFound):
		return errs.ErrRTCCallNotExist
	case errors.Is(err, rtcsvc.ErrInvalidState):
		return errs.ErrRTCCallEnded
	case errors.Is(err, rtcsvc.ErrForbidden), errors.Is(err, rtcsvc.ErrInvalidCall):
		return errs.ErrRTCCallNotExist
	default:
		return errs.ErrInternal.WithCause(err)
	}
}

// deviceKeysBatch resolves online push targets for a batch of users.
func (c *core) deviceKeysBatch(ctx context.Context, uids []int64) []int64 {
	if c.deviceIndex != nil {
		if keys, err := c.deviceIndex.Keys(ctx, uids); err == nil && len(keys) > 0 {
			return keys
		}
	}
	if c.devices != nil {
		if keys, err := c.devices.Keys(ctx, uids); err == nil && len(keys) > 0 {
			return keys
		}
	}
	keys := make([]int64, 0, len(uids))
	for _, uid := range uids {
		keys = append(keys, c.deviceKeys(ctx, uid)...)
	}
	return keys
}

// deviceKeys resolves the online push targets of one user.
func (c *core) deviceKeys(ctx context.Context, uid int64) []int64 {
	// The Redis online set is the authoritative push target: it only contains
	// connections that are bound right now. The device table accumulates one row
	// per browser profile, so using it first floods pushes with dead keys.
	if c.deviceIndex != nil {
		if keys, err := c.deviceIndex.Keys(ctx, []int64{uid}); err == nil {
			return keys
		}
	}
	if c.devices != nil {
		if keys, err := c.devices.Keys(ctx, []int64{uid}); err == nil && len(keys) > 0 {
			return keys
		}
	}
	keys := make([]int64, 0, multi.PlatformPad)
	for platform := int32(1); platform <= multi.PlatformPad; platform++ {
		keys = append(keys, multi.Key(uid, platform, 1))
	}
	return keys
}

func (c *core) cancelOtherDevices(ctx context.Context, call *rtcsvc.Call, answerUID int64, answerKey int64) {
	payload, _ := proto.Marshal(&rtcwire.CallAnswerRes{CallId: call.ID, Phase: 4})
	keys := make([]int64, 0, 4)
	for _, key := range c.deviceKeys(ctx, answerUID) {
		if key != answerKey {
			keys = append(keys, key)
		}
	}
	c.pushDeviceKeys(ctx, keys, contract.OpCallAnswer, payload, false)
}

func (c *core) pushRinging(ctx context.Context, call *rtcsvc.Call) {
	payload, _ := proto.Marshal(&rtcwire.CallAnswerRes{
		CallId: call.ID, Phase: 1, FromUid: call.FromUID, Media: call.Media, Room: call.Room,
		TargetType: call.TargetType,
	})
	members := c.callMembers(ctx, call)
	log.Infof("call ringing: call_id=%s target_type=%d members=%v", call.ID, call.TargetType, members)
	for _, uid := range members {
		if uid == call.FromUID {
			continue
		}
		keys := c.deviceKeys(ctx, uid)
		for _, key := range keys {
			_ = c.rtcCalls.MarkLeg(ctx, call.ID, key, "ringing")
		}
		c.pushDeviceKeys(ctx, keys, contract.OpCallAnswer, payload, false)
	}
}

func (c *core) anyOnline(ctx context.Context, uid int64) bool {
	for _, key := range c.deviceKeys(ctx, uid) {
		if gid, err := c.proxy.LocateGate(ctx, key); err == nil && gid != "" {
			return true
		}
	}
	return false
}

func (c *core) tryAutoAnswer(ctx context.Context, call *rtcsvc.Call) (int64, bool) {
	if c.family == nil || call.TargetType != 3 {
		return 0, false
	}
	members, err := c.family.EligibleMembers(ctx, call.ToUID)
	if err != nil {
		return 0, false
	}
	for _, member := range members {
		if !c.anyOnline(ctx, member.UID) {
			continue
		}
		allowed, allowErr := c.family.AutoAnswerAllowed(ctx, call.ToUID, member.UID)
		if allowErr != nil || !allowed {
			continue
		}
		if _, answerErr := c.rtcCalls.Answer(ctx, call.ID, member.UID, true); answerErr != nil {
			continue
		}
		_ = c.family.AuditAutoAnswer(ctx, call.ToUID, call.FromUID, member.UID, call.ID)
		token, _ := c.rtcProvider.IssueToken(ctx, call.Room, member.UID, rtcfw.RolePublisher)
		c.pushAutoAnswered(ctx, call, member.UID, token)
		return member.UID, true
	}
	return 0, false
}

func (c *core) pushAutoAnswered(ctx context.Context, call *rtcsvc.Call, answeredUID int64, token string) {
	payload, _ := proto.Marshal(&rtcwire.CallAnswerRes{
		CallId: call.ID, Phase: 2, FromUid: answeredUID, Media: call.Media,
		Room: call.Room, Token: token, SfuUrl: c.rtcClientURL, TargetType: call.TargetType,
	})
	for _, uid := range []int64{call.FromUID, answeredUID} {
		c.pushDeviceKeys(ctx, c.deviceKeys(ctx, uid), contract.OpCallAnswer, payload, false)
	}
}

// pushCallFrame pushes one RTC frame to every platform of uid.
//
// Signaling that ends or advances a call must reach the peer even when it is
// online on several devices; the client treats the frame as if it had been the
// response to its own request.
func (c *core) pushCallFrame(ctx context.Context, uid int64, opcode int32, payload []byte) {
	if uid <= 0 {
		return
	}
	c.pushDeviceKeys(ctx, c.deviceKeys(ctx, uid), opcode, payload, false)
}

// callMembers returns every uid that should hear about a call.
func (c *core) callMembers(ctx context.Context, call *rtcsvc.Call) []int64 {
	if call.TargetType <= 1 {
		return []int64{call.ToUID}
	}
	members, err := c.msg.GroupMembers(ctx, call.ToUID, call.FromUID)
	if err != nil {
		log.Warnf("call members failed: call_id=%s group=%d err=%v", call.ID, call.ToUID, err)
		return nil
	}
	return members
}

// leaveCallOnDisconnect treats a lost device as leaving its group/family call.
// The last participant ends the room and the SFU room is deleted, so a group
// call cannot stay "ongoing" with zero participants after a network drop.
func (c *core) leaveCallOnDisconnect(ctx context.Context, uid int64) {
	call, err := c.rtcCalls.ActiveCall(ctx, uid)
	if err != nil || call.TargetType <= 1 {
		return
	}
	updated, participants, ended, err := c.rtcCalls.LeaveGroup(ctx, call.ID, uid)
	if err != nil {
		return
	}
	if ended {
		c.pushGroupCallState(ctx, updated, participants, uid)
		c.cleanupRoom(ctx, updated)
		return
	}
	c.removeParticipant(ctx, updated, uid)
	c.pushGroupCallState(ctx, updated, participants, uid)
}
