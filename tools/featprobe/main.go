// Command featprobe verifies the ADR-0043 features end-to-end over the WS
// gate: burn after reading (1518/1910), screenshot report (1519), group
// security (1318/1319) and the voice-room chain (1808-1819, 1912).
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/network"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/token"
	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	groupwire "github.com/zciyuan/ZIM/internal/protocol/group"
	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	rtcwire "github.com/zciyuan/ZIM/internal/protocol/rtc"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

func main() {
	gate := flag.String("gate", "ws://127.0.0.1:3553/ws", "gate URL")
	secret := flag.String("secret", "dev-only-zim-secret-change-me", "JWT secret")
	flag.Parse()
	issuer, err := token.New(*secret, time.Hour, time.Hour)
	if err != nil {
		die("issuer: %v", err)
	}
	contract.InstallPacker()

	alice, af := connect(*gate, issuer, 10001, "feat-alice")
	bob, bf := connect(*gate, issuer, 10002, "feat-bob")
	defer func() { _ = alice.Close(); _ = bob.Close() }()

	// ---- 1. burn after reading ----
	clientID := fmt.Sprintf("burn-%d", time.Now().UnixNano())
	send(alice, 1, contract.OpSendSingle, marshal(&msgwire.SendSingleReq{
		ClientMsgId: clientID, RecvId: 10002, MsgType: 1, Content: `{"text":"burn me"}`, BurnTtl: 10,
	}))
	sendEnv := env(wait(af, contract.OpSendSingle, 15*time.Second))
	if sendEnv.Code != 0 {
		die("burn send code=%d", sendEnv.Code)
	}
	fmt.Println("PASS burn message sent (burn_ttl=10)")

	bobGot := wait(bf, contract.OpPushMsg, 15*time.Second)
	pushRes := &msgwire.MessageItem{}
	if err := proto.Unmarshal(env(bobGot).Payload, pushRes); err != nil {
		die("unmarshal push: %v", err)
	}
	if pushRes.GetBurnTtl() != 10 {
		die("push burn_ttl = %d, want 10", pushRes.GetBurnTtl())
	}
	serverMsgID := pushRes.GetServerMsgId()
	fmt.Println("PASS receiver push carries burn_ttl")

	send(bob, 2, contract.OpBurnReadAck, marshal(&msgwire.BurnReadAckReq{
		ConversationId: pushRes.GetConversationId(), ServerMsgId: serverMsgID, Seq: pushRes.GetSeq(),
	}))
	ackEnv := env(wait(bf, contract.OpBurnReadAck, 15*time.Second))
	if ackEnv.Code != 0 {
		die("burn ack code=%d", ackEnv.Code)
	}
	ackRes := &msgwire.BurnReadAckRes{}
	_ = proto.Unmarshal(ackEnv.Payload, ackRes)
	if ackRes.GetBurnExpireAt() <= ackRes.GetServerNowMs() {
		die("burn expire_at = %d, now = %d", ackRes.GetBurnExpireAt(), ackRes.GetServerNowMs())
	}
	// Both sides receive 1910.
	wait(af, contract.OpPushBurnTimerStart, 15*time.Second)
	wait(bf, contract.OpPushBurnTimerStart, 15*time.Second)
	fmt.Println("PASS burn read ack starts countdown; 1910 delivered to both sides")

	// ---- 2. screenshot report ----
	send(alice, 3, contract.OpScreenshotReport, marshal(&msgwire.ScreenshotReportReq{
		ConversationId: "g_5001", ConvType: 2, EventType: 1, DeviceInfo: "probe",
	}))
	if env := env(wait(af, contract.OpScreenshotReport, 15*time.Second)); env.Code != 0 {
		die("screenshot report code=%d", env.Code)
	}
	fmt.Println("PASS screenshot report accepted")

	// ---- 3. group security ----
	// Create a real group first; the fixed id 5001 does not exist.
	send(alice, 4, contract.OpCreateGroup, marshal(&groupwire.CreateGroupReq{
		Name: "probe group", GroupType: 2, MemberIds: []int64{10002},
	}))
	createGroupEnv := env(wait(af, contract.OpCreateGroup, 15*time.Second))
	if createGroupEnv.Code != 0 {
		die("create group code=%d", createGroupEnv.Code)
	}
	createGroupRes := &groupwire.CreateGroupRes{}
	_ = proto.Unmarshal(createGroupEnv.Payload, createGroupRes)
	groupID := createGroupRes.GetGroupId()
	if groupID == "" {
		die("create group res invalid: %+v", createGroupRes)
	}
	fmt.Println("PASS group created")

	send(alice, 5, contract.OpSetGroupSecurity, marshal(&groupwire.SetGroupSecurityReq{
		GroupId: groupID, MuteAll: true, ScheduledMuteEnable: true,
		ScheduledMuteStart: "23:00", ScheduledMuteEnd: "07:00", ScheduledTimezone: "Asia/Shanghai",
		ScreenshotPenalty: 1, AntiScreenshot: true,
	}))
	if env := env(wait(af, contract.OpSetGroupSecurity, 15*time.Second)); env.Code != 0 {
		die("set group security code=%d", env.Code)
	}
	fmt.Println("PASS group security settings applied")

	send(alice, 6, contract.OpSetMemberRestriction, marshal(&groupwire.SetMemberRestrictionReq{
		GroupId: groupID, UserId: 10002, MuteFlags: 3, IsWhitelist: false, MuteUntil: -1,
	}))
	if env := env(wait(af, contract.OpSetMemberRestriction, 15*time.Second)); env.Code != 0 {
		die("set member restriction code=%d", env.Code)
	}
	// The muted member cannot send to the group (130102/130006-family error).
	send(bob, 7, contract.OpSendGroup, marshal(&msgwire.SendGroupReq{
		ClientMsgId: fmt.Sprintf("muted-%d", time.Now().UnixNano()),
		GroupId:     parseGroupID(groupID), MsgType: 1, Content: `{"text":"muted"}`,
	}))
	mutedEnv := env(wait(bf, contract.OpSendGroup, 15*time.Second))
	if mutedEnv.Code != 130101 && mutedEnv.Code != 130102 && mutedEnv.Code != 130006 {
		die("muted send code=%d, want mute-family error", mutedEnv.Code)
	}
	fmt.Printf("PASS muted member rejected with code %d\n", mutedEnv.Code)

	// ---- 4. voice room chain ----
	send(alice, 7, contract.OpVoiceRoomCreate, marshal(&rtcwire.VoiceRoomCreateReq{
		RoomType: 2, Title: "probe room", SeatCount: 8, JoinSeatMode: 1,
	}))
	createEnv := env(wait(af, contract.OpVoiceRoomCreate, 15*time.Second))
	if createEnv.Code != 0 {
		die("voice room create code=%d", createEnv.Code)
	}
	createRes := &rtcwire.VoiceRoomCreateRes{}
	_ = proto.Unmarshal(createEnv.Payload, createRes)
	roomID := createRes.GetRoomId()
	if roomID == "" || createRes.GetToken() == "" {
		die("create room res invalid: %+v", createRes)
	}
	fmt.Println("PASS voice room created, owner token issued")

	// Bob joins as a listener and receives the 1912 seat update.
	send(bob, 8, contract.OpVoiceRoomJoin, marshal(&rtcwire.VoiceRoomJoinReq{RoomId: roomID}))
	joinEnv := env(wait(bf, contract.OpVoiceRoomJoin, 15*time.Second))
	if joinEnv.Code != 0 {
		die("voice room join code=%d", joinEnv.Code)
	}
	fmt.Println("PASS listener joined")
	// Bob takes seat 1 (the owner holds seat 0; one seat per member).
	send(bob, 9, contract.OpTakeSeat, marshal(&rtcwire.TakeSeatReq{RoomId: roomID, SeatIndex: 1}))
	if env := env(wait(bf, contract.OpTakeSeat, 15*time.Second)); env.Code != 0 {
		die("bob take seat code=%d", env.Code)
	}
	seatPush := &rtcwire.SeatUpdatePush{}
	_ = proto.Unmarshal(env(wait(af, contract.OpPushSeatUpdate, 15*time.Second)).Payload, seatPush)
	fmt.Println("PASS 1912 seat update delivered to owner")

	send(alice, 10, contract.OpForceMuteSeat, marshal(&rtcwire.ForceMuteSeatReq{RoomId: roomID, SeatIndex: 1, Muted: true}))
	if env := env(wait(af, contract.OpForceMuteSeat, 15*time.Second)); env.Code != 0 {
		die("force mute seat code=%d", env.Code)
	}
	seatPush = &rtcwire.SeatUpdatePush{}
	// The take-seat 1912 may still be queued; keep reading until the
	// force-mute snapshot arrives.
	deadline := time.Now().Add(15 * time.Second)
	for {
		_ = proto.Unmarshal(env(wait(bf, contract.OpPushSeatUpdate, 15*time.Second)).Payload, seatPush)
		done := false
		detail := ""
		for _, seat := range seatPush.GetSeats() {
			detail += fmt.Sprintf(" [idx=%d uid=%d status=%d]", seat.GetSeatIndex(), seat.GetUid(), seat.GetStatus())
			if seat.GetSeatIndex() == 1 && seat.GetStatus() == 3 {
				done = true
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			die("seat 1 must be FORCE_MUTED in 1912:%s", detail)
		}
	}
	fmt.Println("PASS seat force-mute propagated via 1912")

	send(bob, 11, contract.OpLeaveSeat, marshal(&rtcwire.LeaveSeatReq{RoomId: roomID}))
	if env := env(wait(bf, contract.OpLeaveSeat, 15*time.Second)); env.Code != 0 {
		die("leave seat code=%d", env.Code)
	}
	fmt.Println("ALL FEATURE PROBE CHECKS PASSED")
}

func parseGroupID(groupID string) int64 {
	id, err := strconv.ParseInt(groupID, 10, 64)
	if err != nil {
		die("group id: %v", err)
	}
	return id
}

func connect(gate string, issuer *token.Issuer, uid int64, deviceID string) (network.Conn, chan *contract.Frame) {
	pair, err := issuer.Issue(uid, 2, deviceID)
	if err != nil {
		die("issue: %v", err)
	}
	frames := make(chan *contract.Frame, 256)
	q := url.Values{"token": {pair.AccessToken}, "platform": {"2"}, "sdk": {"featprobe"}}
	client := ws.NewClient(ws.WithClientUrl(gate+"?"+q.Encode()), ws.WithClientHeartbeatInterval(0))
	client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		defer buf.Release()
		frame, decodeErr := contract.DecodeFrameBytes(buf.Bytes())
		if decodeErr == nil {
			frames <- frame
		}
	})
	conn, err := client.Dial()
	if err != nil {
		die("dial: %v", err)
	}
	send(conn, 1, contract.OpLogin, marshal(&authwire.LoginReq{
		Token: pair.AccessToken, Platform: 2, Sdk: "featprobe", DeviceId: deviceID,
	}))
	if env := env(wait(frames, contract.OpLogin, 15*time.Second)); env.Code != 0 {
		die("login %s code=%d", deviceID, env.Code)
	}
	return conn, frames
}

func send(conn network.Conn, seq uint32, opcode int32, payload []byte) {
	envBody, _ := contract.EncodeEnvelope(&contract.Envelope{Opcode: opcode, Payload: payload})
	frame, err := contract.EncodeFrame(contract.Frame{Version: contract.Version1, Opcode: opcode, Seq: seq, Body: envBody})
	if err != nil {
		die("frame: %v", err)
	}
	if err = conn.Push(buffer.NewBytes(frame, true)); err != nil {
		die("push: %v", err)
	}
}

func wait(frames chan *contract.Frame, opcode int32, timeout time.Duration) *contract.Frame {
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-frames:
			if frame.Opcode == opcode {
				return frame
			}
			fmt.Printf("  (skip opcode=%d)\n", frame.Opcode)
		case <-deadline:
			die("timeout %d", opcode)
		}
	}
}

func env(frame *contract.Frame) *contract.Envelope {
	envelope := &contract.Envelope{}
	if err := proto.Unmarshal(frame.Body, envelope); err != nil {
		die("envelope: %v", err)
	}
	return envelope
}

func marshal(m proto.Message) []byte {
	raw, err := proto.Marshal(m)
	if err != nil {
		die("marshal: %v", err)
	}
	return raw
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}
