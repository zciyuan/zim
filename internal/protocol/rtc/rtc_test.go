package rtcwire

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/zimfw/contract"
)

func TestOpcodesAreFrozen(t *testing.T) {
	cases := map[int32]string{
		contract.OpCallInvite:       "CallInvite",
		contract.OpCallAnswer:       "CallAnswer",
		contract.OpCallHangup:       "CallHangup",
		contract.OpCallCancel:       "CallCancel",
		contract.OpCallState:        "CallState",
		contract.OpRoomToken:        "RoomToken",
		contract.OpParticipantState: "ParticipantState",
	}
	for opcode, name := range cases {
		segment, ok := contract.SegmentOf(opcode)
		if !ok || segment.Domain != "rtc" {
			t.Fatalf("%s opcode = %d, domain = %q", name, opcode, segment.Domain)
		}
	}
}

func TestCallInviteRoundTrip(t *testing.T) {
	req := &CallInviteReq{CallId: "call_1", Target: &CallTarget{Type: 3, Id: 10001}, Media: 2}
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &CallInviteReq{}
	if err = proto.Unmarshal(raw, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetCallId() != "call_1" || decoded.GetTarget().GetType() != 3 || decoded.GetMedia() != 2 {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestGroupCallRoundTrip(t *testing.T) {
	invite := &CallInviteRes{CallId: "call_1", Room: "room_1", ExpireAt: 100,
		Token: "token", SfuUrl: "wss://sfu", Media: 1, TargetType: 2,
		Participants: []int64{10001, 10002}}
	raw, err := proto.Marshal(invite)
	if err != nil {
		t.Fatal(err)
	}
	decodedInvite := &CallInviteRes{}
	if err = proto.Unmarshal(raw, decodedInvite); err != nil {
		t.Fatal(err)
	}
	if decodedInvite.GetToken() != "token" || decodedInvite.GetTargetType() != 2 ||
		len(decodedInvite.GetParticipants()) != 2 {
		t.Fatalf("invite = %+v", decodedInvite)
	}

	state := &CallStateRes{CallId: "call_1", State: 2, GroupId: 9001, Active: true,
		Media: 1, Room: "room_1", TargetType: 2, Participants: []int64{10001},
		StartedAt: 100, ExpireAt: 200}
	raw, err = proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	decodedState := &CallStateRes{}
	if err = proto.Unmarshal(raw, decodedState); err != nil {
		t.Fatal(err)
	}
	if !decodedState.GetActive() || decodedState.GetGroupId() != 9001 ||
		decodedState.GetTargetType() != 2 || decodedState.GetExpireAt() != 200 {
		t.Fatalf("state = %+v", decodedState)
	}

	token := &RoomTokenRes{CallId: "call_1", Room: "room_1", Token: "t", Media: 1,
		Participants: []int64{10001}, TargetType: 2, ExpireAt: 200}
	raw, err = proto.Marshal(token)
	if err != nil {
		t.Fatal(err)
	}
	decodedToken := &RoomTokenRes{}
	if err = proto.Unmarshal(raw, decodedToken); err != nil {
		t.Fatal(err)
	}
	if decodedToken.GetMedia() != 1 || len(decodedToken.GetParticipants()) != 1 ||
		decodedToken.GetExpireAt() != 200 {
		t.Fatalf("token = %+v", decodedToken)
	}
}
