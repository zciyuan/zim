package logic

import (
	"testing"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	rtcsvc "github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestGroupCallContent(t *testing.T) {
	call := &rtcsvc.Call{ID: "call-1", Room: "zim-call-call-1", Media: 1,
		TargetType: 2, FromUID: 10001, CreatedAt: 1762900000000}
	raw := groupCallContent(call, "ongoing", 0, []int64{10001, 10002})
	parsed := struct {
		CallID       string  `json:"call_id"`
		Room         string  `json:"room"`
		Media        int32   `json:"media"`
		Status       string  `json:"status"`
		Result       string  `json:"result"`
		TargetType   int32   `json:"target_type"`
		Initiator    int64   `json:"initiator"`
		StartedAt    int64   `json:"started_at"`
		Duration     int64   `json:"duration"`
		Participants []int64 `json:"participants"`
	}{}
	if err := json.UnmarshalString(raw, &parsed); err != nil {
		t.Fatalf("content = %s, err = %v", raw, err)
	}
	if parsed.CallID != "call-1" || parsed.Room != "zim-call-call-1" || parsed.Media != 1 ||
		parsed.Status != "ongoing" || parsed.Result != "" || parsed.TargetType != 2 ||
		parsed.Initiator != 10001 || parsed.StartedAt != 1762900000000 || parsed.Duration != 0 {
		t.Fatalf("content = %s", raw)
	}
	if len(parsed.Participants) != 2 || parsed.Participants[1] != 10002 {
		t.Fatalf("participants = %v", parsed.Participants)
	}

	ended := groupCallContent(call, "ended", 125, []int64{10001, 10002})
	if err := json.UnmarshalString(ended, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Status != "ended" || parsed.Result != "ended" || parsed.Duration != 125 {
		t.Fatalf("ended = %s", ended)
	}
}

func TestJoinUIDs(t *testing.T) {
	if got := joinUIDs([]int64{3, 0, 1, -1, 2}); got != "3,1,2" {
		t.Fatalf("joinUIDs = %q", got)
	}
	if got := joinUIDs(nil); got != "" {
		t.Fatalf("empty joinUIDs = %q", got)
	}
}

func TestGroupCallMax(t *testing.T) {
	if got := groupCallMax(1); got != rtcsvc.MaxGroupVoiceParticipants {
		t.Fatalf("voice max = %d", got)
	}
	if got := groupCallMax(2); got != rtcsvc.MaxGroupVideoParticipants {
		t.Fatalf("video max = %d", got)
	}
	if got := familyCallMax(1); got != rtcsvc.MaxFamilyParticipants {
		t.Fatalf("family voice max = %d", got)
	}
	if got := familyCallMax(2); got != rtcsvc.MaxGroupVideoParticipants {
		t.Fatalf("family video max = %d", got)
	}
}

func TestCallStateRes(t *testing.T) {
	group := &rtcsvc.Call{ID: "call-1", FromUID: 1, ToUID: 9001, TargetType: 2,
		Media: 2, Room: "room", State: rtcsvc.StateAnswered, CreatedAt: 100, ExpireAt: 200}
	res := callStateRes(group, []int64{1, 2})
	if !res.Active || res.GroupId != 9001 || res.TargetType != 2 || res.Media != 2 ||
		len(res.Participants) != 2 || res.StartedAt != 100 || res.ExpireAt != 200 {
		t.Fatalf("group state = %+v", res)
	}

	direct := &rtcsvc.Call{ID: "call-2", FromUID: 1, ToUID: 2, TargetType: 1,
		Media: 1, Room: "room", State: rtcsvc.StateRinging, CreatedAt: 100}
	res = callStateRes(direct, []int64{1, 2})
	if !res.Active || res.GroupId != 0 || res.TargetType != 1 {
		t.Fatalf("direct state = %+v", res)
	}

	family := &rtcsvc.Call{ID: "call-4", FromUID: 1, ToUID: 9001, TargetType: 3,
		Media: 1, Room: "room", State: rtcsvc.StateRinging, CreatedAt: 100}
	res = callStateRes(family, []int64{1})
	if !res.Active || res.GroupId != 9001 || res.TargetType != 3 || res.State != rtcsvc.StateRinging {
		t.Fatalf("family state = %+v", res)
	}

	ended := &rtcsvc.Call{ID: "call-3", TargetType: 2, State: rtcsvc.StateEnded}
	if callStateRes(ended, nil).Active {
		t.Fatal("ended call must not be active")
	}
}

func TestCallDuration(t *testing.T) {
	if got := callDuration(&rtcsvc.Call{CreatedAt: time.Now().UnixMilli() - 1500}); got < 1 || got > 2 {
		t.Fatalf("duration = %d", got)
	}
	if got := callDuration(&rtcsvc.Call{CreatedAt: time.Now().Add(time.Minute).UnixMilli()}); got != 0 {
		t.Fatalf("future duration = %d", got)
	}
}

func TestRTCErrorMapping(t *testing.T) {
	cases := []struct {
		err  error
		code int
	}{
		{rtcsvc.ErrBusy, 180002},
		{rtcsvc.ErrRoomFull, 180008},
		{rtcsvc.ErrCallNotFound, 180004},
		{rtcsvc.ErrInvalidState, 180003},
		{rtcsvc.ErrForbidden, 180004},
		{rtcsvc.ErrInvalidCall, 180004},
		{errs.ErrRTCCallNotExist, 180004},
	}
	for _, item := range cases {
		if got := errs.From(rtcError(item.err)).Code().Code(); got != item.code {
			t.Fatalf("err %v mapped to %d, want %d", item.err, got, item.code)
		}
	}
}
