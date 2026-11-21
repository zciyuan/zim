package callrecord

import (
	"context"
	"testing"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

type captureProducer struct {
	messages []pipe.Message
}

func (p *captureProducer) Publish(_ context.Context, message pipe.Message) error {
	p.messages = append(p.messages, message)
	return nil
}

func (p *captureProducer) Close() error { return nil }

func TestRecordParticipants(t *testing.T) {
	cases := []struct {
		name             string
		call             *rtc.Call
		wantType         int32
		wantParticipants []int64
		wantResult       string
	}{
		{
			name:             "family uses uid not group id",
			call:             &rtc.Call{ID: "family-1", FromUID: 10001, ToUID: 9001, TargetType: 3, Media: 1},
			wantType:         22,
			wantParticipants: []int64{10001},
			wantResult:       "missed",
		},
		{
			name:             "direct keeps both peers",
			call:             &rtc.Call{ID: "direct-1", FromUID: 10001, ToUID: 10002, TargetType: 1, Media: 2},
			wantType:         21,
			wantParticipants: []int64{10001, 10002},
			wantResult:       "missed",
		},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			producer := &captureProducer{}
			recorder := New(producer)
			if err := recorder.Record(context.Background(), item.call); err != nil {
				t.Fatal(err)
			}
			if len(producer.messages) != 1 {
				t.Fatalf("messages = %d", len(producer.messages))
			}
			var event struct {
				MsgType int32  `json:"msg_type"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(producer.messages[0].Value, &event); err != nil {
				t.Fatal(err)
			}
			if event.MsgType != item.wantType {
				t.Fatalf("msg_type = %d, want %d", event.MsgType, item.wantType)
			}
			var content struct {
				Result       string  `json:"result"`
				Participants []int64 `json:"participants"`
				TargetType   int32   `json:"target_type"`
			}
			if err := json.UnmarshalString(event.Content, &content); err != nil {
				t.Fatal(err)
			}
			if content.Result != item.wantResult || content.TargetType != item.call.TargetType {
				t.Fatalf("content = %+v", content)
			}
			if len(content.Participants) != len(item.wantParticipants) {
				t.Fatalf("participants = %v", content.Participants)
			}
			for i := range item.wantParticipants {
				if content.Participants[i] != item.wantParticipants[i] {
					t.Fatalf("participants = %v", content.Participants)
				}
			}
		})
	}
}
