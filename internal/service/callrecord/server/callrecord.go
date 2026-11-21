package callrecord

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/internal/service/rtc/server"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// Recorder publishes missed-call events into the normal message pipeline.
type Recorder struct {
	producer pipe.Producer
}

// New returns a recorder backed by a Kafka producer.
func New(producer pipe.Producer) *Recorder { return &Recorder{producer: producer} }

// Record publishes one timeout call record.
func (r *Recorder) Record(ctx context.Context, call *rtc.Call) error {
	if r == nil || r.producer == nil || call == nil {
		return nil
	}
	conversationID := fmt.Sprintf("g_%d", call.ToUID)
	msgType := int32(22)
	recvID := int64(0)
	groupID := call.ToUID
	if call.TargetType == 1 {
		groupID = 0
		low, high := call.FromUID, call.ToUID
		if low > high {
			low, high = high, low
		}
		conversationID = fmt.Sprintf("s_%d_%d", low, high)
		recvID = call.ToUID
		msgType = 20
		if call.Media == 2 {
			msgType = 21
		}
	}
	participants := fmt.Sprintf("[%d,%d]", call.FromUID, call.ToUID)
	if call.TargetType > 1 {
		// ToUID is a group id, not a participant.
		participants = fmt.Sprintf("[%d]", call.FromUID)
	}
	content := fmt.Sprintf(`{"call_id":%q,"duration":0,"result":"missed","participants":%s,"target_type":%d}`,
		call.ID, participants, call.TargetType)
	event := &message.Event{
		ClientMsgID:    "call-timeout-" + call.ID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		ConversationID: conversationID, SendID: call.FromUID, RecvID: recvID, GroupID: groupID,
		MsgType: msgType, Content: content, SendTime: time.Now().UnixMilli(),
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.producer.Publish(ctx, pipe.Message{Topic: define.TopicToRedis, Key: conversationID, Value: value})
}
