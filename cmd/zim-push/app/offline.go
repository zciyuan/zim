package app

import (
	"context"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	pushoffline "github.com/zciyuan/ZIM/internal/service/pushoffline/server"
	"github.com/zciyuan/ZIM/zimfw/multi"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// handleOffline consumes toOfflinePush and delegates to the dispatcher.
func (a *App) handleOffline(ctx context.Context, message pipe.Message) error {
	event, err := pushoffline.Decode(message.Value)
	if err != nil {
		log.Errorf("decode offline push event failed: %v", err)
		return pipe.Poison(err)
	}
	if a.offline == nil {
		return nil
	}
	if len(event.Uids) > 0 {
		return a.offline.HandleMany(ctx, event, event.Uids)
	}
	return a.offline.Handle(ctx, event)
}

// publishOffline enqueues one event for the whole offline recipient set. The
// consumer expands members and sends one provider batch, so a 2,000-member
// group produces one Kafka record instead of 2,000.
func (a *App) publishOffline(ctx context.Context, event *messagesvc.Event,
	buckets map[string][]int64) int {
	if a.offlinePublisher == nil || event == nil || len(event.Recipients) == 0 {
		return 0
	}
	online := make(map[int64]struct{}, len(buckets))
	for _, deviceKeys := range buckets {
		for _, deviceKey := range deviceKeys {
			if uid, _, err := multi.Parse(deviceKey); err == nil {
				online[uid] = struct{}{}
			}
		}
	}
	offline := make([]int64, 0, len(event.Recipients))
	for _, uid := range event.Recipients {
		if uid == event.SendID {
			continue
		}
		if _, ok := online[uid]; ok {
			continue
		}
		offline = append(offline, uid)
	}
	if len(offline) == 0 {
		return 0
	}
	if err := a.offlinePublisher.Publish(ctx, &pushoffline.Event{
		Uids: offline, ServerMsgID: event.ServerMsgID, ConversationID: event.ConversationID,
		Seq: event.Seq, SendID: event.SendID, MsgType: event.MsgType,
		SendTime: event.SendTime, AtUserIDs: event.AtUserIDs,
	}); err != nil {
		log.Warnf("publish offline push failed: server_msg_id = %s uids = %d err = %v",
			event.ServerMsgID, len(offline), err)
		return 0
	}
	metrics.Default.Add("zim_offline_queue_total", nil, int64(len(offline)))
	return len(offline)
}
