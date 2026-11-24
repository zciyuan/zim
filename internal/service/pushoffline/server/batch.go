package pushoffline

import (
	"context"
	"strconv"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
)

type batchLimiter interface {
	AllowMany(ctx context.Context, uids []int64) (map[int64]bool, error)
}

// HandleMany expands and delivers one message to a batch of offline users.
func (d *Dispatcher) HandleMany(ctx context.Context, event *Event, uids []int64) error {
	if d == nil || d.devices == nil || d.provider == nil || event == nil || len(uids) == 0 {
		return nil
	}
	allowed := make([]int64, 0, len(uids))
	if batch, ok := d.limiter.(batchLimiter); ok {
		decision, err := batch.AllowMany(ctx, uids)
		if err != nil {
			log.Warnf("offline push batch limiter failed: uids = %d, err = %v", len(uids), err)
			allowed = append(allowed, uids...)
		} else {
			for _, uid := range uids {
				if decision[uid] {
					allowed = append(allowed, uid)
				}
			}
		}
	} else {
		for _, uid := range uids {
			ok := true
			if d.limiter != nil {
				var err error
				ok, err = d.limiter.Allow(ctx, uid)
				if err != nil {
					ok = true
				}
			}
			if ok {
				allowed = append(allowed, uid)
			}
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	devices, err := d.devices.Targets(ctx, allowed)
	if err != nil {
		metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "device_error"})
		return err
	}
	notifications := make([]Notification, 0, len(devices))
	body := defaultBody
	if event.BurnTTL > 0 {
		// ADR-0043 §2.1.6: burn messages never leak content into vendor pushes.
		body = "你收到一条私密消息"
	}
	for _, device := range devices {
		if device == nil || device.Status != 0 || device.PushToken == "" {
			continue
		}
		notifications = append(notifications, Notification{
			UID: device.OwnerID, Platform: device.Platform, Provider: device.PushProvider,
			Token: device.PushToken, Title: defaultTitle, Body: body, Badge: device.Badge,
			Extra: map[string]string{
				"server_msg_id": event.ServerMsgID,
				"conv_id":       event.ConversationID,
				"seq":           strconv.FormatInt(event.Seq, 10),
				"send_id":       strconv.FormatInt(event.SendID, 10),
			},
		})
	}
	if len(notifications) == 0 {
		metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "no_device"})
		return nil
	}
	results, err := d.provider.Send(ctx, notifications)
	if err != nil {
		metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "provider_error"})
		return err
	}
	var ok, failed, invalid int
	for _, result := range results {
		switch {
		case result.OK:
			ok++
		case result.Invalid:
			invalid++
		default:
			failed++
		}
	}
	metrics.Default.Add("zim_offline_push_total", map[string]string{"result": "ok"}, int64(ok))
	metrics.Default.Add("zim_offline_push_total", map[string]string{"result": "failed"}, int64(failed))
	metrics.Default.Add("zim_offline_push_total", map[string]string{"result": "invalid"}, int64(invalid))
	log.Infof("offline push batch users=%d devices=%d ok=%d failed=%d invalid=%d",
		len(allowed), len(notifications), ok, failed, invalid)
	return nil
}
