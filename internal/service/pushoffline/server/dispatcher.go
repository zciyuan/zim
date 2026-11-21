package pushoffline

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
)

const (
	defaultTitle = "ZIM"
	defaultBody  = "你收到一条新消息"
)

// NewDispatcher returns a dispatcher. provider is required; limiter may be nil.
func NewDispatcher(devices DeviceSource, provider Provider, limiter Limiter) *Dispatcher {
	return &Dispatcher{devices: devices, provider: provider, limiter: limiter, now: time.Now}
}

// Handle delivers one offline event to every active device of the target user.
func (d *Dispatcher) Handle(ctx context.Context, event *Event) error {
	if d == nil || d.devices == nil || d.provider == nil || event == nil || event.UID <= 0 {
		return nil
	}

	// The rate limiter is fail-open: losing a notification is worse than one
	// extra push when Redis is briefly unavailable.
	if d.limiter != nil {
		allowed, err := d.limiter.Allow(ctx, event.UID)
		if err != nil {
			metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "limiter_error"})
			log.Warnf("offline push limiter failed: uid = %d, err = %v", event.UID, err)
		} else if !allowed {
			metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "limited"})
			return nil
		}
	}

	devices, err := d.devices.Targets(ctx, []int64{event.UID})
	if err != nil {
		metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "device_error"})
		return err
	}

	notifications := make([]Notification, 0, len(devices))
	for _, device := range devices {
		if device == nil || device.Status != 0 || device.PushToken == "" {
			continue
		}
		notifications = append(notifications, Notification{
			UID: device.OwnerID, Platform: device.Platform, Provider: device.PushProvider,
			Token: device.PushToken, Title: defaultTitle, Body: defaultBody, Badge: device.Badge,
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

	results, sendErr := d.provider.Send(ctx, notifications)
	if sendErr != nil {
		metrics.Default.Inc("zim_offline_push_total", map[string]string{"result": "provider_error"})
		return sendErr
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
	log.Infof("offline push sent uid=%d devices=%d ok=%d failed=%d invalid=%d",
		event.UID, len(notifications), ok, failed, invalid)
	return nil
}
