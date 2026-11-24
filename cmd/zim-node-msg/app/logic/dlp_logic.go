package logic

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/model"
	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

// dlpDebounceKey is the Redis key prefix for the 3-second screenshot
// notification debounce (ADR-0043 §3.2.4, G-05).
const dlpDebounceKey = "zim:dlp:debounce:"

// dlpGreyBarBody is the system grey-bar text broadcast after a screenshot.
const dlpGreyBarBody = " 截取了聊天屏幕"

// dlpRecordGreyBarBody is the grey-bar text broadcast for screen recording.
const dlpRecordGreyBarBody = " 正在录制屏幕"

// NewKafkaDLPAuditPublisher returns a Kafka-backed audit publisher.
func NewKafkaDLPAuditPublisher(producer pipe.Producer) DLPAuditPublisher {
	return &kafkaDLPAuditPublisher{producer: producer, topic: define.TopicDLPScreenshotAudit}
}

// PublishAudit enqueues one audit record.
func (p *kafkaDLPAuditPublisher) PublishAudit(ctx context.Context, event *DLPAuditEvent) error {
	if p == nil || p.producer == nil || event == nil {
		return nil
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.producer.Publish(ctx, pipe.Message{
		Topic: p.topic, Key: strconv.FormatInt(event.UID, 10), Value: value,
		Headers: map[string]string{"content-type": "application/json"},
	})
}

// screenshotReport handles op:1519 (ADR-0043 §3.2.4): every report is audited;
// notification and penalty are debounced 3s per (conv, uid).
func (c *core) screenshotReport(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpScreenshotReport, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.ScreenshotReportReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpScreenshotReport, nil, errs.ErrMessageEmpty)
			return
		}
		if req.GetConversationId() == "" {
			c.respond(ctx, contract.OpScreenshotReport, nil, errs.ErrMessageEmpty)
			return
		}
		action := c.dlpHandle(ctx.Context(), uid, req)
		payload, err := proto.Marshal(&msgsvc.ScreenshotReportRes{Ok: true})
		if err != nil {
			c.respond(ctx, contract.OpScreenshotReport, nil, errs.ErrInternal.WithCause(err))
			return
		}
		metrics.Default.Inc("zim_dlp_report_total", map[string]string{"result": "ok", "action": dlpActionName(action)})
		c.respond(ctx, contract.OpScreenshotReport, payload, nil)
	})
}

// dlpHandle audits the event, applies the 3s debounce for grey bar + penalty,
// and executes the configured penalty for group conversations.
func (c *core) dlpHandle(ctx context.Context, uid int64, req *msgsvc.ScreenshotReportReq) int16 {
	actionTaken := int16(model.ScreenshotPenaltyWarn)
	eventType := req.GetEventType()
	if eventType != 1 && eventType != 2 {
		eventType = 1
	}
	convType := req.GetConvType()
	if convType != 1 && convType != 2 {
		convType = 1
	}
	convID := req.GetConversationId()
	event := &DLPAuditEvent{
		ID: snowflake.New().Next(), ConvID: convID, ConvType: int16(convType),
		UID: uid, EventType: int16(eventType), DeviceInfo: req.GetDeviceInfo(),
		ActionTaken: actionTaken, CreatedAt: time.Now().UnixMilli(),
	}

	// 1. Full audit: publish the audit record regardless of debounce.
	if c.dlpAudit != nil {
		if err := c.dlpAudit.PublishAudit(ctx, event); err != nil {
			log.Warnf("dlp audit publish failed: uid = %d, conv = %s, err = %v", uid, convID, err)
		}
	}
	// Also persist the audit row through the group service (idempotent row,
	// no dedup needed; both paths are at-least-once by design).
	if err := c.recordDLPEvent(ctx, event); err != nil {
		log.Warnf("dlp audit persist failed: uid = %d, conv = %s, err = %v", uid, convID, err)
	}

	// 2. 3s debounce for the grey bar + penalty path (G-05).
	if c.cache != nil {
		key := dlpDebounceKey + convID + ":" + strconv.FormatInt(uid, 10)
		ok, err := c.cache.SetNX(ctx, key, "1", 3*time.Second).Result()
		if err != nil {
			log.Warnf("dlp debounce failed: uid = %d, conv = %s, err = %v", uid, convID, err)
			return actionTaken
		}
		if !ok {
			return actionTaken
		}
	}

	// 3. Grey bar broadcast.
	c.pushDLPGreyBar(ctx, uid, convID, eventType == 2)

	// 4. Penalty (group chats only; owner/admin immune, ADR-0043 §3.2.4.4).
	if convType != 2 {
		return actionTaken
	}
	penalty, err := c.groupScreenshotPenalty(ctx, convID, uid)
	if err != nil {
		log.Warnf("dlp penalty lookup failed: uid = %d, conv = %s, err = %v", uid, convID, err)
		return actionTaken
	}
	event.ActionTaken = int16(penalty)
	switch int16(penalty) {
	case model.ScreenshotPenaltyMute24H:
		if err = c.dlpMute24H(ctx, convID, uid); err != nil {
			log.Warnf("dlp auto mute failed: uid = %d, conv = %s, err = %v", uid, convID, err)
		} else {
			actionTaken = model.ScreenshotPenaltyMute24H
		}
	case model.ScreenshotPenaltyKick:
		if err = c.dlpKick(ctx, convID, uid); err != nil {
			log.Warnf("dlp auto kick failed: uid = %d, conv = %s, err = %v", uid, convID, err)
		} else {
			actionTaken = model.ScreenshotPenaltyKick
		}
	}
	return actionTaken
}

// groupScreenshotPenalty reads the group's penalty config and member role.
func (c *core) groupScreenshotPenalty(ctx context.Context, convID string, uid int64) (int32, error) {
	client, err := groupclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return 0, err
	}
	gid, err := strconv.ParseInt(convID, 10, 64)
	if err != nil {
		return 0, err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	reply, err := client.GetSecurity(callCtx, &grouppb.GetSecurityArgs{GroupId: gid})
	if err != nil {
		return 0, err
	}
	membership, err := client.GetMembership(callCtx, &grouppb.GetMembershipArgs{GroupId: gid, UserId: uid})
	if err != nil {
		return 0, err
	}
	if membership.GetMember() != nil {
		role := membership.GetMember().GetRole()
		// Owner/admin are immune to automatic penalties.
		if role >= 1 {
			return 0, nil
		}
	}
	return reply.GetSecurity().GetScreenshotPenalty(), nil
}

func (c *core) recordDLPEvent(ctx context.Context, event *DLPAuditEvent) error {
	client, err := groupclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	_, err = client.RecordDLPEvent(callCtx, &grouppb.RecordDLPEventArgs{
		ConvId: event.ConvID, ConvType: int32(event.ConvType), Uid: event.UID,
		EventType: int32(event.EventType), DeviceInfo: event.DeviceInfo,
		ActionTaken: int32(event.ActionTaken),
	})
	return err
}

func (c *core) dlpMute24H(ctx context.Context, convID string, uid int64) error {
	client, err := groupclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return err
	}
	gid, err := strconv.ParseInt(convID, 10, 64)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	_, err = client.MuteMember(callCtx, &grouppb.MuteMemberArgs{
		GroupId: gid, OperatorId: 0, UserId: uid, Until: time.Now().UnixMilli() + 24*3600*1000,
	})
	return err
}

func (c *core) dlpKick(ctx context.Context, convID string, uid int64) error {
	client, err := groupclient.NewClient(c.proxy.NewMeshClient)
	if err != nil {
		return err
	}
	gid, err := strconv.ParseInt(convID, 10, 64)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	defer cancel()
	_, err = client.RemoveMember(callCtx, &grouppb.RemoveMemberArgs{
		GroupId: gid, OperatorId: 0, UserId: uid,
	})
	return err
}

func dlpActionName(action int16) string {
	switch action {
	case model.ScreenshotPenaltyMute24H:
		return "mute"
	case model.ScreenshotPenaltyKick:
		return "kick"
	default:
		return "warn"
	}
}

// pushDLPGreyBar broadcasts a 1907 system grey bar to the conversation.
// The debounce upstream guarantees at most one grey bar per 3s per (conv, uid).
func (c *core) pushDLPGreyBar(ctx context.Context, uid int64, convID string, recording bool) {
	body := dlpGreyBarBody
	if recording {
		body = dlpRecordGreyBarBody
	}
	content := strconv.FormatInt(uid, 10) + body
	payload, err := proto.Marshal(&msgsvc.NotifyPush{
		ConversationId: convID, Content: content, At: time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	targets := make([]int64, 0, 2)
	if gid, parseErr := strconv.ParseInt(convID, 10, 64); parseErr == nil && gid > 0 {
		targets = append(targets, c.groupTargets(ctx, gid, uid)...)
	} else {
		// Single chat: convID is "s_<uid>_<uid>"; push to both parties when
		// resolvable, otherwise the reporter's own devices.
		targets = append(targets, uid)
	}
	c.pushDeviceKeys(ctx, dedupeInt64(targets), contract.OpPushNotify, payload, false)
}

func dedupeInt64(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	unique := values[:0]
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
