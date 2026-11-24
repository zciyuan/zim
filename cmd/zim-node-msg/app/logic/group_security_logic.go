package logic

import (
	"strconv"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	groupwire "github.com/zciyuan/ZIM/internal/protocol/group"
	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// setGroupSecurity handles op:1318 (ADR-0043).
func (c *core) setGroupSecurity(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSetGroupSecurity, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.SetGroupSecurityReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSetGroupSecurity, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpSetGroupSecurity, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpSetGroupSecurity, nil, errs.ErrServerBusy)
			return
		}
		if _, err = client.SetGroupSecurity(ctx.Context(), &grouppb.SetGroupSecurityArgs{
			GroupId: gid, OperatorId: uid, MuteAll: req.GetMuteAll(),
			ScheduledMuteEnable: req.GetScheduledMuteEnable(), ScheduledMuteStart: req.GetScheduledMuteStart(),
			ScheduledMuteEnd: req.GetScheduledMuteEnd(), ScheduledTimezone: req.GetScheduledTimezone(),
			DefaultMuteFlags: int32(req.GetDefaultMuteFlags()), NewMemberMuteSec: req.GetNewMemberMuteSec(),
			AntiSpamEnable: req.GetAntiSpamEnable(), AntiSpamWindowSec: req.GetAntiSpamWindowSec(),
			AntiSpamMaxMsgs: req.GetAntiSpamMaxMsgs(), SensitiveAutoMuteS: req.GetSensitiveAutoMuteS(),
			AntiScreenshot: req.GetAntiScreenshot(), WatermarkEnable: req.GetWatermarkEnable(),
			ScreenshotPenalty: req.GetScreenshotPenalty(),
		}); err != nil {
			c.respond(ctx, contract.OpSetGroupSecurity, nil, groupError(err))
			return
		}
		payload, _ := proto.Marshal(&groupwire.GroupOperationRes{Ok: true})
		c.respond(ctx, contract.OpSetGroupSecurity, payload, nil)
	})
}

// setMemberRestriction handles op:1319 (ADR-0043).
func (c *core) setMemberRestriction(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpSetMemberRestriction, nil, errs.ErrUnauthorized)
			return
		}
		req := &groupwire.SetMemberRestrictionReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpSetMemberRestriction, nil, errs.ErrInternal)
			return
		}
		gid, err := strconv.ParseInt(req.GetGroupId(), 10, 64)
		if err != nil || gid <= 0 {
			c.respond(ctx, contract.OpSetMemberRestriction, nil, errs.ErrGroupNotExist)
			return
		}
		client, err := groupclient.NewClient(c.proxy.NewMeshClient)
		if err != nil {
			c.respond(ctx, contract.OpSetMemberRestriction, nil, errs.ErrServerBusy)
			return
		}
		if _, err = client.SetMemberRestriction(ctx.Context(), &grouppb.SetMemberRestrictionArgs{
			GroupId: gid, OperatorId: uid, UserId: req.GetUserId(),
			MuteFlags: int32(req.GetMuteFlags()), IsWhitelist: req.GetIsWhitelist(),
			MuteUntil: req.GetMuteUntil(),
		}); err != nil {
			c.respond(ctx, contract.OpSetMemberRestriction, nil, groupError(err))
			return
		}
		payload, _ := proto.Marshal(&groupwire.GroupOperationRes{Ok: true})
		c.respond(ctx, contract.OpSetMemberRestriction, payload, nil)
	})
}
