package logic

import (
	"context"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/protobuf/proto"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	msgsvc "github.com/zciyuan/ZIM/internal/protocol/message"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// burnReadAck handles op:1518 (ADR-0043): the single-chat receiver read a burn
// message, so the server starts the authoritative countdown and pushes
// BurnTimerStart (1910) to every online device of both parties.
func (c *core) burnReadAck(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		uid, _, err := multi.Parse(ctx.UID())
		if err != nil {
			c.respond(ctx, contract.OpBurnReadAck, nil, errs.ErrUnauthorized)
			return
		}
		req := &msgsvc.BurnReadAckReq{}
		if _, err = parseMessage(ctx, req); err != nil {
			c.respond(ctx, contract.OpBurnReadAck, nil, errs.ErrMessageEmpty)
			return
		}
		start, err := c.msg.BurnReadAck(ctx.Context(), uid, req.GetServerMsgId())
		if err != nil {
			metrics.Default.Inc("zim_burn_ack_total", map[string]string{"result": "error"})
			c.respond(ctx, contract.OpBurnReadAck, nil, err)
			return
		}
		metrics.Default.Inc("zim_burn_ack_total", map[string]string{"result": "ok"})
		c.pushBurnTimerStart(ctx.Context(), start)
		payload, _ := proto.Marshal(&msgsvc.BurnReadAckRes{
			BurnExpireAt: start.BurnExpireAt, ServerNowMs: start.ServerNowMS,
		})
		c.respond(ctx, contract.OpBurnReadAck, payload, nil)
	})
}

// pushBurnTimerStart multicasts 1910 to both parties' devices so every online
// endpoint calibrates its countdown to the server deadline (ADR-0043 §2.4.4,
// G-02). Per-target pushes keep one stale key from sinking the whole batch.
func (c *core) pushBurnTimerStart(ctx context.Context, start *message.BurnStart) {
	payload, err := proto.Marshal(&msgsvc.BurnTimerStartPush{
		ConversationId: start.ConversationID, ServerMsgId: start.ServerMsgID,
		Seq: start.Seq, BurnExpireAt: start.BurnExpireAt, ServerNowMs: start.ServerNowMS,
	})
	if err != nil {
		return
	}
	seen := make(map[int64]struct{}, 2)
	for _, uid := range []int64{start.SenderUID, start.ReceiverUID} {
		if uid <= 0 {
			continue
		}
		for _, key := range c.deviceKeys(ctx, uid) {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			c.pushDeviceKeys(ctx, []int64{key}, contract.OpPushBurnTimerStart, payload, false)
		}
	}
}
