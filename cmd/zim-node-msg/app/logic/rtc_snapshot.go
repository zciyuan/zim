package logic

import (
	"context"

	authwire "github.com/zciyuan/ZIM/internal/protocol/auth"
	rtcfw "github.com/zciyuan/ZIM/zimfw/rtc"
)

// fillActiveCall attaches the caller's live call to the login response. The
// snapshot is authoritative for reconnects: signaling pushes are best-effort,
// so a client that reconnects mid-call reconciles here instead of waiting for
// an event that may have been delivered while it was offline.
func (c *core) fillActiveCall(ctx context.Context, uid int64, res *authwire.LoginRes) {
	if c.rtcCalls == nil || c.rtcProvider == nil || res == nil {
		return
	}
	call, err := c.rtcCalls.ActiveCall(ctx, uid)
	if err != nil {
		return
	}
	token, err := c.rtcProvider.IssueToken(ctx, call.Room, uid, rtcfw.RolePublisher)
	if err != nil {
		return
	}
	participants, _ := c.rtcCalls.Participants(ctx, call.ID)
	res.ActiveCall = &authwire.CallSnapshot{
		CallId: call.ID, State: call.State, Room: call.Room, ExpireAt: call.ExpireAt,
		Media: call.Media, TargetType: call.TargetType, Participants: participants,
		Version: call.Version, Seq: call.Seq, Token: token, SfuUrl: c.rtcClientURL,
	}
}
