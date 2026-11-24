package logic

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	groupclient "github.com/zciyuan/ZIM/internal/service/group/client"
	grouppb "github.com/zciyuan/ZIM/internal/service/group/pb"
	message "github.com/zciyuan/ZIM/internal/service/message/server"
	relationclient "github.com/zciyuan/ZIM/internal/service/relation/client"
	relationpb "github.com/zciyuan/ZIM/internal/service/relation/pb"
)

const directoryTimeout = 3 * time.Second

// NewDirectory returns a message directory backed by the group/relation mesh.
func NewDirectory(proxy *node.Proxy) message.Directory {
	return &directory{proxy: proxy}
}

// Membership returns group and member state.
func (d *directory) Membership(ctx context.Context, gid, uid int64) (*message.Membership, error) {
	client, err := groupclient.NewClient(d.proxy.NewMeshClient)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	reply, err := client.GetMembership(callCtx, &grouppb.GetMembershipArgs{GroupId: gid, UserId: uid})
	if err == nil {
		ids, idsErr := client.MemberIDs(callCtx, &grouppb.MemberIDsArgs{GroupId: gid})
		if idsErr != nil {
			err = idsErr
		} else {
			member := &message.Membership{
				GroupID: gid, MaxMember: reply.GetGroup().GetMaxMember(),
				GroupType: int16(reply.GetGroup().GetGroupType()),
				MemberIDs: ids.GetUserIds(),
			}
			if reply.GetMember() != nil {
				member.Role = int16(reply.GetMember().GetRole())
				member.MemberMute = reply.GetMember().GetMuteUntil()
			}
			if reply.GetGroup() != nil {
				member.GroupMute = reply.GetGroup().GetMutedUntil()
			}
			if security := reply.GetSecurity(); security != nil {
				member.MuteAll = security.GetMuteAll()
				member.DefaultMuteFlags = security.GetDefaultMuteFlags()
				member.ScheduledMuteActive = security.GetScheduledMuteActive()
				member.IsWhitelist = security.GetIsWhitelist()
				member.MemberMuteFlags = security.GetMemberMuteFlags()
				member.MemberMuteUntil = security.GetMemberMuteUntil()
				member.NewMemberUntil = security.GetNewMemberUntil()
				member.AntiSpamEnable = security.GetAntiSpamEnable()
				member.AntiSpamWindowSec = security.GetAntiSpamWindowSec()
				member.AntiSpamMaxMsgs = security.GetAntiSpamMaxMsgs()
				member.SensitiveAutoMuteS = security.GetSensitiveAutoMuteS()
			}
			cancel()
			return member, nil
		}
	}
	cancel()
	return nil, err
}

// Relation returns friendship and block state.
func (d *directory) Relation(ctx context.Context, uid, target int64) (bool, bool, error) {
	client, err := relationclient.NewClient(d.proxy.NewMeshClient)
	if err != nil {
		return false, false, err
	}
	callCtx, cancel := context.WithTimeout(ctx, directoryTimeout)
	reply, err := client.IsFriend(callCtx, &relationpb.IsFriendArgs{Uid: uid, TargetId: target})
	cancel()
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, false, nil
		}
		return false, false, err
	}
	return reply.GetFriend(), reply.GetBlack(), nil
}
