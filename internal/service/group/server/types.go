package server

import (
	"context"

	"github.com/dobyte/due/v2/cluster/mesh"

	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// ConversationLeaver raises a member's min_seq and reads their last seq.
type ConversationLeaver interface {
	LastSeq(ctx context.Context, ownerID int64, convID string) int64
	RaiseMin(ctx context.Context, uid int64, convID string, seq int64) error
}

// Server is the group mesh service provider.
type Server struct {
	pb.UnimplementedGroupServer
	proxy     *mesh.Proxy
	repo      Repository
	ids       *snowflake.Generator
	changes   *changelog.Service
	versions  GroupVersion
	webhook   webhook.Notifier
	conv      ConversationLeaver
	members   *MemberCache
	statusPub MemberStatusPublisher
}

// GroupVersion tracks member/join version counters.
type GroupVersion interface {
	IncrMember(ctx context.Context, gid int64) (int64, error)
	IncrJoin(ctx context.Context, gid int64) (int64, error)
}

type createGroupArgs struct {
	OwnerID          int64
	Name             string
	FaceURL          string
	GroupType        int32
	MemberIDs        []int64
	NeedVerification int32
}

type getGroupArgs struct{ GroupID int64 }

type updateGroupArgs struct {
	GroupID           int64
	OperatorID        int64
	Name              string
	FaceURL           string
	Notification      string
	NeedVerification  int32
	LookMemberInfo    int32
	ApplyMemberFriend int32
}

type dismissGroupArgs struct {
	GroupID    int64
	OperatorID int64
}

type quitGroupArgs struct {
	GroupID int64
	UserID  int64
}

type inviteMembersArgs struct {
	GroupID    int64
	OperatorID int64
	UserIDs    []int64
}

type removeMemberArgs struct {
	GroupID    int64
	OperatorID int64
	UserID     int64
}

type membersArgs struct {
	GroupID int64
	Offset  int32
	Limit   int32
}

type myGroupsArgs struct{ UserID int64 }

type setAdminArgs struct {
	GroupID    int64
	OperatorID int64
	UserID     int64
	Admin      bool
}

type muteMemberArgs struct {
	GroupID    int64
	OperatorID int64
	UserID     int64
	Until      int64
}

type muteAllArgs struct {
	GroupID    int64
	OperatorID int64
	Until      int64
}

type getMembershipArgs struct {
	GroupID int64
	UserID  int64
}

type memberIDsArgs struct{ GroupID int64 }

type transferGroupArgs struct {
	GroupID    int64
	OperatorID int64
	NewOwnerID int64
}

type applyJoinArgs struct {
	GroupID int64
	UserID  int64
	Why     string
}

type answerJoinArgs struct {
	GroupID    int64
	OperatorID int64
	UserID     int64
	Accept     bool
}

type listAppliesArgs struct {
	GroupID    int64
	OperatorID int64
	Status     int32
	Offset     int32
	Limit      int32
}

type searchGroupsArgs struct {
	Keyword string
	Limit   int32
}

// ADR-0043: group security settings (op:1318).
type setGroupSecurityArgs struct {
	GroupID             int64
	OperatorID          int64
	MuteAll             bool
	ScheduledMuteEnable bool
	ScheduledMuteStart  string
	ScheduledMuteEnd    string
	ScheduledTimezone   string
	DefaultMuteFlags    int32
	NewMemberMuteSec    int32
	AntiSpamEnable      bool
	AntiSpamWindowSec   int32
	AntiSpamMaxMsgs     int32
	SensitiveAutoMuteS  int32
	AntiScreenshot      bool
	WatermarkEnable     bool
	ScreenshotPenalty   int32
}

// ADR-0043: member category mute flags / whitelist (op:1319).
type setMemberRestrictionArgs struct {
	GroupID     int64
	OperatorID  int64
	UserID      int64
	MuteFlags   int32
	IsWhitelist bool
	MuteUntil   int64
}

// ADR-0043: DLP audit record.
type dlpEventArgs struct {
	ConvID      string
	ConvType    int16
	UID         int64
	EventType   int16
	DeviceInfo  string
	ActionTaken int16
}
