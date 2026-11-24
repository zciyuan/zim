package server

import (
	"context"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

var hhmmPattern = regexp.MustCompile(`^([01]\d|2[0-3]):[0-5]\d$`)

// MemberStatusPublisher is notified after mute/kick/whitelist changes so
// downstream services (voiceroom) can revoke LiveKit publish rights
// (ADR-0043, `zim.pipe.groupMemberStatus`).
type MemberStatusPublisher interface {
	PublishMute(ctx context.Context, gid, uid int64, flags int32, until int64, operatorUID int64) error
	PublishKick(ctx context.Context, gid, uid int64, operatorUID int64) error
}

// SetMemberStatusPublisher wires the mute/kick event publisher.
func (s *Server) SetMemberStatusPublisher(pub MemberStatusPublisher) {
	s.statusPub = pub
}

// doSetGroupSecurity applies op:1318 settings. Only owner/admin may change
// group security settings (ADR-0043 §3.1).
func (s *Server) doSetGroupSecurity(ctx context.Context, args setGroupSecurityArgs) (*pb.SecuritySettingsReply, error) {
	if _, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID); err != nil {
		return nil, err
	} else if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	if err := validateSchedule(args.ScheduledMuteStart, args.ScheduledMuteEnd, args.ScheduledTimezone); err != nil {
		return nil, err
	}
	if args.ScreenshotPenalty < 0 || args.ScreenshotPenalty > 2 {
		return nil, status.Error(codes.InvalidArgument, "screenshot penalty invalid")
	}
	current, err := s.repo.GetSecurity(ctx, args.GroupID)
	if err != nil {
		return nil, status.Error(codes.Internal, "security lookup failed")
	}
	now := time.Now().UnixMilli()
	settings := &model.GroupSecuritySettings{
		GroupID: args.GroupID, MuteAll: args.MuteAll,
		ScheduledMuteEnable: args.ScheduledMuteEnable,
		ScheduledMuteStart:  current.ScheduledMuteStart, ScheduledMuteEnd: current.ScheduledMuteEnd,
		ScheduledTimezone: current.ScheduledTimezone,
		DefaultMuteFlags:  current.DefaultMuteFlags, NewMemberMuteSec: current.NewMemberMuteSec,
		AntiSpamEnable: current.AntiSpamEnable, AntiSpamWindowSec: current.AntiSpamWindowSec,
		AntiSpamMaxMsgs: current.AntiSpamMaxMsgs, SensitiveAutoMuteS: current.SensitiveAutoMuteS,
		AntiScreenshot: current.AntiScreenshot, WatermarkEnable: current.WatermarkEnable,
		ScreenshotPenalty: current.ScreenshotPenalty, UpdatedAt: now,
	}
	// The RPC always carries the full configuration block (op:1318 is a
	// whole-row upsert, mirroring the DDL defaults in ZIM-02).
	if args.ScheduledMuteStart != "" {
		settings.ScheduledMuteStart = args.ScheduledMuteStart
	}
	if args.ScheduledMuteEnd != "" {
		settings.ScheduledMuteEnd = args.ScheduledMuteEnd
	}
	if args.ScheduledTimezone != "" {
		settings.ScheduledTimezone = args.ScheduledTimezone
	}
	settings.DefaultMuteFlags = args.DefaultMuteFlags
	settings.NewMemberMuteSec = args.NewMemberMuteSec
	settings.AntiSpamEnable = args.AntiSpamEnable
	settings.AntiSpamWindowSec = args.AntiSpamWindowSec
	settings.AntiSpamMaxMsgs = args.AntiSpamMaxMsgs
	settings.SensitiveAutoMuteS = args.SensitiveAutoMuteS
	settings.AntiScreenshot = args.AntiScreenshot
	settings.WatermarkEnable = args.WatermarkEnable
	settings.ScreenshotPenalty = int16(args.ScreenshotPenalty)
	if err = s.repo.UpsertSecurity(ctx, settings); err != nil {
		return nil, status.Error(codes.Internal, "security update failed")
	}
	return &pb.SecuritySettingsReply{Security: securityState(settings, nil, nil, 0)}, nil
}

// doSetMemberRestriction applies op:1319 category mute flags / whitelist.
// Admins cannot restrict the owner or another admin (ADR-0043: 130100).
func (s *Server) doSetMemberRestriction(ctx context.Context, args setMemberRestrictionArgs) (*pb.OperationReply, error) {
	_, operator, err := s.groupAndMember(ctx, args.GroupID, args.OperatorID)
	if err != nil {
		return nil, err
	}
	if operator.Role != model.GroupRoleOwner && operator.Role != model.GroupRoleAdmin {
		return nil, status.Error(codes.PermissionDenied, "no group permission")
	}
	target, err := s.repo.Member(ctx, args.GroupID, args.UserID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "member not found")
	}
	if operator.Role != model.GroupRoleOwner {
		if target.Role == model.GroupRoleOwner {
			return nil, status.Error(codes.PermissionDenied, "cannot mute owner")
		}
		if target.Role == model.GroupRoleAdmin {
			return nil, status.Error(codes.PermissionDenied, "cannot mute admin")
		}
	}
	current, err := s.repo.GetRestriction(ctx, args.GroupID, args.UserID)
	if err != nil {
		return nil, status.Error(codes.Internal, "restriction lookup failed")
	}
	restriction := &model.GroupMemberRestriction{
		GroupID: args.GroupID, UID: args.UserID, MuteFlags: args.MuteFlags,
		IsWhitelist: args.IsWhitelist, MuteUntil: args.MuteUntil, OperatorUID: args.OperatorID,
		UpdatedAt: time.Now().UnixMilli(),
	}
	if current != nil {
		if args.MuteFlags == 0 && !args.IsWhitelist && args.MuteUntil == 0 {
			restriction.MuteFlags = current.MuteFlags
			restriction.IsWhitelist = current.IsWhitelist
			restriction.MuteUntil = current.MuteUntil
		}
		restriction.Reason = current.Reason
	}
	if err = s.repo.UpsertRestriction(ctx, restriction); err != nil {
		return nil, status.Error(codes.Internal, "restriction update failed")
	}
	if s.statusPub != nil {
		_ = s.statusPub.PublishMute(ctx, args.GroupID, args.UserID, args.MuteFlags, args.MuteUntil, args.OperatorID)
	}
	return &pb.OperationReply{Ok: true}, nil
}

// doRecordDLPEvent appends a screenshot/record audit row (ADR-0043 §3.2).
func (s *Server) doRecordDLPEvent(ctx context.Context, args dlpEventArgs) error {
	return s.repo.InsertDLPEvent(ctx, &model.DLPSecurityEvent{
		ID: s.ids.Next(), ConvID: args.ConvID, ConvType: args.ConvType, UID: args.UID,
		EventType: args.EventType, DeviceInfo: args.DeviceInfo,
		ActionTaken: args.ActionTaken, CreatedAt: time.Now().UnixMilli(),
	})
}

// securityState renders the mute state the message path checks. now ms is the
// evaluation instant; member/restriction may be nil.
func securityState(settings *model.GroupSecuritySettings, restriction *model.GroupMemberRestriction, member *model.GroupMember, now int64) *pb.SecurityState {
	state := &pb.SecurityState{}
	if settings == nil {
		return state
	}
	state.MuteAll = settings.MuteAll
	state.DefaultMuteFlags = settings.DefaultMuteFlags
	state.ScheduledMuteActive = scheduledMuteActive(settings, now)
	state.AntiScreenshot = settings.AntiScreenshot
	state.WatermarkEnable = settings.WatermarkEnable
	state.ScreenshotPenalty = int32(settings.ScreenshotPenalty)
	state.AntiSpamEnable = settings.AntiSpamEnable
	state.AntiSpamWindowSec = settings.AntiSpamWindowSec
	state.AntiSpamMaxMsgs = settings.AntiSpamMaxMsgs
	state.SensitiveAutoMuteS = settings.SensitiveAutoMuteS
	if restriction != nil {
		state.IsWhitelist = restriction.IsWhitelist
		state.MemberMuteFlags = restriction.MuteFlags
		state.MemberMuteUntil = restriction.MuteUntil
	}
	if member != nil && settings.NewMemberMuteSec > 0 {
		protectedUntil := member.JoinedAt + int64(settings.NewMemberMuteSec)*1000
		if now < protectedUntil {
			state.NewMemberUntil = protectedUntil
		}
	}
	return state
}

// scheduledMuteActive evaluates the HH:MM window in the group's IANA timezone,
// including overnight windows where start > end (ADR-0043 G-04).
func scheduledMuteActive(settings *model.GroupSecuritySettings, now int64) bool {
	if !settings.ScheduledMuteEnable {
		return false
	}
	location, err := time.LoadLocation(settings.ScheduledTimezone)
	if err != nil {
		return false
	}
	start, startErr := time.Parse("15:04", settings.ScheduledMuteStart)
	end, endErr := time.Parse("15:04", settings.ScheduledMuteEnd)
	if startErr != nil || endErr != nil {
		return false
	}
	clock := time.UnixMilli(now).In(location)
	current := clock.Hour()*60 + clock.Minute()
	startMin := start.Hour()*60 + start.Minute()
	endMin := end.Hour()*60 + end.Minute()
	if startMin == endMin {
		return false
	}
	if startMin < endMin {
		return current >= startMin && current < endMin
	}
	return current >= startMin || current < endMin
}

func validateSchedule(start, end, timezone string) error {
	if start == "" && end == "" && timezone == "" {
		return nil
	}
	if !hhmmPattern.MatchString(start) || !hhmmPattern.MatchString(end) {
		return status.Error(codes.InvalidArgument, "schedule time must be HH:MM")
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return status.Error(codes.InvalidArgument, "schedule timezone invalid")
	}
	return nil
}
