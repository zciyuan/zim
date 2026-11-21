package family

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/errors"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// Errors.
var (
	ErrNotFamily   = errors.New("not a family group")
	ErrNotAllowed  = errors.New("family operation not allowed")
	ErrNotEligible = errors.New("member role is not eligible")
)

// New returns the family service.
func New(repo Repository) *Service {
	return &Service{repo: repo, now: time.Now, ids: snowflake.New().Next}
}

// UpdateSettings changes the family-level auto-answer switch.
func (s *Service) UpdateSettings(ctx context.Context, operator, gid int64, enabled, familyMode bool) error {
	group, err := s.requireFamily(ctx, gid)
	if err != nil {
		return err
	}
	if group.OwnerID != operator {
		return ErrNotAllowed
	}
	if err = s.repo.UpsertSettings(ctx, &Settings{GroupID: gid, AutoAnswerEnabled: enabled, FamilyMode: familyMode}); err != nil {
		return err
	}
	return s.audit(ctx, gid, operator, operator, "settings", "")
}

// GrantConsent records a child/elder member's auto-answer consent.
func (s *Service) GrantConsent(ctx context.Context, operator, gid, uid int64, granted bool, guardian int64, channel string) error {
	group, err := s.requireFamily(ctx, gid)
	if err != nil {
		return err
	}
	member, err := s.repo.GetMember(ctx, gid, uid)
	if err != nil {
		return err
	}
	if member.Role != model.GroupRoleChild && member.Role != model.GroupRoleElder {
		return ErrNotEligible
	}
	if operator != uid && operator != group.OwnerID {
		return ErrNotAllowed
	}
	if err = s.repo.UpsertConsent(ctx, &Consent{
		GroupID: gid, UserID: uid, Granted: granted, GuardianUID: guardian, Channel: channel,
	}); err != nil {
		return err
	}
	return s.audit(ctx, gid, operator, uid, "consent", "")
}

// EligibleMembers returns child/elder members eligible for auto-answer.
func (s *Service) EligibleMembers(ctx context.Context, gid int64) ([]*MemberInfo, error) {
	if _, err := s.requireFamily(ctx, gid); err != nil {
		return nil, err
	}
	return s.repo.EligibleMembers(ctx, gid)
}

// AutoAnswerAllowed requires family group + eligible role + admin switch + member consent.
func (s *Service) AutoAnswerAllowed(ctx context.Context, gid, uid int64) (bool, error) {
	group, err := s.requireFamily(ctx, gid)
	if err != nil {
		return false, err
	}
	_ = group
	member, err := s.repo.GetMember(ctx, gid, uid)
	if err != nil {
		return false, err
	}
	if member.Role != model.GroupRoleChild && member.Role != model.GroupRoleElder {
		return false, nil
	}
	settings, err := s.repo.GetSettings(ctx, gid)
	if err != nil || settings == nil || !settings.AutoAnswerEnabled {
		return false, err
	}
	consent, err := s.repo.GetConsent(ctx, gid, uid)
	if err != nil || consent == nil {
		return false, err
	}
	return consent.Granted, nil
}

// AuditAutoAnswer records one automatic answer event.
func (s *Service) AuditAutoAnswer(ctx context.Context, gid, actor, target int64, callID string) error {
	return s.audit(ctx, gid, actor, target, "auto_answer", callID)
}

// ListAudit returns recent family audit records.
func (s *Service) ListAudit(ctx context.Context, gid int64, limit int) ([]*Audit, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	return s.repo.ListAudit(ctx, gid, limit)
}

func (s *Service) requireFamily(ctx context.Context, gid int64) (*GroupInfo, error) {
	group, err := s.repo.GetGroup(ctx, gid)
	if err != nil {
		return nil, err
	}
	if group.Type != model.GroupTypeFamily {
		return nil, ErrNotFamily
	}
	return group, nil
}

func (s *Service) audit(ctx context.Context, gid, actor, target int64, action, callID string) error {
	return s.repo.AddAudit(ctx, &Audit{
		ID: s.ids(), GroupID: gid, ActorUID: actor, TargetUID: target,
		Action: action, CallID: callID, CreatedAt: s.now().UnixMilli(),
	})
}
