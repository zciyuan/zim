package family

import (
	"context"
	"time"
)

// GroupInfo is the subset of a group needed for family authorization.
type GroupInfo struct {
	ID      int64
	Type    int16
	OwnerID int64
}

// MemberInfo is one family member.
type MemberInfo struct {
	UID  int64
	Role int16
}

// Settings is the family-level auto-answer switch.
type Settings struct {
	GroupID           int64
	AutoAnswerEnabled bool
	FamilyMode        bool
}

// Consent is one member's auto-answer consent.
type Consent struct {
	GroupID     int64
	UserID      int64
	Granted     bool
	GuardianUID int64
	Channel     string
}

// Audit is one auto-answer audit record.
type Audit struct {
	ID        int64
	GroupID   int64
	ActorUID  int64
	TargetUID int64
	Action    string
	CallID    string
	CreatedAt int64
}

// Repository persists family settings, consent and audit.
type Repository interface {
	GetGroup(ctx context.Context, gid int64) (*GroupInfo, error)
	GetMember(ctx context.Context, gid, uid int64) (*MemberInfo, error)
	EligibleMembers(ctx context.Context, gid int64) ([]*MemberInfo, error)
	GetSettings(ctx context.Context, gid int64) (*Settings, error)
	UpsertSettings(ctx context.Context, settings *Settings) error
	GetConsent(ctx context.Context, gid, uid int64) (*Consent, error)
	UpsertConsent(ctx context.Context, consent *Consent) error
	AddAudit(ctx context.Context, audit *Audit) error
	ListAudit(ctx context.Context, gid int64, limit int) ([]*Audit, error)
}

// Service implements the T22 double-authorization policy.
type Service struct {
	repo Repository
	now  func() time.Time
	ids  func() int64
}
