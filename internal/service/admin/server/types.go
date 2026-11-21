// Package admin implements the ZIM admin control-plane use cases.
package admin

import (
	"context"

	"github.com/zciyuan/ZIM/internal/model"
)

// Repository is the admin user repository.
type Repository interface {
	ListUsers(ctx context.Context, limit, offset int) ([]*model.User, error)
	SetStatus(ctx context.Context, uid int64, status int16, now int64) error
	CountUsers(ctx context.Context) (int64, error)
}

// GroupRepository is the admin group repository.
type GroupRepository interface {
	ListGroups(ctx context.Context, limit, offset int) ([]*model.Group, error)
	Dismiss(ctx context.Context, gid int64) error
	CountGroups(ctx context.Context) (int64, error)
	SetSearchable(ctx context.Context, gid int64, searchable bool) error
	ListFamilies(ctx context.Context, limit, offset int) ([]*model.Group, error)
}

// AuditRepository is the admin audit store.
type AuditRepository interface {
	Add(ctx context.Context, record *AuditRecord) error
	List(ctx context.Context, operator string, limit int) ([]*AuditRecord, error)
}

// AuditRecord is one admin operation record.
type AuditRecord struct {
	ID        int64  `json:"id"`
	Operator  string `json:"operator"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	RequestID string `json:"request_id"`
	CreatedAt int64  `json:"created_at"`
}

// Setting is one admin key/value.
type Setting struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	UpdatedAt int64  `json:"updated_at"`
}

// SensitiveWord is one blocked word.
type SensitiveWord struct {
	ID        int64  `json:"id"`
	Word      string `json:"word"`
	Action    int16  `json:"action"`
	CreatedAt int64  `json:"created_at"`
}

// Report is one abuse report.
type Report struct {
	ID         int64  `json:"id"`
	ReporterID int64  `json:"reporter_id"`
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Reason     string `json:"reason"`
	Status     int16  `json:"status"`
	CreatedAt  int64  `json:"created_at"`
}

// PushCert is one push provider certificate summary.
type PushCert struct {
	Platform   int16 `json:"platform"`
	Configured bool  `json:"configured"`
	UpdatedAt  int64 `json:"updated_at"`
}

// WebhookDLQ is one failed callback.
type WebhookDLQ struct {
	ID        int64  `json:"id"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	Error     string `json:"error"`
	CreatedAt int64  `json:"created_at"`
}

// ConfigRepository is the admin config/sensitive-word store.
type ConfigRepository interface {
	GetSetting(ctx context.Context, key string) (*Setting, error)
	SetSetting(ctx context.Context, key, value string) error
	ListSensitiveWords(ctx context.Context) ([]*SensitiveWord, error)
	AddSensitiveWord(ctx context.Context, word string, action int16) error
	DeleteSensitiveWord(ctx context.Context, id int64) error
	ListReports(ctx context.Context, status int16, limit int) ([]*Report, error)
	CreateReport(ctx context.Context, reporter int64, targetType, targetID, reason string) error
	HandleReport(ctx context.Context, id int64, status int16) error
	ListPushCerts(ctx context.Context) ([]*PushCert, error)
	SetPushCert(ctx context.Context, platform int16, cert string) error
	ListWebhookDLQ(ctx context.Context, limit int) ([]*WebhookDLQ, error)
	DeleteWebhookDLQ(ctx context.Context, id int64) error
}

// Service implements admin operations.
type Service struct {
	repo   Repository
	groups GroupRepository
	audits AuditRepository
	config ConfigRepository
}

// New returns an admin service.
func New(repo Repository, groups GroupRepository, audits AuditRepository, config ConfigRepository) *Service {
	return &Service{repo: repo, groups: groups, audits: audits, config: config}
}
