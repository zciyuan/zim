package admin

import (
	"context"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// ListUsers returns a page of users.
func (s *Service) ListUsers(ctx context.Context, limit, offset int) ([]*model.User, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListUsers(ctx, limit, offset)
}

// SetUserStatus updates a user's status (0 normal, 1 banned).
func (s *Service) SetUserStatus(ctx context.Context, uid int64, status int16) error {
	return s.repo.SetStatus(ctx, uid, status, time.Now().UnixMilli())
}

// ListGroups returns a page of groups.
func (s *Service) ListGroups(ctx context.Context, limit, offset int) ([]*model.Group, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return s.groups.ListGroups(ctx, limit, offset)
}

// DismissGroup deletes a group and its members.
func (s *Service) DismissGroup(ctx context.Context, gid int64) error {
	return s.groups.Dismiss(ctx, gid)
}

// Audit records one admin operation.
func (s *Service) Audit(ctx context.Context, operator, action, target, requestID string) error {
	if s.audits == nil {
		return nil
	}
	return s.audits.Add(ctx, &AuditRecord{
		ID: snowflake.New().Next(), Operator: operator, Action: action,
		Target: target, RequestID: requestID, CreatedAt: time.Now().UnixMilli(),
	})
}

// ListAudit returns recent admin operations.
func (s *Service) ListAudit(ctx context.Context, operator string, limit int) ([]*AuditRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if s.audits == nil {
		return nil, nil
	}
	return s.audits.List(ctx, operator, limit)
}

// Stats returns admin dashboard counts.
func (s *Service) Stats(ctx context.Context) (map[string]int64, error) {
	users, err := s.repo.CountUsers(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := s.groups.CountGroups(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]int64{"users": users, "groups": groups}, nil
}

// ListFamilies returns a page of family groups.
func (s *Service) ListFamilies(ctx context.Context, limit, offset int) ([]*model.Group, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return s.groups.ListFamilies(ctx, limit, offset)
}

// GetSetting returns one admin setting.
func (s *Service) GetSetting(ctx context.Context, key string) (*Setting, error) {
	return s.config.GetSetting(ctx, key)
}

// SetSetting writes one admin setting.
func (s *Service) SetSetting(ctx context.Context, key, value string) error {
	return s.config.SetSetting(ctx, key, value)
}

// ListSensitiveWords returns blocked words.
func (s *Service) ListSensitiveWords(ctx context.Context) ([]*SensitiveWord, error) {
	return s.config.ListSensitiveWords(ctx)
}

// AddSensitiveWord inserts a blocked word.
func (s *Service) AddSensitiveWord(ctx context.Context, word string, action int16) error {
	return s.config.AddSensitiveWord(ctx, word, action)
}

// DeleteSensitiveWord removes a blocked word.
func (s *Service) DeleteSensitiveWord(ctx context.Context, id int64) error {
	return s.config.DeleteSensitiveWord(ctx, id)
}

// ListReports returns abuse reports.
func (s *Service) ListReports(ctx context.Context, status int16, limit int) ([]*Report, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	return s.config.ListReports(ctx, status, limit)
}

// CreateReport records a report.
func (s *Service) CreateReport(ctx context.Context, reporter int64, targetType, targetID, reason string) error {
	return s.config.CreateReport(ctx, reporter, targetType, targetID, reason)
}

// HandleReport updates a report status.
func (s *Service) HandleReport(ctx context.Context, id int64, status int16) error {
	return s.config.HandleReport(ctx, id, status)
}

// ListPushCerts returns configured push platforms.
func (s *Service) ListPushCerts(ctx context.Context) ([]*PushCert, error) {
	return s.config.ListPushCerts(ctx)
}

// SetPushCert writes a push certificate.
func (s *Service) SetPushCert(ctx context.Context, platform int16, cert string) error {
	return s.config.SetPushCert(ctx, platform, cert)
}

// ListWebhookDLQ returns failed webhook events.
func (s *Service) ListWebhookDLQ(ctx context.Context, limit int) ([]*WebhookDLQ, error) {
	return s.config.ListWebhookDLQ(ctx, limit)
}

// DeleteWebhookDLQ removes one failed event.
func (s *Service) DeleteWebhookDLQ(ctx context.Context, id int64) error {
	return s.config.DeleteWebhookDLQ(ctx, id)
}

// SetGroupSearchable toggles group search visibility.
func (s *Service) SetGroupSearchable(ctx context.Context, gid int64, searchable bool) error {
	return s.groups.SetSearchable(ctx, gid, searchable)
}
