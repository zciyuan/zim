package family

import (
	"context"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeRepo struct {
	group    *GroupInfo
	members  map[int64]*MemberInfo
	settings *Settings
	consent  map[int64]*Consent
	audits   []*Audit
}

func (f *fakeRepo) GetGroup(context.Context, int64) (*GroupInfo, error) { return f.group, nil }
func (f *fakeRepo) GetMember(_ context.Context, _, uid int64) (*MemberInfo, error) {
	return f.members[uid], nil
}
func (f *fakeRepo) EligibleMembers(context.Context, int64) ([]*MemberInfo, error) {
	return []*MemberInfo{{UID: 20, Role: model.GroupRoleChild}}, nil
}

func (f *fakeRepo) GetSettings(context.Context, int64) (*Settings, error) { return f.settings, nil }
func (f *fakeRepo) UpsertSettings(_ context.Context, s *Settings) error {
	f.settings = s
	return nil
}
func (f *fakeRepo) GetConsent(_ context.Context, _, uid int64) (*Consent, error) {
	return f.consent[uid], nil
}
func (f *fakeRepo) UpsertConsent(_ context.Context, c *Consent) error {
	f.consent[c.UserID] = c
	return nil
}
func (f *fakeRepo) AddAudit(_ context.Context, a *Audit) error {
	f.audits = append(f.audits, a)
	return nil
}
func (f *fakeRepo) ListAudit(context.Context, int64, int) ([]*Audit, error) { return f.audits, nil }

func TestDoubleAuthorization(t *testing.T) {
	repo := &fakeRepo{
		group:    &GroupInfo{ID: 1, Type: model.GroupTypeFamily, OwnerID: 10},
		members:  map[int64]*MemberInfo{20: {UID: 20, Role: model.GroupRoleChild}, 30: {UID: 30, Role: model.GroupRoleMember}},
		settings: &Settings{GroupID: 1},
		consent:  make(map[int64]*Consent),
	}
	service := New(repo)
	service.now = func() time.Time { return time.Unix(1, 0) }
	service.ids = func() int64 { return 1 }
	ctx := context.Background()

	if err := service.UpdateSettings(ctx, 20, 1, true, false); err != ErrNotAllowed {
		t.Fatalf("non-owner settings err = %v", err)
	}
	if err := service.GrantConsent(ctx, 10, 1, 30, true, 10, "guardian"); err != ErrNotEligible {
		t.Fatalf("ineligible err = %v", err)
	}
	if err := service.UpdateSettings(ctx, 10, 1, true, false); err != nil {
		t.Fatal(err)
	}
	if allowed, err := service.AutoAnswerAllowed(ctx, 1, 20); err != nil || allowed {
		t.Fatalf("without consent allowed = %v, err = %v", allowed, err)
	}
	if err := service.GrantConsent(ctx, 10, 1, 20, true, 10, "guardian"); err != nil {
		t.Fatal(err)
	}
	if allowed, err := service.AutoAnswerAllowed(ctx, 1, 20); err != nil || !allowed {
		t.Fatalf("with consent allowed = %v, err = %v", allowed, err)
	}
	if len(repo.audits) != 2 {
		t.Fatalf("audits = %d", len(repo.audits))
	}
}
