package family

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	familysvc "github.com/zciyuan/ZIM/internal/service/family/server"
)

// GetGroup returns group type and owner.
func (d *DAO) GetGroup(ctx context.Context, gid int64) (*familysvc.GroupInfo, error) {
	info := &familysvc.GroupInfo{}
	err := d.pool.QueryRow(ctx, `SELECT group_id, group_type, owner_id FROM zim_group WHERE group_id = $1`, gid).
		Scan(&info.ID, &info.Type, &info.OwnerID)
	return info, err
}

// GetMember returns one member's family role.
func (d *DAO) GetMember(ctx context.Context, gid, uid int64) (*familysvc.MemberInfo, error) {
	info := &familysvc.MemberInfo{UID: uid}
	err := d.pool.QueryRow(ctx, `SELECT role FROM zim_group_member WHERE group_id = $1 AND user_id = $2`, gid, uid).
		Scan(&info.Role)
	return info, err
}

// GetSettings returns the family switch, defaulting to disabled.
func (d *DAO) GetSettings(ctx context.Context, gid int64) (*familysvc.Settings, error) {
	settings := &familysvc.Settings{GroupID: gid}
	var enabled, familyMode int16
	err := d.pool.QueryRow(ctx, `SELECT auto_answer_enabled, family_mode FROM zim_family_setting WHERE group_id = $1`, gid).
		Scan(&enabled, &familyMode)
	if err == pgx.ErrNoRows {
		return settings, nil
	}
	settings.AutoAnswerEnabled = enabled == 1
	settings.FamilyMode = familyMode == 1
	return settings, err
}

// UpsertSettings writes the family switch.
func (d *DAO) UpsertSettings(ctx context.Context, settings *familysvc.Settings) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_family_setting
(group_id, auto_answer_enabled, family_mode, updated_at) VALUES ($1,$2,$3,$4)
ON CONFLICT (group_id) DO UPDATE SET auto_answer_enabled = EXCLUDED.auto_answer_enabled,
 family_mode = EXCLUDED.family_mode, updated_at = EXCLUDED.updated_at`,
		settings.GroupID, boolInt(settings.AutoAnswerEnabled), boolInt(settings.FamilyMode), time.Now().UnixMilli())
	return err
}

// GetConsent returns one member's consent.
func (d *DAO) GetConsent(ctx context.Context, gid, uid int64) (*familysvc.Consent, error) {
	consent := &familysvc.Consent{GroupID: gid, UserID: uid}
	var granted int16
	err := d.pool.QueryRow(ctx, `SELECT granted, guardian_uid, channel FROM zim_family_consent
WHERE group_id = $1 AND user_id = $2 AND scope = 'auto_answer'`, gid, uid).
		Scan(&granted, &consent.GuardianUID, &consent.Channel)
	if err == pgx.ErrNoRows {
		return consent, nil
	}
	consent.Granted = granted == 1
	return consent, err
}

// UpsertConsent writes one member's consent.
func (d *DAO) UpsertConsent(ctx context.Context, consent *familysvc.Consent) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_family_consent
(group_id, user_id, scope, granted, guardian_uid, channel, updated_at)
VALUES ($1,$2,'auto_answer',$3,$4,$5,$6)
ON CONFLICT (group_id, user_id, scope) DO UPDATE SET granted = EXCLUDED.granted,
 guardian_uid = EXCLUDED.guardian_uid, channel = EXCLUDED.channel, updated_at = EXCLUDED.updated_at`,
		consent.GroupID, consent.UserID, boolInt(consent.Granted), consent.GuardianUID,
		consent.Channel, time.Now().UnixMilli())
	return err
}

// AddAudit appends one family audit record.
func (d *DAO) AddAudit(ctx context.Context, audit *familysvc.Audit) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_family_audit
(id, group_id, actor_id, target_uid, action, call_id, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		audit.ID, audit.GroupID, audit.ActorUID, audit.TargetUID, audit.Action, audit.CallID, audit.CreatedAt)
	return err
}

// ListAudit returns recent audit records.
func (d *DAO) ListAudit(ctx context.Context, gid int64, limit int) ([]*familysvc.Audit, error) {
	rows, err := d.pool.Query(ctx, `SELECT id, group_id, actor_id, target_uid, action, call_id, created_at
FROM zim_family_audit WHERE group_id = $1 ORDER BY id DESC LIMIT $2`, gid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*familysvc.Audit, 0, limit)
	for rows.Next() {
		item := &familysvc.Audit{}
		if err = rows.Scan(&item.ID, &item.GroupID, &item.ActorUID, &item.TargetUID,
			&item.Action, &item.CallID, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func boolInt(value bool) int16 {
	if value {
		return 1
	}
	return 0
}

// EligibleMembers returns child/elder members of a family group.
func (d *DAO) EligibleMembers(ctx context.Context, gid int64) ([]*familysvc.MemberInfo, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, role FROM zim_group_member
WHERE group_id = $1 AND role IN (3,4) ORDER BY user_id`, gid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*familysvc.MemberInfo, 0, 4)
	for rows.Next() {
		item := &familysvc.MemberInfo{}
		if err = rows.Scan(&item.UID, &item.Role); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
