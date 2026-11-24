package group

import (
	"context"

	"github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
)

// GetSecurity returns group security settings, or a zero-value settings row
// with defaults when the group has no row yet.
func (d *DAO) GetSecurity(ctx context.Context, gid int64) (*model.GroupSecuritySettings, error) {
	row := d.pool.QueryRow(ctx, `
		SELECT mute_all, scheduled_mute_enable, scheduled_mute_start, scheduled_mute_end,
		       scheduled_timezone, default_mute_flags, new_member_mute_sec, anti_spam_enable,
		       anti_spam_window_sec, anti_spam_max_msgs, sensitive_auto_mute_s, anti_screenshot,
		       watermark_enable, screenshot_penalty, updated_at
		FROM zim_group_security_settings WHERE group_id = $1`, gid)
	settings := &model.GroupSecuritySettings{GroupID: gid}
	err := row.Scan(
		&settings.MuteAll, &settings.ScheduledMuteEnable, &settings.ScheduledMuteStart,
		&settings.ScheduledMuteEnd, &settings.ScheduledTimezone, &settings.DefaultMuteFlags,
		&settings.NewMemberMuteSec, &settings.AntiSpamEnable, &settings.AntiSpamWindowSec,
		&settings.AntiSpamMaxMsgs, &settings.SensitiveAutoMuteS, &settings.AntiScreenshot,
		&settings.WatermarkEnable, &settings.ScreenshotPenalty, &settings.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return &model.GroupSecuritySettings{
			GroupID: gid, ScheduledMuteStart: "00:00", ScheduledMuteEnd: "08:00",
			ScheduledTimezone: "Asia/Shanghai", AntiSpamEnable: true,
			AntiSpamWindowSec: 5, AntiSpamMaxMsgs: 5, SensitiveAutoMuteS: 3600,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	return settings, nil
}

// UpsertSecurity writes the security settings row for a group.
func (d *DAO) UpsertSecurity(ctx context.Context, settings *model.GroupSecuritySettings) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO zim_group_security_settings (
			group_id, mute_all, scheduled_mute_enable, scheduled_mute_start, scheduled_mute_end,
			scheduled_timezone, default_mute_flags, new_member_mute_sec, anti_spam_enable,
			anti_spam_window_sec, anti_spam_max_msgs, sensitive_auto_mute_s, anti_screenshot,
			watermark_enable, screenshot_penalty, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (group_id) DO UPDATE SET
			mute_all = EXCLUDED.mute_all,
			scheduled_mute_enable = EXCLUDED.scheduled_mute_enable,
			scheduled_mute_start = EXCLUDED.scheduled_mute_start,
			scheduled_mute_end = EXCLUDED.scheduled_mute_end,
			scheduled_timezone = EXCLUDED.scheduled_timezone,
			default_mute_flags = EXCLUDED.default_mute_flags,
			new_member_mute_sec = EXCLUDED.new_member_mute_sec,
			anti_spam_enable = EXCLUDED.anti_spam_enable,
			anti_spam_window_sec = EXCLUDED.anti_spam_window_sec,
			anti_spam_max_msgs = EXCLUDED.anti_spam_max_msgs,
			sensitive_auto_mute_s = EXCLUDED.sensitive_auto_mute_s,
			anti_screenshot = EXCLUDED.anti_screenshot,
			watermark_enable = EXCLUDED.watermark_enable,
			screenshot_penalty = EXCLUDED.screenshot_penalty,
			updated_at = EXCLUDED.updated_at`,
		settings.GroupID, settings.MuteAll, settings.ScheduledMuteEnable, settings.ScheduledMuteStart,
		settings.ScheduledMuteEnd, settings.ScheduledTimezone, settings.DefaultMuteFlags,
		settings.NewMemberMuteSec, settings.AntiSpamEnable, settings.AntiSpamWindowSec,
		settings.AntiSpamMaxMsgs, settings.SensitiveAutoMuteS, settings.AntiScreenshot,
		settings.WatermarkEnable, settings.ScreenshotPenalty, settings.UpdatedAt,
	)
	return err
}

// GetRestriction returns the restriction row for a member, or nil when absent.
func (d *DAO) GetRestriction(ctx context.Context, gid, uid int64) (*model.GroupMemberRestriction, error) {
	row := d.pool.QueryRow(ctx, `
		SELECT group_id, uid, is_whitelist, mute_flags, mute_until, reason, operator_uid, updated_at
		FROM zim_group_member_restrictions WHERE group_id = $1 AND uid = $2`, gid, uid)
	restriction := &model.GroupMemberRestriction{}
	err := row.Scan(
		&restriction.GroupID, &restriction.UID, &restriction.IsWhitelist, &restriction.MuteFlags,
		&restriction.MuteUntil, &restriction.Reason, &restriction.OperatorUID, &restriction.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return restriction, nil
}

// UpsertRestriction writes a member restriction row.
func (d *DAO) UpsertRestriction(ctx context.Context, restriction *model.GroupMemberRestriction) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO zim_group_member_restrictions (
			group_id, uid, is_whitelist, mute_flags, mute_until, reason, operator_uid, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (group_id, uid) DO UPDATE SET
			is_whitelist = EXCLUDED.is_whitelist,
			mute_flags = EXCLUDED.mute_flags,
			mute_until = EXCLUDED.mute_until,
			reason = EXCLUDED.reason,
			operator_uid = EXCLUDED.operator_uid,
			updated_at = EXCLUDED.updated_at`,
		restriction.GroupID, restriction.UID, restriction.IsWhitelist, restriction.MuteFlags,
		restriction.MuteUntil, restriction.Reason, restriction.OperatorUID, restriction.UpdatedAt,
	)
	return err
}

// InsertDLPEvent appends one screenshot/record audit row.
func (d *DAO) InsertDLPEvent(ctx context.Context, event *model.DLPSecurityEvent) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO zim_dlp_security_events (id, conv_id, conv_type, uid, event_type, device_info, action_taken, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		event.ID, event.ConvID, event.ConvType, event.UID, event.EventType,
		event.DeviceInfo, event.ActionTaken, event.CreatedAt,
	)
	return err
}
