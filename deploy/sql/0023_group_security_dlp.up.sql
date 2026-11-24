-- 0023: group security settings, member restrictions, DLP events (ADR-0043).
-- 时间列统一 BIGINT 毫秒；金额不涉及本迁移。

CREATE TABLE IF NOT EXISTS zim_group_security_settings (
    group_id               BIGINT PRIMARY KEY,
    mute_all               BOOLEAN NOT NULL DEFAULT FALSE,
    scheduled_mute_enable  BOOLEAN NOT NULL DEFAULT FALSE,
    scheduled_mute_start   VARCHAR(5) NOT NULL DEFAULT '00:00', -- HH:MM
    scheduled_mute_end     VARCHAR(5) NOT NULL DEFAULT '08:00', -- HH:MM
    scheduled_timezone     VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
    default_mute_flags     INT NOT NULL DEFAULT 0,
    new_member_mute_sec    INT NOT NULL DEFAULT 0,
    anti_spam_enable       BOOLEAN NOT NULL DEFAULT TRUE,
    anti_spam_window_sec   INT NOT NULL DEFAULT 5,
    anti_spam_max_msgs     INT NOT NULL DEFAULT 5,
    sensitive_auto_mute_s  INT NOT NULL DEFAULT 3600,
    anti_screenshot        BOOLEAN NOT NULL DEFAULT FALSE,
    watermark_enable       BOOLEAN NOT NULL DEFAULT FALSE,
    screenshot_penalty     SMALLINT NOT NULL DEFAULT 0, -- 0=WARN 1=MUTE_24H 2=KICK
    updated_at             BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS zim_group_member_restrictions (
    group_id     BIGINT NOT NULL,
    uid          BIGINT NOT NULL,
    is_whitelist BOOLEAN NOT NULL DEFAULT FALSE,
    mute_flags   INT NOT NULL DEFAULT 0,
    mute_until   BIGINT NOT NULL DEFAULT 0, -- 0=未禁言, -1=永久
    reason       VARCHAR(255) NOT NULL DEFAULT '',
    operator_uid BIGINT NOT NULL DEFAULT 0, -- 0=系统自动风控
    updated_at   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, uid)
);
CREATE INDEX IF NOT EXISTS idx_gmr_mute_until ON zim_group_member_restrictions (group_id, mute_until) WHERE mute_until <> 0;

CREATE TABLE IF NOT EXISTS zim_dlp_security_events (
    id           BIGINT PRIMARY KEY,
    conv_id      VARCHAR(64) NOT NULL,
    conv_type    SMALLINT NOT NULL, -- 1=单聊 2=群聊
    uid          BIGINT NOT NULL,
    event_type   SMALLINT NOT NULL, -- 1=SCREENSHOT 2=SCREEN_RECORD_START
    device_info  VARCHAR(255) NOT NULL DEFAULT '',
    action_taken SMALLINT NOT NULL DEFAULT 0, -- 0=NONE 1=WARN 2=MUTE_24H 3=KICK
    created_at   BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_dlp_conv_time ON zim_dlp_security_events (conv_id, created_at DESC);
