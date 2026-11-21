CREATE TABLE IF NOT EXISTS zim_family_setting (
    group_id BIGINT PRIMARY KEY,
    auto_answer_enabled SMALLINT NOT NULL DEFAULT 0,
    family_mode SMALLINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS zim_family_consent (
    group_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    scope VARCHAR(32) NOT NULL,
    granted SMALLINT NOT NULL DEFAULT 0,
    guardian_uid BIGINT NOT NULL DEFAULT 0,
    channel VARCHAR(16) NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, user_id, scope)
);

CREATE TABLE IF NOT EXISTS zim_family_audit (
    id BIGINT PRIMARY KEY,
    group_id BIGINT NOT NULL,
    actor_id BIGINT NOT NULL,
    target_uid BIGINT NOT NULL,
    action VARCHAR(32) NOT NULL,
    call_id VARCHAR(64) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_family_audit_group ON zim_family_audit (group_id, id DESC);
