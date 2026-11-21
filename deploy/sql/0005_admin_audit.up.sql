CREATE TABLE IF NOT EXISTS zim_admin_audit (
    id BIGINT PRIMARY KEY,
    operator VARCHAR(64) NOT NULL DEFAULT '',
    action VARCHAR(64) NOT NULL,
    target VARCHAR(128) NOT NULL DEFAULT '',
    request_id VARCHAR(64) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_admin_audit_operator ON zim_admin_audit (operator, id DESC);
