CREATE TABLE IF NOT EXISTS zim_report (
    id BIGSERIAL PRIMARY KEY,
    reporter_id BIGINT NOT NULL DEFAULT 0,
    target_type VARCHAR(32) NOT NULL DEFAULT '',
    target_id VARCHAR(128) NOT NULL DEFAULT '',
    reason VARCHAR(255) NOT NULL DEFAULT '',
    status SMALLINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL DEFAULT 0,
    handled_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_report_status ON zim_report (status, id DESC);
