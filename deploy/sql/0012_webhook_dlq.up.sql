CREATE TABLE IF NOT EXISTS zim_webhook_dlq (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL DEFAULT 0
);
