CREATE TABLE IF NOT EXISTS zim_notification (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(128) NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL DEFAULT 0,
    published_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_notification_published ON zim_notification (published_at DESC, id DESC);
