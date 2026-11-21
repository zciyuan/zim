CREATE TABLE IF NOT EXISTS zim_admin_setting (
    key VARCHAR(128) PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS zim_sensitive_word (
    id BIGSERIAL PRIMARY KEY,
    word VARCHAR(128) NOT NULL UNIQUE,
    action SMALLINT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sensitive_word_word ON zim_sensitive_word (word);
