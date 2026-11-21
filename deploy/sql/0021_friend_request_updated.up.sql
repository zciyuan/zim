ALTER TABLE zim_friend_request
    ADD COLUMN IF NOT EXISTS updated_at BIGINT NOT NULL DEFAULT 0;

UPDATE zim_friend_request SET updated_at = created_at WHERE updated_at = 0;

CREATE INDEX IF NOT EXISTS idx_freq_to_updated
    ON zim_friend_request (to_id, updated_at DESC);
