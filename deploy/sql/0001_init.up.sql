-- ZIM relational baseline. All timestamps are Unix milliseconds.
-- The table is intentionally unpartitioned; zim_conversation and zim_seq_user
-- are partitioned when they are added.

CREATE TABLE IF NOT EXISTS zim_user (
    user_id       BIGINT PRIMARY KEY,
    username      VARCHAR(64) NOT NULL,
    password_hash VARCHAR(128) NOT NULL,
    nickname      VARCHAR(64),
    face_url      VARCHAR(255),
    gender        SMALLINT NOT NULL DEFAULT 0,
    status        SMALLINT NOT NULL DEFAULT 0,
    created_at    BIGINT,
    updated_at    BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS uk_user_name ON zim_user (username);
