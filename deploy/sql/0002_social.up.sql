-- Social graph and group baseline. Timestamps are Unix milliseconds.

CREATE TABLE IF NOT EXISTS zim_friend (
    uid          BIGINT NOT NULL,
    friend_id    BIGINT NOT NULL,
    remark       VARCHAR(64),
    is_pinned    SMALLINT NOT NULL DEFAULT 0,
    created_at   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (uid, friend_id)
);
CREATE INDEX IF NOT EXISTS idx_friend_friend ON zim_friend (friend_id);

CREATE TABLE IF NOT EXISTS zim_friend_request (
    id           BIGINT PRIMARY KEY,
    from_id      BIGINT NOT NULL,
    to_id        BIGINT NOT NULL,
    why          VARCHAR(255),
    status       SMALLINT NOT NULL DEFAULT 0, -- 0 pending 1 accepted 2 rejected 3 expired
    created_at   BIGINT NOT NULL DEFAULT 0,
    handled_at   BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_freq_to ON zim_friend_request (to_id, status, id DESC);
CREATE INDEX IF NOT EXISTS idx_freq_from ON zim_friend_request (from_id, id DESC);

CREATE TABLE IF NOT EXISTS zim_blacklist (
    uid          BIGINT NOT NULL,
    black_id     BIGINT NOT NULL,
    created_at   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (uid, black_id)
);

CREATE TABLE IF NOT EXISTS zim_group (
    group_id           BIGINT PRIMARY KEY,
    name               VARCHAR(64) NOT NULL,
    face_url           VARCHAR(255),
    notification       VARCHAR(1024),
    owner_id           BIGINT NOT NULL,
    group_type         SMALLINT NOT NULL DEFAULT 1, -- 1 normal 2 authorized 3 family
    max_member         INT NOT NULL DEFAULT 2000,
    need_verification  SMALLINT NOT NULL DEFAULT 0,
    look_member_info   SMALLINT NOT NULL DEFAULT 1,
    apply_member_friend SMALLINT NOT NULL DEFAULT 1,
    status             SMALLINT NOT NULL DEFAULT 0,
    muted_until        BIGINT NOT NULL DEFAULT 0,
    created_at         BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_group_owner ON zim_group (owner_id);

CREATE TABLE IF NOT EXISTS zim_group_member (
    group_id    BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    role        SMALLINT NOT NULL DEFAULT 0, -- 0 member 1 admin 2 owner
    mute_until  BIGINT NOT NULL DEFAULT 0,
    joined_at   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_gm_user ON zim_group_member (user_id);

CREATE TABLE IF NOT EXISTS zim_group_apply (
    id           BIGINT PRIMARY KEY,
    group_id     BIGINT NOT NULL,
    user_id      BIGINT NOT NULL,
    why          VARCHAR(255),
    status       SMALLINT NOT NULL DEFAULT 0,
    created_at   BIGINT NOT NULL DEFAULT 0,
    handled_at   BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_gapply_group ON zim_group_apply (group_id, status, id DESC);
