-- 0025: 语音房表（ADR-0043）。

CREATE TABLE IF NOT EXISTS zim_voice_room (
    room_id        BIGINT PRIMARY KEY,
    room_type      SMALLINT NOT NULL, -- 1=GROUP 2=PUBLIC 3=PASSWORD
    group_id       BIGINT NOT NULL DEFAULT 0,
    owner_uid      BIGINT NOT NULL,
    title          VARCHAR(128) NOT NULL,
    announcement   VARCHAR(512) NOT NULL DEFAULT '',
    seat_count     SMALLINT NOT NULL DEFAULT 8,
    join_seat_mode SMALLINT NOT NULL DEFAULT 1, -- 1=自由上麦 2=举手审批
    password_hash  VARCHAR(255) NOT NULL DEFAULT '',
    status         SMALLINT NOT NULL DEFAULT 1, -- 1=LIVE 2=CLOSED
    created_at     BIGINT NOT NULL DEFAULT 0,
    closed_at      BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_vr_group_live ON zim_voice_room (group_id) WHERE status = 1;
