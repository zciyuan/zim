CREATE TABLE IF NOT EXISTS zim_notification_read (
    uid BIGINT NOT NULL,
    notification_id BIGINT NOT NULL,
    read_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (uid, notification_id)
);
