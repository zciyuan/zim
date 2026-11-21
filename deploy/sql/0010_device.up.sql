-- 0010_device: per-user device registry for offline push (ADR-0024).
CREATE TABLE IF NOT EXISTS zim_device (
  owner_id       BIGINT       NOT NULL,
  platform       SMALLINT     NOT NULL,
  device_id      VARCHAR(128) NOT NULL DEFAULT '',
  device_name    VARCHAR(64)  NOT NULL DEFAULT '',
  push_provider  VARCHAR(16)  NOT NULL DEFAULT '',
  push_token     VARCHAR(512) NOT NULL DEFAULT '',
  voip_token     VARCHAR(512) NOT NULL DEFAULT '',
  app_version    VARCHAR(32)  NOT NULL DEFAULT '',
  os_version     VARCHAR(32)  NOT NULL DEFAULT '',
  status         SMALLINT     NOT NULL DEFAULT 0,
  last_active_at BIGINT       NOT NULL DEFAULT 0,
  badge          INT          NOT NULL DEFAULT 0,
  created_at     BIGINT       NOT NULL DEFAULT 0,
  updated_at     BIGINT       NOT NULL DEFAULT 0,
  PRIMARY KEY (owner_id, platform)
);

CREATE INDEX IF NOT EXISTS idx_device_token ON zim_device (push_provider, push_token);
