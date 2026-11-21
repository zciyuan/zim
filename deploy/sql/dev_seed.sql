-- Development-only seed. Password for alice/bob is "password".
-- Never apply this file in staging or production.
INSERT INTO zim_user (user_id, username, password_hash, nickname, status, created_at, updated_at)
VALUES
    (10001, 'alice', '$2a$10$3ZnjZIeHxeDM1sVdi6J.kezWSVRX53tVt5ucOBzKda4IEhC8Lf.S2', 'Alice', 0, 1762900000000, 1762900000000),
    (10002, 'bob',   '$2a$10$3ZnjZIeHxeDM1sVdi6J.kezWSVRX53tVt5ucOBzKda4IEhC8Lf.S2', 'Bob',   0, 1762900000000, 1762900000000)
ON CONFLICT DO NOTHING;
