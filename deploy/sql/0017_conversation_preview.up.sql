CREATE TABLE IF NOT EXISTS zim_conversation_preview (
    conv_id        TEXT PRIMARY KEY,
    last_msg_id    TEXT NOT NULL DEFAULT '',
    last_msg_seq   BIGINT NOT NULL DEFAULT 0,
    last_msg_at    BIGINT NOT NULL DEFAULT 0,
    last_msg_brief TEXT NOT NULL DEFAULT '',
    updated_at     BIGINT NOT NULL DEFAULT 0
);

INSERT INTO zim_conversation_preview (conv_id, last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at)
SELECT DISTINCT ON (conv_id)
       conv_id, COALESCE(last_msg_id, ''), COALESCE(last_msg_seq, 0),
       COALESCE(last_msg_at, 0), COALESCE(last_msg_brief, ''), COALESCE(updated_at, 0)
FROM zim_conversation
WHERE COALESCE(last_msg_seq, 0) > 0
ORDER BY conv_id, last_msg_seq DESC
ON CONFLICT (conv_id) DO NOTHING;
