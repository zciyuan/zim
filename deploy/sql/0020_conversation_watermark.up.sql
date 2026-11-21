ALTER TABLE zim_conversation
    ADD COLUMN IF NOT EXISTS read_seq BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS min_seq  BIGINT NOT NULL DEFAULT 0;

UPDATE zim_conversation c
SET read_seq = GREATEST(COALESCE(s.read_seq, c.last_msg_seq - 1), 0),
    min_seq  = COALESCE(s.min_seq, 0)
FROM zim_conversation s0
LEFT JOIN zim_seq_user s
       ON s.uid = s0.owner_id AND s.conversation_id = s0.conv_id
WHERE c.owner_id = s0.owner_id AND c.conv_id = s0.conv_id;
