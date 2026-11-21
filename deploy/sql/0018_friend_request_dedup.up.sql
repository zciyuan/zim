-- Collapse duplicate pending friend requests before adding the unique guard.
UPDATE zim_friend_request f
SET status = 3, handled_at = EXTRACT(EPOCH FROM now())::bigint * 1000
WHERE f.status = 0
  AND f.id NOT IN (
      SELECT MIN(id) FROM zim_friend_request WHERE status = 0 GROUP BY from_id, to_id
  );

CREATE UNIQUE INDEX IF NOT EXISTS uk_freq_pending_pair
    ON zim_friend_request (from_id, to_id) WHERE status = 0;

-- Seed a read baseline for conversations that never had a seq_user row.
INSERT INTO zim_seq_user (uid, conversation_id, read_seq, min_seq, updated_at)
SELECT owner_id, conv_id, GREATEST(COALESCE(last_msg_seq, 0) - 1, 0), 0, COALESCE(updated_at, 0)
FROM zim_conversation
ON CONFLICT (uid, conversation_id) DO NOTHING;
