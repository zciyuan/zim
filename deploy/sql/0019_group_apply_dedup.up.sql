-- Collapse duplicate pending group applications before adding the guard.
UPDATE zim_group_apply a
SET status = 3, handled_at = EXTRACT(EPOCH FROM now())::bigint * 1000
WHERE a.status = 0
  AND a.id NOT IN (
      SELECT MIN(id) FROM zim_group_apply WHERE status = 0 GROUP BY group_id, user_id
  );

CREATE UNIQUE INDEX IF NOT EXISTS uk_gapply_pending_pair
    ON zim_group_apply (group_id, user_id) WHERE status = 0;
