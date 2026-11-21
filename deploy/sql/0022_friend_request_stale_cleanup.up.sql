-- Close legacy pending friend requests whose pair is already friends.
--
-- Before the relation service closed BOTH directions of a request pair, an
-- accepted A->B request could leave a reciprocal B->A row (or a duplicate from
-- the pre-0018 insert path) at status = 0. Those rows stay in the recipient's
-- inbox and offer 同意/拒绝 buttons that can only fail with 120002.
--
-- Only rows with a current friendship are touched: the friendship is the source
-- of truth for "already accepted", and a pending request sent after a friend was
-- deleted (no friendship any more) must stay answerable. The update is
-- idempotent, so re-running the compose migrate step is safe.
UPDATE zim_friend_request r
SET status = 1,
    handled_at = COALESCE(NULLIF(r.updated_at, 0), NULLIF(r.created_at, 0),
                          EXTRACT(EPOCH FROM now())::bigint * 1000)
WHERE r.status = 0
  AND EXISTS (SELECT 1 FROM zim_friend f
              WHERE (f.uid = r.from_id AND f.friend_id = r.to_id)
                 OR (f.uid = r.to_id AND f.friend_id = r.from_id));
