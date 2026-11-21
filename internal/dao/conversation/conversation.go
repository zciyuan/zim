// Package conversation is the PostgreSQL DAO for conversations and watermarks.
package conversation

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
)

func now() int64 { return time.Now().UnixMilli() }

// New returns a conversation DAO.
func New(pool *pgxpool.Pool) *DAO {
	return &DAO{pool: pool}
}

// Create inserts a conversation if absent.
func (d *DAO) Create(ctx context.Context, conv *model.Conversation) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_conversation
(owner_id, conv_id, type, peer_id, group_id, recv_opt, is_pinned, burn_seconds, deleted,
 last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (owner_id, conv_id) DO NOTHING`,
		conv.OwnerID, conv.ConvID, conv.Type, conv.PeerID, conv.GroupID, conv.RecvOpt,
		conv.IsPinned, conv.BurnSeconds, conv.Deleted, conv.LastMsgID, conv.LastMsgSeq,
		conv.LastMsgAt, conv.LastMsgBrief, conv.UpdatedAt)
	return err
}

// Get returns one conversation.
func (d *DAO) Get(ctx context.Context, ownerID int64, convID string) (*model.Conversation, error) {
	conv := &model.Conversation{}
	err := d.pool.QueryRow(ctx, `SELECT c.owner_id, c.conv_id, c.type, c.peer_id, c.group_id, c.recv_opt,
c.is_pinned, c.burn_seconds, c.deleted, COALESCE(p.last_msg_id,c.last_msg_id,''), COALESCE(p.last_msg_seq,c.last_msg_seq,0),
COALESCE(p.last_msg_at,c.last_msg_at,0), COALESCE(p.last_msg_brief,c.last_msg_brief,''), COALESCE(c.read_seq,0), COALESCE(c.min_seq,0), COALESCE(c.updated_at,0)
FROM zim_conversation c
LEFT JOIN zim_conversation_preview p ON p.conv_id = c.conv_id
WHERE c.owner_id = $1 AND c.conv_id = $2`, ownerID, convID).Scan(
		&conv.OwnerID, &conv.ConvID, &conv.Type, &conv.PeerID, &conv.GroupID, &conv.RecvOpt,
		&conv.IsPinned, &conv.BurnSeconds, &conv.Deleted, &conv.LastMsgID, &conv.LastMsgSeq,
		&conv.LastMsgAt, &conv.LastMsgBrief, &conv.ReadSeq, &conv.MinSeq, &conv.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

// TouchLastMessage updates the summary fields, creating the row if needed.
func (d *DAO) TouchLastMessage(ctx context.Context, conv *model.Conversation) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_conversation
(owner_id, conv_id, type, peer_id, group_id, last_msg_id, last_msg_seq, last_msg_at,
 last_msg_brief, deleted, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10)
ON CONFLICT (owner_id, conv_id) DO UPDATE SET
  last_msg_id = EXCLUDED.last_msg_id,
  last_msg_seq = GREATEST(zim_conversation.last_msg_seq, EXCLUDED.last_msg_seq),
  last_msg_at = EXCLUDED.last_msg_at,
  last_msg_brief = EXCLUDED.last_msg_brief,
  deleted = 0,
  updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.last_msg_seq >= zim_conversation.last_msg_seq`,
		conv.OwnerID, conv.ConvID, conv.Type, conv.PeerID, conv.GroupID, conv.LastMsgID,
		conv.LastMsgSeq, conv.LastMsgAt, conv.LastMsgBrief, conv.UpdatedAt)
	return err
}

// List returns active conversations ordered by pin and last message.
func (d *DAO) List(ctx context.Context, ownerID int64, offset, limit int) ([]*model.Conversation, error) {
	rows, err := d.pool.Query(ctx, `SELECT c.owner_id, c.conv_id, c.type, c.peer_id, c.group_id, c.recv_opt,
c.is_pinned, c.burn_seconds, c.deleted, COALESCE(p.last_msg_id,c.last_msg_id,''), COALESCE(p.last_msg_seq,c.last_msg_seq,0),
COALESCE(p.last_msg_at,c.last_msg_at,0), COALESCE(p.last_msg_brief,c.last_msg_brief,''), COALESCE(c.read_seq,0), COALESCE(c.min_seq,0), COALESCE(c.updated_at,0)
FROM zim_conversation c
LEFT JOIN zim_conversation_preview p ON p.conv_id = c.conv_id
WHERE c.owner_id = $1 AND c.deleted = 0
ORDER BY c.is_pinned DESC, COALESCE(p.last_msg_at,c.last_msg_at,0) DESC, c.conv_id
LIMIT $2 OFFSET $3`, ownerID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]*model.Conversation, 0, limit)
	for rows.Next() {
		conv := &model.Conversation{}
		if err = rows.Scan(&conv.OwnerID, &conv.ConvID, &conv.Type, &conv.PeerID, &conv.GroupID,
			&conv.RecvOpt, &conv.IsPinned, &conv.BurnSeconds, &conv.Deleted, &conv.LastMsgID,
			&conv.LastMsgSeq, &conv.LastMsgAt, &conv.LastMsgBrief, &conv.ReadSeq, &conv.MinSeq, &conv.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, conv)
	}
	return items, rows.Err()
}

// Delete marks a conversation deleted for one owner.
func (d *DAO) Delete(ctx context.Context, ownerID int64, convID string) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_conversation SET deleted = 1, updated_at = $3
WHERE owner_id = $1 AND conv_id = $2`, ownerID, convID, now())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetPin updates the pin flag.
func (d *DAO) SetPin(ctx context.Context, ownerID int64, convID string, pinned int16) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_conversation SET is_pinned = $3, updated_at = $4
WHERE owner_id = $1 AND conv_id = $2`, ownerID, convID, pinned, now())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetRecvOpt updates the receive option.
func (d *DAO) SetRecvOpt(ctx context.Context, ownerID int64, convID string, opt int16) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_conversation SET recv_opt = $3, updated_at = $4
WHERE owner_id = $1 AND conv_id = $2`, ownerID, convID, opt, now())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// GetSeqUser returns the read/min watermarks, or zero values when absent.
func (d *DAO) GetSeqUser(ctx context.Context, uid int64, convID string) (*model.SeqUser, error) {
	state := &model.SeqUser{UID: uid, ConversationID: convID}
	err := d.pool.QueryRow(ctx, `SELECT read_seq, min_seq, COALESCE(updated_at,0)
FROM zim_seq_user WHERE uid = $1 AND conversation_id = $2`, uid, convID).Scan(
		&state.ReadSeq, &state.MinSeq, &state.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return state, nil
		}
		return nil, err
	}
	return state, nil
}

// SetReadSeq advances read_seq without moving min_seq.
func (d *DAO) SetReadSeq(ctx context.Context, uid int64, convID string, seq int64) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_seq_user (uid, conversation_id, read_seq, min_seq, updated_at)
VALUES ($1,$2,$3,0,$4)
ON CONFLICT (uid, conversation_id) DO UPDATE SET
  read_seq = GREATEST(zim_seq_user.read_seq, EXCLUDED.read_seq),
  updated_at = EXCLUDED.updated_at`, uid, convID, seq, now())
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `UPDATE zim_conversation SET read_seq = GREATEST(read_seq, $3), updated_at = $4
WHERE owner_id = $1 AND conv_id = $2`, uid, convID, seq, now())
	return err
}

// RaiseMin moves min_seq (and read_seq) forward, used on leave/kick/clear.
func (d *DAO) RaiseMin(ctx context.Context, uid int64, convID string, seq int64) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_seq_user (uid, conversation_id, read_seq, min_seq, updated_at)
VALUES ($1,$2,$3,$3,$4)
ON CONFLICT (uid, conversation_id) DO UPDATE SET
  min_seq = GREATEST(zim_seq_user.min_seq, EXCLUDED.min_seq),
  read_seq = GREATEST(zim_seq_user.read_seq, EXCLUDED.read_seq),
  updated_at = EXCLUDED.updated_at`, uid, convID, seq, now())
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `UPDATE zim_conversation
SET min_seq = GREATEST(min_seq, $3), read_seq = GREATEST(read_seq, $3), updated_at = $4
WHERE owner_id = $1 AND conv_id = $2`, uid, convID, seq, now())
	return err
}

// TouchLastMessageBatch applies all previews in one set-based statement.
func (d *DAO) TouchLastMessageBatch(ctx context.Context, conversations []*model.Conversation) error {
	if len(conversations) == 0 {
		return nil
	}
	owners := make([]int64, 0, len(conversations))
	convIDs := make([]string, 0, len(conversations))
	types := make([]int16, 0, len(conversations))
	peers := make([]int64, 0, len(conversations))
	groups := make([]int64, 0, len(conversations))
	msgIDs := make([]string, 0, len(conversations))
	seqs := make([]int64, 0, len(conversations))
	times := make([]int64, 0, len(conversations))
	briefs := make([]string, 0, len(conversations))
	updated := make([]int64, 0, len(conversations))
	for _, conv := range conversations {
		owners = append(owners, conv.OwnerID)
		convIDs = append(convIDs, conv.ConvID)
		types = append(types, conv.Type)
		peers = append(peers, conv.PeerID)
		groups = append(groups, conv.GroupID)
		msgIDs = append(msgIDs, conv.LastMsgID)
		seqs = append(seqs, conv.LastMsgSeq)
		times = append(times, conv.LastMsgAt)
		briefs = append(briefs, conv.LastMsgBrief)
		updated = append(updated, conv.UpdatedAt)
	}
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_conversation
(owner_id, conv_id, type, peer_id, group_id, last_msg_id, last_msg_seq, last_msg_at,
 last_msg_brief, deleted, read_seq, min_seq, updated_at)
SELECT owner_id, conv_id, type, peer_id, group_id, last_msg_id, last_msg_seq, last_msg_at,
 last_msg_brief, 0, GREATEST(last_msg_seq - 1, 0), 0, updated_at
FROM unnest($1::bigint[], $2::text[], $3::smallint[], $4::bigint[], $5::bigint[],
            $6::text[], $7::bigint[], $8::bigint[], $9::text[], $10::bigint[])
  AS t(owner_id, conv_id, type, peer_id, group_id, last_msg_id, last_msg_seq, last_msg_at,
       last_msg_brief, updated_at)
ON CONFLICT (owner_id, conv_id) DO UPDATE SET
  last_msg_id = EXCLUDED.last_msg_id,
  last_msg_seq = GREATEST(zim_conversation.last_msg_seq, EXCLUDED.last_msg_seq),
  last_msg_at = EXCLUDED.last_msg_at,
  last_msg_brief = EXCLUDED.last_msg_brief,
  deleted = 0,
  updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.last_msg_seq >= zim_conversation.last_msg_seq`,
		owners, convIDs, types, peers, groups, msgIDs, seqs, times, briefs, updated)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `INSERT INTO zim_seq_user (uid, conversation_id, read_seq, min_seq, updated_at)
SELECT owner_id, conv_id, GREATEST(last_msg_seq - 1, 0), 0, updated_at
FROM unnest($1::bigint[], $2::text[], $3::bigint[], $4::bigint[])
  AS t(owner_id, conv_id, last_msg_seq, updated_at)
ON CONFLICT (uid, conversation_id) DO NOTHING`, owners, convIDs, seqs, updated)
	return err
}

// TouchPreview updates the one shared preview row for a conversation.
func (d *DAO) TouchPreview(ctx context.Context, conv *model.Conversation) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_conversation_preview
(conv_id, last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (conv_id) DO UPDATE SET
  last_msg_id = EXCLUDED.last_msg_id,
  last_msg_seq = EXCLUDED.last_msg_seq,
  last_msg_at = EXCLUDED.last_msg_at,
  last_msg_brief = EXCLUDED.last_msg_brief,
  updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.last_msg_seq >= zim_conversation_preview.last_msg_seq`,
		conv.ConvID, conv.LastMsgID, conv.LastMsgSeq, conv.LastMsgAt, conv.LastMsgBrief, conv.UpdatedAt)
	return err
}

// PreviewSeq returns the shared preview sequence, or zero when absent.
func (d *DAO) PreviewSeq(ctx context.Context, convID string) (int64, error) {
	var seq int64
	err := d.pool.QueryRow(ctx, `SELECT last_msg_seq FROM zim_conversation_preview WHERE conv_id = $1`, convID).Scan(&seq)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

// TouchPreviewBatch upserts shared preview rows in one statement.
func (d *DAO) TouchPreviewBatch(ctx context.Context, conversations []*model.Conversation) error {
	if len(conversations) == 0 {
		return nil
	}
	convIDs := make([]string, 0, len(conversations))
	msgIDs := make([]string, 0, len(conversations))
	seqs := make([]int64, 0, len(conversations))
	times := make([]int64, 0, len(conversations))
	briefs := make([]string, 0, len(conversations))
	updated := make([]int64, 0, len(conversations))
	for _, conv := range conversations {
		convIDs = append(convIDs, conv.ConvID)
		msgIDs = append(msgIDs, conv.LastMsgID)
		seqs = append(seqs, conv.LastMsgSeq)
		times = append(times, conv.LastMsgAt)
		briefs = append(briefs, conv.LastMsgBrief)
		updated = append(updated, conv.UpdatedAt)
	}
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_conversation_preview
(conv_id, last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at)
SELECT conv_id, last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at
FROM unnest($1::text[], $2::text[], $3::bigint[], $4::bigint[], $5::text[], $6::bigint[])
  AS t(conv_id, last_msg_id, last_msg_seq, last_msg_at, last_msg_brief, updated_at)
ON CONFLICT (conv_id) DO UPDATE SET
  last_msg_id = EXCLUDED.last_msg_id,
  last_msg_seq = EXCLUDED.last_msg_seq,
  last_msg_at = EXCLUDED.last_msg_at,
  last_msg_brief = EXCLUDED.last_msg_brief,
  updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.last_msg_seq >= zim_conversation_preview.last_msg_seq`,
		convIDs, msgIDs, seqs, times, briefs, updated)
	return err
}

// GetSeqUsers returns read/min watermarks for a batch of conversations.
func (d *DAO) GetSeqUsers(ctx context.Context, uid int64, convIDs []string) (map[string]*model.SeqUser, error) {
	result := make(map[string]*model.SeqUser, len(convIDs))
	if len(convIDs) == 0 {
		return result, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT conversation_id, read_seq, min_seq
FROM zim_seq_user WHERE uid = $1 AND conversation_id = ANY($2)`, uid, convIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		state := &model.SeqUser{UID: uid}
		if err = rows.Scan(&state.ConversationID, &state.ReadSeq, &state.MinSeq); err != nil {
			return nil, err
		}
		result[state.ConversationID] = state
	}
	return result, rows.Err()
}

// SetReadBatch persists many users' monotonic read watermarks in one
// transaction: one unnest insert plus one unnest update.
func (d *DAO) SetReadBatch(ctx context.Context, marks []*model.ReadMark) error {
	if len(marks) == 0 {
		return nil
	}
	uids := make([]int64, 0, len(marks))
	convIDs := make([]string, 0, len(marks))
	seqs := make([]int64, 0, len(marks))
	for _, mark := range marks {
		uids = append(uids, mark.UID)
		convIDs = append(convIDs, mark.ConvID)
		seqs = append(seqs, mark.Seq)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `INSERT INTO zim_seq_user (uid, conversation_id, read_seq, min_seq, updated_at)
SELECT uid, conv_id, seq, 0, $4
FROM unnest($1::bigint[], $2::text[], $3::bigint[]) AS t(uid, conv_id, seq)
ON CONFLICT (uid, conversation_id) DO UPDATE SET
  read_seq = GREATEST(zim_seq_user.read_seq, EXCLUDED.read_seq),
  updated_at = EXCLUDED.updated_at`, uids, convIDs, seqs, now()); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE zim_conversation c
SET read_seq = GREATEST(c.read_seq, v.seq), updated_at = $4
FROM unnest($1::bigint[], $2::text[], $3::bigint[]) AS v(uid, conv_id, seq)
WHERE c.owner_id = v.uid AND c.conv_id = v.conv_id`, uids, convIDs, seqs, now())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
