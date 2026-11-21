// Package group is the PostgreSQL DAO for groups and members.
package group

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zciyuan/ZIM/internal/model"
)

// DAO is the group DAO.

// New returns a group DAO.
func New(pool *pgxpool.Pool) *DAO {
	return &DAO{pool: pool}
}

// Create inserts a group and its owner membership in one transaction.
func (d *DAO) Create(ctx context.Context, group *model.Group) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err = tx.Exec(ctx, `INSERT INTO zim_group
(group_id, name, face_url, notification, owner_id, group_type, max_member, need_verification,
 look_member_info, apply_member_friend, status, muted_until, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		group.ID, group.Name, group.FaceURL, group.Notification, group.OwnerID, group.GroupType,
		group.MaxMember, group.NeedVerification, group.LookMemberInfo, group.ApplyMemberFriend,
		group.Status, group.MutedUntil, group.CreatedAt); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO zim_group_member (group_id, user_id, role, joined_at)
VALUES ($1,$2,$3,$4)`, group.ID, group.OwnerID, model.GroupRoleOwner, group.CreatedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Get returns a group by ID.
func (d *DAO) Get(ctx context.Context, gid int64) (*model.Group, error) {
	group := &model.Group{}
	err := d.pool.QueryRow(ctx, `SELECT group_id, name, COALESCE(face_url,''), COALESCE(notification,''),
owner_id, group_type, max_member, need_verification, look_member_info, apply_member_friend,
status, COALESCE(muted_until,0), COALESCE(created_at,0) FROM zim_group WHERE group_id = $1`, gid).Scan(
		&group.ID, &group.Name, &group.FaceURL, &group.Notification, &group.OwnerID, &group.GroupType,
		&group.MaxMember, &group.NeedVerification, &group.LookMemberInfo, &group.ApplyMemberFriend,
		&group.Status, &group.MutedUntil, &group.CreatedAt)
	if err != nil {
		return nil, err
	}
	return group, nil
}

// Update updates editable group fields.
func (d *DAO) Update(ctx context.Context, group *model.Group) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_group
SET name = $2, face_url = $3, notification = $4, need_verification = $5,
    look_member_info = $6, apply_member_friend = $7
WHERE group_id = $1`, group.ID, group.Name, group.FaceURL, group.Notification,
		group.NeedVerification, group.LookMemberInfo, group.ApplyMemberFriend)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// Dismiss deletes members and the group in one transaction.
func (d *DAO) Dismiss(ctx context.Context, gid int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err = tx.Exec(ctx, `DELETE FROM zim_group_member WHERE group_id = $1`, gid); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM zim_group WHERE group_id = $1`, gid)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return tx.Commit(ctx)
}

// AddMember inserts one member if absent.
func (d *DAO) AddMember(ctx context.Context, member *model.GroupMember) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_group_member (group_id, user_id, role, mute_until, joined_at)
VALUES ($1,$2,$3,$4,$5) ON CONFLICT (group_id, user_id) DO NOTHING`,
		member.GroupID, member.UserID, member.Role, member.MuteUntil, member.JoinedAt)
	return err
}

// AddMembers inserts members in one transaction.
func (d *DAO) AddMembers(ctx context.Context, members []*model.GroupMember) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, member := range members {
		if _, err = tx.Exec(ctx, `INSERT INTO zim_group_member (group_id, user_id, role, mute_until, joined_at)
VALUES ($1,$2,$3,$4,$5) ON CONFLICT (group_id, user_id) DO NOTHING`,
			member.GroupID, member.UserID, member.Role, member.MuteUntil, member.JoinedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RemoveMember deletes one membership.
func (d *DAO) RemoveMember(ctx context.Context, gid, uid int64) error {
	tag, err := d.pool.Exec(ctx, `DELETE FROM zim_group_member WHERE group_id = $1 AND user_id = $2`, gid, uid)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// Member returns one membership.
func (d *DAO) Member(ctx context.Context, gid, uid int64) (*model.GroupMember, error) {
	member := &model.GroupMember{}
	err := d.pool.QueryRow(ctx, `SELECT group_id, user_id, role, COALESCE(mute_until,0), COALESCE(joined_at,0)
FROM zim_group_member WHERE group_id = $1 AND user_id = $2`, gid, uid).Scan(
		&member.GroupID, &member.UserID, &member.Role, &member.MuteUntil, &member.JoinedAt)
	if err != nil {
		return nil, err
	}
	return member, nil
}

// CountMembers returns the current member count.
func (d *DAO) CountMembers(ctx context.Context, gid int64) (int, error) {
	var count int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM zim_group_member WHERE group_id = $1`, gid).Scan(&count)
	return count, err
}

// MemberIDs returns all member IDs for fanout.
func (d *DAO) MemberIDs(ctx context.Context, gid int64) ([]int64, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id FROM zim_group_member WHERE group_id = $1 ORDER BY user_id`, gid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var uid int64
		if err = rows.Scan(&uid); err != nil {
			return nil, err
		}
		ids = append(ids, uid)
	}
	return ids, rows.Err()
}

// Members returns members ordered by role and join time.
func (d *DAO) Members(ctx context.Context, gid int64, offset, limit int) ([]*model.GroupMember, error) {
	rows, err := d.pool.Query(ctx, `SELECT group_id, user_id, role, COALESCE(mute_until,0), COALESCE(joined_at,0)
FROM zim_group_member WHERE group_id = $1 ORDER BY role DESC, joined_at, user_id LIMIT $2 OFFSET $3`,
		gid, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make([]*model.GroupMember, 0, limit)
	for rows.Next() {
		member := &model.GroupMember{}
		if err = rows.Scan(&member.GroupID, &member.UserID, &member.Role,
			&member.MuteUntil, &member.JoinedAt); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

// MyGroups returns groups the user belongs to.
func (d *DAO) MyGroups(ctx context.Context, uid int64) ([]*model.Group, error) {
	rows, err := d.pool.Query(ctx, `SELECT g.group_id, g.name, COALESCE(g.face_url,''), COALESCE(g.notification,''),
g.owner_id, g.group_type, g.max_member, g.need_verification, g.look_member_info, g.apply_member_friend,
g.status, COALESCE(g.muted_until,0), COALESCE(g.created_at,0)
FROM zim_group g JOIN zim_group_member m ON m.group_id = g.group_id
WHERE m.user_id = $1 ORDER BY g.group_id`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groups := make([]*model.Group, 0)
	for rows.Next() {
		group := &model.Group{}
		if err = rows.Scan(&group.ID, &group.Name, &group.FaceURL, &group.Notification, &group.OwnerID,
			&group.GroupType, &group.MaxMember, &group.NeedVerification, &group.LookMemberInfo,
			&group.ApplyMemberFriend, &group.Status, &group.MutedUntil, &group.CreatedAt); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

// SetRole updates a member role.
func (d *DAO) SetRole(ctx context.Context, gid, uid int64, role int16) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_group_member SET role = $3 WHERE group_id = $1 AND user_id = $2`,
		gid, uid, role)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetMemberMute sets one member's mute deadline.
func (d *DAO) SetMemberMute(ctx context.Context, gid, uid int64, until int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_group_member SET mute_until = $3 WHERE group_id = $1 AND user_id = $2`,
		gid, uid, until)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetMutedUntil sets the whole-group mute deadline.
func (d *DAO) SetMutedUntil(ctx context.Context, gid int64, until int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_group SET muted_until = $2 WHERE group_id = $1`, gid, until)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// TransferOwner changes the group owner and swaps member roles atomically.
func (d *DAO) TransferOwner(ctx context.Context, gid, oldOwner, newOwner int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE zim_group SET owner_id = $3 WHERE group_id = $1 AND owner_id = $2`,
		gid, oldOwner, newOwner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	if _, err = tx.Exec(ctx, `UPDATE zim_group_member SET role = $3 WHERE group_id = $1 AND user_id = $2`,
		gid, oldOwner, model.GroupRoleMember); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE zim_group_member SET role = $3 WHERE group_id = $1 AND user_id = $2`,
		gid, newOwner, model.GroupRoleOwner); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CreateApply inserts a join request.
func (d *DAO) CreateApply(ctx context.Context, apply *model.GroupApply) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO zim_group_apply
    (id, group_id, user_id, why, status, created_at, handled_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (group_id, user_id) WHERE status = 0 DO UPDATE SET
      why = EXCLUDED.why, created_at = EXCLUDED.created_at`,
		apply.ID, apply.GroupID, apply.UserID, apply.Why, apply.Status, apply.CreatedAt, apply.HandledAt)
	return err
}

// PendingApply returns the newest pending request for a member.
func (d *DAO) PendingApply(ctx context.Context, gid, uid int64) (*model.GroupApply, error) {
	apply := &model.GroupApply{}
	err := d.pool.QueryRow(ctx, `SELECT id, group_id, user_id, why, status, created_at, handled_at
FROM zim_group_apply WHERE group_id = $1 AND user_id = $2 AND status = $3
ORDER BY id DESC LIMIT 1`, gid, uid, model.GroupApplyPending).Scan(
		&apply.ID, &apply.GroupID, &apply.UserID, &apply.Why, &apply.Status, &apply.CreatedAt, &apply.HandledAt)
	if err != nil {
		return nil, err
	}
	return apply, nil
}

// AnswerApply updates the request status.
func (d *DAO) AnswerApply(ctx context.Context, id int64, status int16, handledAt int64) error {
	tag, err := d.pool.Exec(ctx, `UPDATE zim_group_apply SET status = $2, handled_at = $3 WHERE id = $1`,
		id, status, handledAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ListApplies returns group join requests by status.
func (d *DAO) ListApplies(ctx context.Context, gid int64, status int16, offset, limit int) ([]*model.GroupApply, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := d.pool.Query(ctx, `SELECT id, group_id, user_id, why, status, created_at, handled_at
FROM zim_group_apply WHERE group_id = $1 AND status = $2
ORDER BY id DESC LIMIT $3 OFFSET $4`, gid, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	applies := make([]*model.GroupApply, 0, limit)
	for rows.Next() {
		apply := &model.GroupApply{}
		if err = rows.Scan(&apply.ID, &apply.GroupID, &apply.UserID, &apply.Why,
			&apply.Status, &apply.CreatedAt, &apply.HandledAt); err != nil {
			return nil, err
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// Search returns groups whose name contains keyword.
func (d *DAO) Search(ctx context.Context, keyword string, limit int) ([]*model.Group, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := d.pool.Query(ctx, `SELECT group_id, name, face_url, notification, owner_id, group_type,
    max_member, need_verification, look_member_info, apply_member_friend, status, muted_until, created_at
FROM zim_group WHERE status = 0 AND searchable = 1 AND name ILIKE '%' || $1 || '%' ORDER BY group_id DESC LIMIT $2`,
		keyword, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := make([]*model.Group, 0, limit)
	for rows.Next() {
		group := &model.Group{}
		if err = rows.Scan(&group.ID, &group.Name, &group.FaceURL, &group.Notification, &group.OwnerID,
			&group.GroupType, &group.MaxMember, &group.NeedVerification, &group.LookMemberInfo,
			&group.ApplyMemberFriend, &group.Status, &group.MutedUntil, &group.CreatedAt); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

// ListGroups returns groups for the admin console.
func (d *DAO) ListGroups(ctx context.Context, limit, offset int) ([]*model.Group, error) {
	rows, err := d.pool.Query(ctx, `SELECT group_id, name, COALESCE(face_url, ''), owner_id, group_type,
 COALESCE(created_at, 0) FROM zim_group ORDER BY group_id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*model.Group, 0, limit)
	for rows.Next() {
		item := &model.Group{}
		if err = rows.Scan(&item.ID, &item.Name, &item.FaceURL, &item.OwnerID, &item.GroupType,
			&item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CountGroups returns total groups.
func (d *DAO) CountGroups(ctx context.Context) (int64, error) {
	var count int64
	err := d.pool.QueryRow(ctx, `SELECT count(*) FROM zim_group`).Scan(&count)
	return count, err
}

// ListFamilies returns family groups for the admin console.
func (d *DAO) ListFamilies(ctx context.Context, limit, offset int) ([]*model.Group, error) {
	rows, err := d.pool.Query(ctx, `SELECT group_id, name, COALESCE(face_url, ''), owner_id, group_type,
 COALESCE(created_at, 0) FROM zim_group WHERE group_type = 3 ORDER BY group_id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*model.Group, 0, limit)
	for rows.Next() {
		item := &model.Group{}
		if err = rows.Scan(&item.ID, &item.Name, &item.FaceURL, &item.OwnerID, &item.GroupType, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// SetSearchable enables or disables group search visibility.
func (d *DAO) SetSearchable(ctx context.Context, gid int64, searchable bool) error {
	flag := int16(0)
	if searchable {
		flag = 1
	}
	_, err := d.pool.Exec(ctx, `UPDATE zim_group SET searchable = $2 WHERE group_id = $1`, gid, flag)
	return err
}
