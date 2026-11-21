package server

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeRepo struct {
	mu      sync.Mutex
	groups  map[int64]*model.Group
	members map[int64]map[int64]*model.GroupMember
	applies []*model.GroupApply
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{groups: make(map[int64]*model.Group), members: make(map[int64]map[int64]*model.GroupMember)}
}

func (f *fakeRepo) Create(_ context.Context, group *model.Group) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy := *group
	f.groups[group.ID] = &copy
	f.members[group.ID] = map[int64]*model.GroupMember{
		group.OwnerID: {GroupID: group.ID, UserID: group.OwnerID, Role: model.GroupRoleOwner},
	}
	return nil
}

func (f *fakeRepo) Get(_ context.Context, gid int64) (*model.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if group, ok := f.groups[gid]; ok {
		return group, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeRepo) Update(_ context.Context, group *model.Group) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[group.ID] = group
	return nil
}

func (f *fakeRepo) Dismiss(_ context.Context, gid int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.groups, gid)
	delete(f.members, gid)
	return nil
}

func (f *fakeRepo) TransferOwner(_ context.Context, gid, oldOwner, newOwner int64) error {
	group, ok := f.groups[gid]
	if !ok {
		return errors.New("group not found")
	}
	group.OwnerID = newOwner
	return nil
}

func (f *fakeRepo) AddMembers(_ context.Context, members []*model.GroupMember) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, member := range members {
		if f.members[member.GroupID] == nil {
			f.members[member.GroupID] = make(map[int64]*model.GroupMember)
		}
		f.members[member.GroupID][member.UserID] = member
	}
	return nil
}

func (f *fakeRepo) RemoveMember(_ context.Context, gid, uid int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.members[gid][uid]; !ok {
		return pgx.ErrNoRows
	}
	delete(f.members[gid], uid)
	return nil
}

func (f *fakeRepo) Member(_ context.Context, gid, uid int64) (*model.GroupMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if member, ok := f.members[gid][uid]; ok {
		return member, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeRepo) Members(_ context.Context, gid int64, _, _ int) ([]*model.GroupMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	members := make([]*model.GroupMember, 0, len(f.members[gid]))
	for _, member := range f.members[gid] {
		members = append(members, member)
	}
	return members, nil
}

func (f *fakeRepo) MyGroups(_ context.Context, uid int64) ([]*model.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	groups := make([]*model.Group, 0)
	for gid, members := range f.members {
		if _, ok := members[uid]; ok {
			groups = append(groups, f.groups[gid])
		}
	}
	return groups, nil
}

func (f *fakeRepo) SetRole(_ context.Context, gid, uid int64, role int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if member, ok := f.members[gid][uid]; ok {
		member.Role = role
		return nil
	}
	return pgx.ErrNoRows
}

func (f *fakeRepo) SetMemberMute(_ context.Context, gid, uid int64, until int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if member, ok := f.members[gid][uid]; ok {
		member.MuteUntil = until
		return nil
	}
	return pgx.ErrNoRows
}

func (f *fakeRepo) SetMutedUntil(_ context.Context, gid int64, until int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if group, ok := f.groups[gid]; ok {
		group.MutedUntil = until
		return nil
	}
	return pgx.ErrNoRows
}

func (f *fakeRepo) CountMembers(_ context.Context, gid int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.members[gid]), nil
}

func (f *fakeRepo) MemberIDs(_ context.Context, gid int64) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.members[gid]))
	for uid := range f.members[gid] {
		ids = append(ids, uid)
	}
	return ids, nil
}

func (f *fakeRepo) CreateApply(_ context.Context, apply *model.GroupApply) error {
	f.applies = append(f.applies, apply)
	return nil
}

func (f *fakeRepo) PendingApply(_ context.Context, gid, uid int64) (*model.GroupApply, error) {
	for _, apply := range f.applies {
		if apply.GroupID == gid && apply.UserID == uid && apply.Status == model.GroupApplyPending {
			return apply, nil
		}
	}
	return nil, errors.New("not found")
}

func (f *fakeRepo) AnswerApply(_ context.Context, id int64, status int16, handledAt int64) error {
	for _, apply := range f.applies {
		if apply.ID == id {
			apply.Status = status
			apply.HandledAt = handledAt
			return nil
		}
	}
	return errors.New("not found")
}

func (f *fakeRepo) ListApplies(_ context.Context, gid int64, status int16, _, _ int) ([]*model.GroupApply, error) {
	result := make([]*model.GroupApply, 0)
	for _, apply := range f.applies {
		if apply.GroupID == gid && apply.Status == status {
			result = append(result, apply)
		}
	}
	return result, nil
}

func (f *fakeRepo) Search(_ context.Context, keyword string, limit int) ([]*model.Group, error) {
	result := make([]*model.Group, 0)
	for _, group := range f.groups {
		if len(result) < limit && containsFold(group.Name, keyword) {
			result = append(result, group)
		}
	}
	return result, nil
}

func containsFold(value, sub string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(sub))
}
