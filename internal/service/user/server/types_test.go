package server

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeRepo struct {
	users map[int64]*model.User
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{users: map[int64]*model.User{
		1: {ID: 1, Username: "alice", Nickname: "Alice", FaceURL: "a", Gender: 1, Status: 0},
		2: {ID: 2, Username: "bob", Nickname: "Bob", Status: 0},
	}}
}

func (f *fakeRepo) FindByID(_ context.Context, uid int64) (*model.User, error) {
	if user, ok := f.users[uid]; ok {
		return user, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeRepo) FindByIDs(_ context.Context, uids []int64) ([]*model.User, error) {
	users := make([]*model.User, 0, len(uids))
	for _, uid := range uids {
		if user, ok := f.users[uid]; ok {
			users = append(users, user)
		}
	}
	return users, nil
}

func (f *fakeRepo) Search(_ context.Context, keyword string, limit int) ([]*model.User, error) {
	users := make([]*model.User, 0, limit)
	for _, user := range f.users {
		if user.Username == keyword || user.Nickname == keyword {
			users = append(users, user)
		}
	}
	return users, nil
}

func (f *fakeRepo) UpdateProfile(_ context.Context, uid int64, nickname, faceURL string, gender int16, _ int64) error {
	user, ok := f.users[uid]
	if !ok {
		return pgx.ErrNoRows
	}
	user.Nickname = nickname
	user.FaceURL = faceURL
	user.Gender = gender
	return nil
}
