package account

import "github.com/zciyuan/ZIM/internal/model"

type fakeRepo struct {
	user      *model.User
	err       error
	created   *model.User
	createErr error
	nextID    int64
	nextErr   error
}
