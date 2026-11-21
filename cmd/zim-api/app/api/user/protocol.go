package user

import (
	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

type batchRes struct {
	Users []*pb.UserProfile `json:"users"`
}

type updateReq struct {
	Nickname string `json:"nickname"`
	Avatar   string `json:"avatar"`
	Gender   int32  `json:"gender"`
}
