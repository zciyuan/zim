package relation

import (
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	userpb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

type friendRes struct {
	FriendID  int64               `json:"friend_id"`
	Remark    string              `json:"remark"`
	Pinned    int32               `json:"pinned"`
	CreatedAt int64               `json:"created_at"`
	Profile   *userpb.UserProfile `json:"profile,omitempty"`
}

type friendsRes struct {
	Friends []*friendRes `json:"friends"`
}

type requestsRes struct {
	Requests []*pb.FriendRequestItem `json:"requests"`
}

type blacklistRes struct {
	Users []*userpb.UserProfile `json:"users"`
}

type addFriendReq struct {
	FriendID int64  `json:"friend_id"`
	GroupID  int64  `json:"group_id"`
	Why      string `json:"why"`
}

type answerFriendReq struct {
	FriendID int64 `json:"friend_id"`
	Accept   bool  `json:"accept"`
}

type operationRes struct {
	OK bool `json:"ok"`
}
