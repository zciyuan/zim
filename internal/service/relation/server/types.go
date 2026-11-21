package server

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/localcache"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
	"github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
)

// Server is the relation mesh service provider.
type Server struct {
	pb.UnimplementedRelationServer
	proxy   *mesh.Proxy
	repo    Repository
	ids     *snowflake.Generator
	changes *changelog.Service
	webhook webhook.Notifier
	redis   redis.UniversalClient
	friends *localcache.Cache[int64, []*model.Friend]
}

type sendFriendRequestArgs struct {
	FromID int64
	ToID   int64
	Why    string
}

type answerFriendArgs struct {
	FromID int64
	ToID   int64
	Accept bool
}

type listFriendRequestsArgs struct {
	UID     int64
	Inbound bool
}

type deleteFriendArgs struct {
	UID      int64
	FriendID int64
}

type listFriendsArgs struct {
	UID int64
}

type updateRemarkArgs struct {
	UID      int64
	FriendID int64
	Remark   string
}

type pinFriendArgs struct {
	UID      int64
	FriendID int64
	Pinned   bool
}

type addBlackArgs struct {
	UID     int64
	BlackID int64
	Remove  bool
}

type listBlacklistArgs struct {
	UID int64
}

type isFriendArgs struct {
	UID      int64
	TargetID int64
}
