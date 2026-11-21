package server

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/localcache"
	"github.com/zciyuan/ZIM/internal/model"
	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

// Server is the user mesh service provider.
type Server struct {
	pb.UnimplementedUserServer
	proxy    *mesh.Proxy
	repo     Repository
	redis    redis.UniversalClient
	profiles *localcache.Cache[int64, *model.User]
}

type getProfileArgs struct {
	UID int64
}

type batchGetUsersArgs struct {
	UIDs []int64
}

type updateProfileArgs struct {
	UID      int64
	Nickname string
	Avatar   string
	Gender   int32
}

type searchUsersArgs struct {
	Keyword string
	Limit   int32
}
