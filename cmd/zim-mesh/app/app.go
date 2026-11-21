// Package app registers every mesh service in one process.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/redis/go-redis/v9"

	account "github.com/zciyuan/ZIM/internal/service/account/server"
	authsvc "github.com/zciyuan/ZIM/internal/service/auth/server"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	groupsvc "github.com/zciyuan/ZIM/internal/service/group/server"
	groupversion "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	relationsvc "github.com/zciyuan/ZIM/internal/service/relation/server"
	usersvc "github.com/zciyuan/ZIM/internal/service/user/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
)

// Deps is the shared dependency set of the mesh process.
type Deps struct {
	Account      *account.Service
	UserRepo     usersvc.Repository
	UserCache    redis.UniversalClient
	FriendRepo   relationsvc.Repository
	FriendCache  redis.UniversalClient
	GroupRepo    groupsvc.Repository
	ChangeLog    *changelogsvc.Service
	Conversation *conversationsvc.Service
	GroupVersion *groupversion.Service
	MemberCache  *groupsvc.MemberCache
	Webhook      webhook.Notifier
}

// Init registers all mesh providers on one proxy.
func Init(proxy *mesh.Proxy, deps Deps) {
	authServer := authsvc.NewServer(proxy)
	authServer.SetAccount(deps.Account)
	authServer.Init()
	userServer := usersvc.NewServer(proxy, deps.UserRepo)
	userServer.SetCache(deps.UserCache)
	userServer.Init()

	relationServer := relationsvc.NewServer(proxy, deps.FriendRepo)
	relationServer.SetCache(deps.FriendCache)
	relationServer.SetChangeLog(deps.ChangeLog)
	relationServer.SetWebhook(deps.Webhook)
	relationServer.Init()

	groupServer := groupsvc.NewServer(proxy, deps.GroupRepo)
	groupServer.SetChangeLog(deps.ChangeLog)
	groupServer.SetConversation(deps.Conversation)
	groupServer.SetGroupVersion(deps.GroupVersion)
	groupServer.SetMemberCache(deps.MemberCache)
	groupServer.SetWebhook(deps.Webhook)
	groupServer.Init()
}
