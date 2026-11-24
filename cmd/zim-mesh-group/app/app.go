// Package app assembles the group mesh application.
package app

import (
	"github.com/dobyte/due/v2/cluster/mesh"

	changelog "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	groupsvc "github.com/zciyuan/ZIM/internal/service/group/server"
	groupversion "github.com/zciyuan/ZIM/internal/service/groupversion/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
)

// Init registers the group mesh service provider.
func Init(proxy *mesh.Proxy, repo groupsvc.Repository, changes *changelog.Service,
	conv *conversationsvc.Service, versions *groupversion.Service, members *groupsvc.MemberCache,
	notifier webhook.Notifier, statusPub groupsvc.MemberStatusPublisher) {
	server := groupsvc.NewServer(proxy, repo)
	server.SetChangeLog(changes)
	server.SetConversation(conv)
	server.SetGroupVersion(versions)
	server.SetMemberCache(members)
	server.SetWebhook(notifier)
	server.SetMemberStatusPublisher(statusPub)
	server.Init()
}
