// Package app assembles the conversation node application.
package app

import (
	"github.com/dobyte/due/v2/cluster/node"

	"github.com/zciyuan/ZIM/cmd/zim-node-conv/app/logic"
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversation "github.com/zciyuan/ZIM/internal/service/conversation/server"
)

// Init registers conversation routes.
func Init(proxy *node.Proxy, svc *conversation.Service, changes *changelogsvc.Service) {
	logic.NewCore(proxy, svc, changes).Init()
}
