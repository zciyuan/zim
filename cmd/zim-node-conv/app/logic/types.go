package logic

import (
	"github.com/dobyte/due/v2/cluster/node"

	changelog "github.com/zciyuan/ZIM/internal/service/changelog/server"
	conversation "github.com/zciyuan/ZIM/internal/service/conversation/server"
)

// core owns the conversation routes.
type core struct {
	proxy   *node.Proxy
	svc     *conversation.Service
	changes *changelog.Service
}
