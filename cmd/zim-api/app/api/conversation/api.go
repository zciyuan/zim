package conversation

import (
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	historysvc "github.com/zciyuan/ZIM/internal/service/history/server"
)

// API is the conversation REST module.
type API struct {
	service *conversationsvc.Service
	history *historysvc.Service
}
