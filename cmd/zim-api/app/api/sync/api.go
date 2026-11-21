package sync

import (
	changelogsvc "github.com/zciyuan/ZIM/internal/service/changelog/server"
)

// API is the sync REST module.
type API struct {
	changes *changelogsvc.Service
}
