package upload

import uploadsvc "github.com/zciyuan/ZIM/internal/service/upload/server"

// API is the upload REST module.
type API struct {
	service *uploadsvc.Service
}
