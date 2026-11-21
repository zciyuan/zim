package sync

type changeRes struct {
	ObjID   string `json:"obj_id"`
	State   string `json:"state"`
	Version int64  `json:"version"`
	Mtime   int64  `json:"mtime"`
}

type changesRes struct {
	Entity  string       `json:"entity"`
	Version int64        `json:"version"`
	Changes []*changeRes `json:"changes"`
}
