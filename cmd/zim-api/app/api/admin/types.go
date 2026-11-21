package admin

// Permission is one admin permission point.
type Permission struct {
	Domain string `json:"domain"`
	Code   string `json:"code"`
}

// Permissions returns the ZIM-01 §25.4 admin permission points.
func Permissions() []Permission {
	return []Permission{
		{"user", "zim:user:list"}, {"user", "zim:user:ban"}, {"user", "zim:user:kick"}, {"user", "zim:user:reset_pwd"},
		{"group", "zim:group:list"}, {"group", "zim:group:dismiss"}, {"group", "zim:group:transfer"}, {"group", "zim:group:mute"},
		{"family", "zim:family:list"}, {"family", "zim:family:dismiss"},
		{"message", "zim:msg:query"}, {"message", "zim:msg:recall"}, {"message", "zim:msg:delete"},
		{"sensitive", "zim:sensitive:list"}, {"sensitive", "zim:sensitive:write"},
		{"push", "zim:push:cert"}, {"push", "zim:push:test"},
		{"config", "zim:config:read"}, {"config", "zim:config:write"},
		{"audit", "zim:audit:report"}, {"audit", "zim:audit:content"},
		{"oplog", "zim:oplog:list"},
	}
}

// userItem is one admin user row.
type userItem struct {
	UID       int64  `json:"uid"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	Avatar    string `json:"avatar"`
	Gender    int32  `json:"gender"`
	Status    int32  `json:"status"`
	CreatedAt int64  `json:"created_at"`
}

type banUserReq struct {
	Banned bool `json:"banned"`
}

type groupItem struct {
	GID       int64  `json:"gid"`
	Name      string `json:"name"`
	Avatar    string `json:"avatar"`
	OwnerUID  int64  `json:"owner_uid"`
	GroupType int32  `json:"group_type"`
	CreatedAt int64  `json:"created_at"`
}

type messageItem struct {
	ServerMsgID    string `json:"server_msg_id"`
	ClientMsgID    string `json:"client_msg_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SendID         int64  `json:"send_id"`
	RecvID         int64  `json:"recv_id"`
	GroupID        int64  `json:"group_id"`
	MsgType        int32  `json:"msg_type"`
	Content        string `json:"content"`
	SendTime       int64  `json:"send_time"`
}

type settingReq struct {
	Value string `json:"value"`
}

type sensitiveWordReq struct {
	Word   string `json:"word"`
	Action int16  `json:"action"`
}

type reportReq struct {
	ReporterID int64  `json:"reporter_id"`
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Reason     string `json:"reason"`
}

type handleReportReq struct {
	Status int16 `json:"status"`
}

type pushCertReq struct {
	Cert string `json:"cert"`
}

type notificationReq struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type searchableReq struct {
	Searchable bool `json:"searchable"`
}
