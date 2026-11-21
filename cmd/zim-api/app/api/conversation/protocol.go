package conversation

import (
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
)

type listRes struct {
	Items []*conversationsvc.Item `json:"items"`
}

type readReq struct {
	Seq int64 `json:"seq"`
}

type readRes struct {
	ReadSeq int64 `json:"read_seq"`
}

type unreadRes struct {
	Unread int64 `json:"unread"`
}

type pinReq struct {
	Pinned bool `json:"pinned"`
}

type operationRes struct {
	OK bool `json:"ok"`
}

type messageRes struct {
	ServerMsgID string   `json:"server_msg_id"`
	ClientMsgID string   `json:"client_msg_id"`
	Seq         int64    `json:"seq"`
	SendID      int64    `json:"send_id"`
	RecvID      int64    `json:"recv_id"`
	GroupID     int64    `json:"group_id"`
	MsgType     int32    `json:"msg_type"`
	Content     string   `json:"content"`
	AtUserIDs   []string `json:"at_user_ids,omitempty"`
	SendTime    int64    `json:"send_time"`
	Status      int32    `json:"status"`
}

type messagesRes struct {
	Messages []*messageRes `json:"messages"`
	HasMore  bool          `json:"has_more"`
}

type clearRes struct {
	MinSeq int64 `json:"min_seq"`
}

type searchItem struct {
	ServerMsgID    string `json:"server_msg_id"`
	ConversationID string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SendID         int64  `json:"send_id"`
	RecvID         int64  `json:"recv_id"`
	GroupID        int64  `json:"group_id"`
	MsgType        int32  `json:"msg_type"`
	Content        string `json:"content"`
	SendTime       int64  `json:"send_time"`
	Status         int32  `json:"status"`
}
