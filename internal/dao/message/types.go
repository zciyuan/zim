package message

import (
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
)

// DAO is the Mongo message DAO.
type DAO struct {
	blocks    *mongo.Collection
	idem      *mongo.Collection
	batchers  []*appendBatcher
	batchOnce sync.Once
}

// AppendResult reports the persisted message identity.
type AppendResult struct {
	ServerMsgID string
	Seq         int64
	Duplicate   bool
}

// blockDoc is one aggregate document holding up to MessageBlockSize messages.
type blockDoc struct {
	DocID          string     `bson:"doc_id"`
	ConversationID string     `bson:"conversation_id"`
	BlockIdx       int64      `bson:"block_idx"`
	Msgs           []blockMsg `bson:"msgs"`
}

type blockMsg struct {
	Msg    messageDoc `bson:"msg"`
	Revoke int32      `bson:"revoke"`
}

type messageDoc struct {
	ServerMsgID  string   `bson:"server_msg_id"`
	ClientMsgID  string   `bson:"client_msg_id"`
	SendID       int64    `bson:"send_id"`
	RecvID       int64    `bson:"recv_id"`
	GroupID      int64    `bson:"group_id"`
	MsgType      int32    `bson:"msg_type"`
	Content      string   `bson:"content"`
	SearchTokens []string `bson:"search_tokens,omitempty"`
	AtUserIDs    []string `bson:"at_user_ids,omitempty"`
	Seq          int64    `bson:"seq"`
	SendTime     int64    `bson:"send_time"`
	Status       int32    `bson:"status"`
}

type idemDoc struct {
	ID          string `bson:"_id"`
	ServerMsgID string `bson:"server_msg_id"`
	Seq         int64  `bson:"seq"`
	State       string `bson:"state"`
	UpdatedAt   int64  `bson:"updated_at"`
}

const (
	idemPending   = "pending"
	idemCommitted = "committed"
)
