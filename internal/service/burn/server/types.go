package burn

import "github.com/dobyte/due/v2/encoding/json"

// burnMessage is the sweep row shape.
type burnMessage struct {
	ServerMsgID    string
	ConversationID string
	Seq            int64
	SendID         int64
	RecvID         int64
	MsgType        int32
	Content        string
}

// objectKey extracts the uploaded object key from a media message payload
// (content JSON carries url = "/v1/files/{key}", ZIM-02 §3.4). Empty for
// non-media content without a URL.
func objectKey(content string) string {
	if content == "" {
		return ""
	}
	var payload struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return ""
	}
	const prefix = "/v1/files/"
	if len(payload.URL) > len(prefix) && payload.URL[:len(prefix)] == prefix {
		return payload.URL[len(prefix):]
	}
	return ""
}
