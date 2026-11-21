package message

import (
	"context"
	"strings"

	duejson "github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

const (
	// mergeMaxItems bounds how many messages one merged card can carry.
	mergeMaxItems = 50
)

// MergeRequest is one op:1503 merged forward.
type MergeRequest struct {
	ClientMsgID  string
	RecvID       int64
	GroupID      int64
	ServerMsgIDs []string
	Title        string
}

// mergedItem is one row inside the merged card.
type mergedItem struct {
	ServerMsgID string `json:"server_msg_id"`
	SendID      int64  `json:"send_id"`
	MsgType     int32  `json:"msg_type"`
	Text        string `json:"text"`
}

type mergedCard struct {
	Title string       `json:"title"`
	Items []mergedItem `json:"items"`
}

// SendMerge validates the referenced messages and sends one merged card.
//
// Every referenced message must be readable by the sender and belong to the
// target conversation, so a merged card cannot leak another chat's content.
func (s *Service) SendMerge(ctx context.Context, sender int64, req *MergeRequest) (*SendResult, error) {
	if req == nil || len(req.ServerMsgIDs) == 0 {
		return nil, errs.ErrMessageEmpty
	}
	if len(req.ServerMsgIDs) > mergeMaxItems {
		return nil, errs.ErrMessageTooLong
	}
	target := &SendRequest{ClientMsgID: req.ClientMsgID, MsgType: 1}
	if req.GroupID > 0 {
		target.GroupID = req.GroupID
	} else {
		if req.RecvID <= 0 {
			return nil, errs.ErrMessageEmpty
		}
		target.ReceiverID = req.RecvID
	}

	items := make([]mergedItem, 0, len(req.ServerMsgIDs))
	seen := make(map[string]struct{}, len(req.ServerMsgIDs))
	for _, id := range req.ServerMsgIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, errs.ErrMessageEmpty
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		msg, err := s.repo.FindOne(ctx, id)
		if err != nil {
			return nil, errs.ErrInternal.WithCause(err)
		}
		if err = s.Access(ctx, sender, msg); err != nil {
			return nil, err
		}
		// Access() above already proves the sender can read the message; the
		// card may be sent to a different conversation on purpose (forwarding).
		items = append(items, mergedItem{
			ServerMsgID: msg.ServerMsgID,
			SendID:      msg.SendID,
			MsgType:     msg.MsgType,
			Text:        briefText(msg),
		})
	}
	if len(items) == 0 {
		return nil, errs.ErrMessageEmpty
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "聊天记录"
	}
	body, err := duejson.Marshal(struct {
		Text  string     `json:"text"`
		Merge mergedCard `json:"merge"`
	}{
		Text:  "[聊天记录] " + title,
		Merge: mergedCard{Title: title, Items: items},
	})
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	target.Content = string(body)

	if req.GroupID > 0 {
		return s.SendGroup(ctx, sender, target)
	}
	return s.SendSingle(ctx, sender, target)
}
