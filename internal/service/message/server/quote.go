package message

import (
	"context"
	"strings"

	duejson "github.com/dobyte/due/v2/encoding/json"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// quoteBriefRunes bounds the quoted snippet stored in the reply content.
const quoteBriefRunes = 120

// QuoteRequest is one op:1504 quote reply.
type QuoteRequest struct {
	ClientMsgID string
	RecvID      int64
	GroupID     int64
	ServerMsgID string
	// Content is the reply body; only its text is kept.
	Content string
}

// quoted is the server-generated quote block embedded in the reply content.
type quoted struct {
	ServerMsgID string `json:"server_msg_id"`
	SendID      int64  `json:"send_id"`
	MsgType     int32  `json:"msg_type"`
	Text        string `json:"text"`
}

// SendQuote validates the quoted message and sends a reply that carries its
// brief. The quote block is generated server-side so a client cannot forge the
// original author or text.
func (s *Service) SendQuote(ctx context.Context, sender int64, req *QuoteRequest) (*SendResult, error) {
	if req == nil || strings.TrimSpace(req.ServerMsgID) == "" {
		return nil, errs.ErrMessageEmpty
	}
	origin, err := s.repo.FindOne(ctx, req.ServerMsgID)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	if err = s.Access(ctx, sender, origin); err != nil {
		return nil, err
	}

	target := &SendRequest{
		ClientMsgID: req.ClientMsgID,
		MsgType:     1,
		Content:     req.Content,
	}
	if req.GroupID > 0 {
		if origin.GroupID != req.GroupID {
			return nil, errs.ErrConversationNotExist
		}
		target.GroupID = req.GroupID
	} else {
		if origin.GroupID > 0 {
			return nil, errs.ErrConversationNotExist
		}
		if req.RecvID <= 0 {
			return nil, errs.ErrMessageEmpty
		}
		target.ReceiverID = req.RecvID
	}

	text := replyText(req.Content)
	body, err := duejson.Marshal(struct {
		Text  string `json:"text"`
		Quote quoted `json:"quote"`
	}{
		Text: text,
		Quote: quoted{
			ServerMsgID: origin.ServerMsgID,
			SendID:      origin.SendID,
			MsgType:     origin.MsgType,
			Text:        briefText(origin),
		},
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

// replyText picks the reply body out of the client content JSON.
func replyText(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ""
	}
	var parsed struct {
		Text string `json:"text"`
	}
	if err := duejson.UnmarshalString(trimmed, &parsed); err == nil && parsed.Text != "" {
		return parsed.Text
	}
	return trimmed
}

// briefText renders the quote snippet for any message type.
func briefText(msg *model.Message) string {
	var text string
	if msg.MsgType == 1 {
		text = replyText(msg.Content)
	} else {
		labels := map[int32]string{
			2: "[图片]", 3: "[语音]", 4: "[视频]", 5: "[文件]",
			6: "[位置]", 7: "[自定义消息]", 20: "[语音通话]", 21: "[视频通话]", 22: "[群通话]",
		}
		text = labels[msg.MsgType]
		if text == "" {
			text = "[消息]"
		}
	}
	runes := []rune(text)
	if len(runes) > quoteBriefRunes {
		return string(runes[:quoteBriefRunes]) + "…"
	}
	return text
}
