package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestSendQuote(t *testing.T) {
	service, repo, origin := seedMessage(t)

	result, err := service.SendQuote(context.Background(), 1, &QuoteRequest{
		ClientMsgID: "quote-1",
		RecvID:      2,
		ServerMsgID: origin.ServerMsgID,
		Content:     `{"text":"nice one"}`,
	})
	if err != nil {
		t.Fatalf("SendQuote: %v", err)
	}
	stored, err := repo.FindOne(context.Background(), result.ServerMsgID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	var payload struct {
		Text  string `json:"text"`
		Quote struct {
			ServerMsgID string `json:"server_msg_id"`
			SendID      int64  `json:"send_id"`
			Text        string `json:"text"`
		} `json:"quote"`
	}
	if err = json.Unmarshal([]byte(stored.Content), &payload); err != nil {
		t.Fatalf("content json: %v (%s)", err, stored.Content)
	}
	if payload.Text != "nice one" {
		t.Fatalf("text = %q", payload.Text)
	}
	if payload.Quote.ServerMsgID != origin.ServerMsgID || payload.Quote.SendID != 1 {
		t.Fatalf("quote = %+v", payload.Quote)
	}
	if payload.Quote.Text != "before" && payload.Quote.Text != `{"text":"before"}` {
		t.Fatalf("quote text = %q", payload.Quote.Text)
	}
}

func TestSendQuoteValidation(t *testing.T) {
	service, _, origin := seedMessage(t)
	ctx := context.Background()

	if _, err := service.SendQuote(ctx, 1, nil); !errors.Is(err, errs.ErrMessageEmpty) {
		t.Fatalf("nil request err = %v", err)
	}
	if _, err := service.SendQuote(ctx, 1, &QuoteRequest{ServerMsgID: origin.ServerMsgID}); !errors.Is(err, errs.ErrMessageEmpty) {
		t.Fatalf("missing receiver err = %v", err)
	}
	if _, err := service.SendQuote(ctx, 1, &QuoteRequest{
		RecvID: 2, ServerMsgID: origin.ServerMsgID, GroupID: 99, Content: `{"text":"x"}`,
	}); !errors.Is(err, errs.ErrConversationNotExist) {
		t.Fatalf("group mismatch err = %v", err)
	}
	if _, err := service.SendQuote(ctx, 3, &QuoteRequest{
		RecvID: 1, ServerMsgID: origin.ServerMsgID, Content: `{"text":"x"}`,
	}); !errors.Is(err, errs.ErrConversationNotExist) {
		t.Fatalf("non participant err = %v", err)
	}
}
