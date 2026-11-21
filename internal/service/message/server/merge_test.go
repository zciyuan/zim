package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestSendMerge(t *testing.T) {
	service, repo, first := seedMessage(t)
	second, err := service.SendSingle(context.Background(), 1, &SendRequest{
		ClientMsgID: "merge-2", ReceiverID: 2, MsgType: 1, Content: `{"text":"second"}`,
	})
	if err != nil {
		t.Fatalf("SendSingle: %v", err)
	}

	result, err := service.SendMerge(context.Background(), 1, &MergeRequest{
		ClientMsgID:  "merge-card",
		RecvID:       2,
		ServerMsgIDs: []string{first.ServerMsgID, second.ServerMsgID},
		Title:        "聊天记录",
	})
	if err != nil {
		t.Fatalf("SendMerge: %v", err)
	}
	stored, err := repo.FindOne(context.Background(), result.ServerMsgID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	var payload struct {
		Text  string `json:"text"`
		Merge struct {
			Title string `json:"title"`
			Items []struct {
				ServerMsgID string `json:"server_msg_id"`
				Text        string `json:"text"`
			} `json:"items"`
		} `json:"merge"`
	}
	if err = json.Unmarshal([]byte(stored.Content), &payload); err != nil {
		t.Fatalf("content json: %v (%s)", err, stored.Content)
	}
	if len(payload.Merge.Items) != 2 {
		t.Fatalf("items = %+v", payload.Merge.Items)
	}
	if payload.Merge.Items[0].ServerMsgID != first.ServerMsgID {
		t.Fatalf("order lost: %+v", payload.Merge.Items)
	}
	if payload.Merge.Title != "聊天记录" {
		t.Fatalf("title = %q", payload.Merge.Title)
	}
}

func TestSendMergeValidation(t *testing.T) {
	service, _, origin := seedMessage(t)
	ctx := context.Background()

	if _, err := service.SendMerge(ctx, 1, nil); !errors.Is(err, errs.ErrMessageEmpty) {
		t.Fatalf("nil request err = %v", err)
	}
	if _, err := service.SendMerge(ctx, 1, &MergeRequest{RecvID: 2}); !errors.Is(err, errs.ErrMessageEmpty) {
		t.Fatalf("empty ids err = %v", err)
	}
	tooMany := make([]string, mergeMaxItems+1)
	for i := range tooMany {
		tooMany[i] = origin.ServerMsgID
	}
	if _, err := service.SendMerge(ctx, 1, &MergeRequest{RecvID: 2, ServerMsgIDs: tooMany}); !errors.Is(err, errs.ErrMessageTooLong) {
		t.Fatalf("too many err = %v", err)
	}
	// Forwarding across conversations is the feature: a merged card may carry
	// messages from another chat as long as the sender can read them.
	if _, err := service.SendMerge(ctx, 1, &MergeRequest{
		ClientMsgID: "merge-cross", RecvID: 2, ServerMsgIDs: []string{origin.ServerMsgID},
	}); err != nil {
		t.Fatalf("cross-conversation merge err = %v", err)
	}
}
