package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// seedMessage stores one message through the normal send path.
func seedMessage(t *testing.T) (*Service, *fakeRepo, *model.Message) {
	t.Helper()
	service, repo, _, _, _ := newTestService(&fakeDir{friend: true})
	result, err := service.SendSingle(context.Background(), 1, &SendRequest{
		ClientMsgID: "edit-1", ReceiverID: 2, MsgType: 1, Content: `{"text":"before"}`,
	})
	if err != nil {
		t.Fatalf("SendSingle: %v", err)
	}
	stored, err := repo.FindOne(context.Background(), result.ServerMsgID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	return service, repo, stored
}

func TestEditMessage(t *testing.T) {
	service, repo, stored := seedMessage(t)

	updated, err := service.Edit(context.Background(), 1, stored.ServerMsgID, `{"text":"after"}`)
	if err != nil {
		t.Fatalf("Edit: %v", err)
	}
	if updated.Content != `{"text":"after"}` {
		t.Fatalf("returned content = %q", updated.Content)
	}
	persisted, _ := repo.FindOne(context.Background(), stored.ServerMsgID)
	if persisted.Content != `{"text":"after"}` {
		t.Fatalf("persisted content = %q", persisted.Content)
	}
}

func TestEditRejectsNonSenderAndEmpty(t *testing.T) {
	service, _, stored := seedMessage(t)

	if _, err := service.Edit(context.Background(), 2, stored.ServerMsgID, `{"text":"x"}`); !errors.Is(err, errs.ErrNotMessageSender) {
		t.Fatalf("non-sender err = %v", err)
	}
	if _, err := service.Edit(context.Background(), 1, stored.ServerMsgID, "   "); !errors.Is(err, errs.ErrMessageEmpty) {
		t.Fatalf("empty err = %v", err)
	}
}

func TestEditRejectsExpiredWindow(t *testing.T) {
	service, _, stored := seedMessage(t)
	// Move the clock past the edit window (2 minutes).
	clock := stored.SendTime + editWindowMS + 1000
	service.now = func() time.Time { return time.UnixMilli(clock) }

	if _, err := service.Edit(context.Background(), 1, stored.ServerMsgID, `{"text":"late"}`); !errors.Is(err, errs.ErrRecallTimeout) {
		t.Fatalf("expired err = %v", err)
	}
}

func TestDeleteMessage(t *testing.T) {
	service, repo, stored := seedMessage(t)

	if _, err := service.Delete(context.Background(), 1, stored.ServerMsgID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	persisted, _ := repo.FindOne(context.Background(), stored.ServerMsgID)
	if persisted.Status != model.MessageDeleted {
		t.Fatalf("status = %d", persisted.Status)
	}
	if _, err := service.Delete(context.Background(), 2, stored.ServerMsgID); !errors.Is(err, errs.ErrNotMessageSender) {
		t.Fatalf("non-sender err = %v", err)
	}
}

func TestSearchMessages(t *testing.T) {
	service, _, _ := seedMessage(t)

	matches, err := service.Search(context.Background(), "s_1_2", "before", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(matches) != 1 || matches[0].Content != `{"text":"before"}` {
		t.Fatalf("matches = %+v", matches)
	}
	if _, err = service.Search(context.Background(), "", "before", 10); !errors.Is(err, errs.ErrConversationNotExist) {
		t.Fatalf("empty conversation err = %v", err)
	}
	if matches, err = service.Search(context.Background(), "s_1_2", "   ", 10); err != nil || len(matches) != 0 {
		t.Fatalf("blank keyword = %+v, %v", matches, err)
	}
}
