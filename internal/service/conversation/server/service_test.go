package conversation

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func newTestService() (*Service, *fakeRepo) {
	repo := newFakeRepo()
	return New(repo), repo
}

func TestListUnread(t *testing.T) {
	service, repo := newTestService()
	_ = repo.Create(context.Background(), &model.Conversation{
		OwnerID: 1, ConvID: "s_1_2", Type: model.ConversationSingle, LastMsgSeq: 5,
	})
	_ = repo.SetReadSeq(context.Background(), 1, "s_1_2", 2)
	items, err := service.List(context.Background(), 1, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Unread != 3 || items[0].MaxSeq != 5 {
		t.Fatalf("items = %+v", items)
	}
}

func TestReadClampsToMax(t *testing.T) {
	service, repo := newTestService()
	_ = repo.Create(context.Background(), &model.Conversation{OwnerID: 1, ConvID: "s_1_2", LastMsgSeq: 5})
	seq, err := service.Read(context.Background(), 1, "s_1_2", 99)
	if err != nil || seq != 5 {
		t.Fatalf("seq = %d, err = %v", seq, err)
	}
	unread, err := service.Unread(context.Background(), 1, "s_1_2")
	if err != nil || unread != 0 {
		t.Fatalf("unread = %d, err = %v", unread, err)
	}
}

func TestPinDeleteAndErrors(t *testing.T) {
	service, repo := newTestService()
	_ = repo.Create(context.Background(), &model.Conversation{OwnerID: 1, ConvID: "s_1_2", LastMsgSeq: 5})
	if err := service.Pin(context.Background(), 1, "s_1_2", true); err != nil {
		t.Fatal(err)
	}
	if err := service.SetRecvOpt(context.Background(), 1, "s_1_2", 1); err != nil {
		t.Fatal(err)
	}
	if err := service.Delete(context.Background(), 1, "s_1_2"); err != nil {
		t.Fatal(err)
	}
	if err := service.Delete(context.Background(), 1, "missing"); !errors.Is(err, errs.ErrConversationNotExist) {
		t.Fatalf("err = %v", err)
	}
}

func TestCreateDirect(t *testing.T) {
	service, repo := newTestService()
	if err := service.CreateDirect(context.Background(), 1, 2, "s_1_2"); err != nil {
		t.Fatal(err)
	}
	items, _ := repo.List(context.Background(), 1, 0, 10)
	if len(items) != 1 || items[0].PeerID != 2 {
		t.Fatalf("items = %+v", items)
	}
	items, _ = repo.List(context.Background(), 2, 0, 10)
	if len(items) != 1 || items[0].PeerID != 1 {
		t.Fatalf("items = %+v", items)
	}
}

func TestVisibleRangeEnforcesOwnerAndMinSeq(t *testing.T) {
	service, repo := newTestService()
	_ = repo.Create(context.Background(), &model.Conversation{
		OwnerID: 1, ConvID: "g_1", Type: model.ConversationGroup, LastMsgSeq: 100,
	})
	repo.seqUsers[repo.key(1, "g_1")] = &model.SeqUser{UID: 1, ConversationID: "g_1", MinSeq: 50}

	min, max, err := service.VisibleRange(context.Background(), 1, "g_1")
	if err != nil {
		t.Fatal(err)
	}
	if min != 50 || max != 100 {
		t.Fatalf("range = [%d,%d], want [50,100]", min, max)
	}

	if _, _, err = service.VisibleRange(context.Background(), 2, "g_1"); !errors.Is(err, errs.ErrNotGroupMember) {
		t.Fatalf("non-member err = %v", err)
	}
	if _, _, err = service.VisibleRange(context.Background(), 2, "s_1_2"); !errors.Is(err, errs.ErrConversationNotExist) {
		t.Fatalf("single err = %v", err)
	}
}

func TestListNoReadStateDoesNotShowWholeSequence(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), &model.Conversation{
		OwnerID: 1, ConvID: "s_1_2", LastMsgSeq: 100,
	})
	service := New(repo)
	items, err := service.List(context.Background(), 1, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Unread != 1 {
		t.Fatalf("items = %+v", items)
	}
}

func TestReadPersistsImmediately(t *testing.T) {
	repo := newFakeRepo()
	service := New(repo)
	_ = repo.Create(context.Background(), &model.Conversation{
		OwnerID: 1, ConvID: "s_1_2", LastMsgSeq: 10,
	})
	if _, err := service.Read(context.Background(), 1, "s_1_2", 7); err != nil {
		t.Fatal(err)
	}
	state, err := repo.GetSeqUser(context.Background(), 1, "s_1_2")
	if err != nil || state.ReadSeq != 7 {
		t.Fatalf("read_seq = %d, err = %v", state.ReadSeq, err)
	}
	conv, _ := repo.Get(context.Background(), 1, "s_1_2")
	if conv.ReadSeq != 7 {
		t.Fatalf("conversation read_seq = %d", conv.ReadSeq)
	}
}

func TestFlushAggregatesMultipleUsers(t *testing.T) {
	repo := newFakeRepo()
	cache := &fakeReadCache{values: map[string]int64{}}
	service := New(repo, cache)
	for uid := int64(1); uid <= 3; uid++ {
		convID := "s_" + strconv.FormatInt(uid, 10)
		_ = repo.Create(context.Background(), &model.Conversation{
			OwnerID: uid, ConvID: convID, LastMsgSeq: 10,
		})
		if _, err := service.Read(context.Background(), uid, convID, 9); err != nil {
			t.Fatal(err)
		}
	}
	service.Flush(context.Background())
	if repo.batches != 1 || repo.lastSize != 3 {
		t.Fatalf("batches = %d, size = %d, want 1/3", repo.batches, repo.lastSize)
	}
}
