package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/idem"
)

func newTestService(dir *fakeDir) (*Service, *fakeRepo, *fakeConv, *fakeSeq, *time.Time) {
	repo := &fakeRepo{}
	seq := &fakeSeq{}
	conv := &fakeConv{}
	now := time.Unix(1_700_000_000, 0)
	service := &Service{
		repo: repo, seq: seq,
		idem: &fakeIdem{results: make(map[string]*idem.Result)},
		conv: conv, dir: dir,
		ids: snowflake.NewWithClock(1, func() time.Time { return now }), now: func() time.Time { return now },
	}
	return service, repo, conv, seq, &now
}

func TestSendSingleAndIdempotency(t *testing.T) {
	service, _, conv, _, _ := newTestService(&fakeDir{friend: true})
	req := &SendRequest{ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: `{"text":"hi"}`}

	first, err := service.SendSingle(context.Background(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || first.ServerMsgID == "" {
		t.Fatalf("first = %+v", first)
	}
	if len(conv.touch) != 2 {
		t.Fatalf("touches = %d, want 2", len(conv.touch))
	}
	if conv.touch[0].PeerID != 2 || conv.touch[1].PeerID != 1 {
		t.Fatalf("peer ids = %d, %d", conv.touch[0].PeerID, conv.touch[1].PeerID)
	}

	second, err := service.SendSingle(context.Background(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.Seq != 1 || second.ServerMsgID != first.ServerMsgID {
		t.Fatalf("second = %+v", second)
	}
	if len(conv.touch) != 2 {
		t.Fatalf("duplicate touched conversations")
	}
}

func TestSendSingleRejectsBlack(t *testing.T) {
	service, _, _, _, _ := newTestService(&fakeDir{black: true})
	_, err := service.SendSingle(context.Background(), 1, &SendRequest{
		ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: "x",
	})
	if !errors.Is(err, errs.ErrBlockedByPeer) {
		t.Fatalf("err = %v", err)
	}
}

func TestSendGroupMuteAndUnread(t *testing.T) {
	dir := &fakeDir{membership: &Membership{
		GroupID: 10, GroupMute: time.Now().Add(time.Hour).UnixMilli(),
		MemberIDs: []int64{1, 2, 3},
	}}
	service, _, _, _, _ := newTestService(dir)
	req := &SendRequest{ClientMsgID: "g1", GroupID: 10, MsgType: 1, Content: "x"}
	if _, err := service.SendGroup(context.Background(), 1, req); !errors.Is(err, errs.ErrMemberMuted) {
		t.Fatalf("mute err = %v", err)
	}

	dir.membership.GroupMute = 0
	result, err := service.SendGroup(context.Background(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.UnreadCount != 3 {
		t.Fatalf("unread = %d", result.UnreadCount)
	}
}

func TestPullAndRecall(t *testing.T) {
	service, _, _, _, now := newTestService(&fakeDir{friend: true})
	req := &SendRequest{ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: "x"}
	result, err := service.SendSingle(context.Background(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	messages, hasMore, err := service.Pull(context.Background(), "s_1_2", 1, result.Seq, 10)
	if err != nil || len(messages) != 1 || hasMore {
		t.Fatalf("pull = %d, %v, %v", len(messages), hasMore, err)
	}
	*now = now.Add(3 * time.Minute)
	if err = service.Recall(context.Background(), 1, result.ServerMsgID); !errors.Is(err, errs.ErrRecallTimeout) {
		t.Fatalf("recall err = %v", err)
	}
}
