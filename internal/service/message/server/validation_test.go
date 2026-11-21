package message

import (
	"context"
	"errors"
	"testing"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestSendValidation(t *testing.T) {
	service, _, _, _, _ := newTestService(&fakeDir{friend: true})
	cases := []*SendRequest{
		nil,
		{ClientMsgID: "", ReceiverID: 2, MsgType: 1, Content: "x"},
		{ClientMsgID: "c", ReceiverID: 2, MsgType: 1, Content: ""},
		{ClientMsgID: "c", ReceiverID: 2, MsgType: 99, Content: "x"},
		{ClientMsgID: "c", ReceiverID: 1, MsgType: 1, Content: "x"},
	}
	for _, req := range cases {
		if _, err := service.SendSingle(context.Background(), 1, req); err == nil {
			t.Fatalf("request %+v must fail", req)
		}
	}
}

func TestSendGroupNotMember(t *testing.T) {
	service, _, _, _, _ := newTestService(&fakeDir{membership: nil})
	_, err := service.SendGroup(context.Background(), 1, &SendRequest{
		ClientMsgID: "g1", GroupID: 3, MsgType: 1, Content: "x",
	})
	if !errors.Is(err, errs.ErrNotGroupMember) {
		t.Fatalf("err = %v", err)
	}
}

func TestRecallByOtherUserRejected(t *testing.T) {
	service, _, _, _, _ := newTestService(&fakeDir{friend: true})
	result, err := service.SendSingle(context.Background(), 1, &SendRequest{
		ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Recall(context.Background(), 2, result.ServerMsgID); !errors.Is(err, errs.ErrNotMessageSender) {
		t.Fatalf("err = %v", err)
	}
}

func TestPullValidation(t *testing.T) {
	service, _, _, _, _ := newTestService(&fakeDir{})
	if _, _, err := service.Pull(context.Background(), "", 1, 2, 10); err == nil {
		t.Fatal("empty conversation must fail")
	}
	if _, _, err := service.Pull(context.Background(), "s_1_2", 5, 2, 10); err == nil {
		t.Fatal("reversed range must fail")
	}
}
