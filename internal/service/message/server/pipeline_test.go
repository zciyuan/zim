package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func TestPipelineSend(t *testing.T) {
	producer := &fakeProducer{}
	queue := &fakeResultQueue{result: &SendResult{ServerMsgID: "s1", Seq: 9, SendTime: 100}}
	pipeline := &Pipeline{
		producer: producer, results: queue, timeout: time.Second,
		now: func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	req := &SendRequest{ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: "x"}
	result, err := pipeline.send(context.Background(), 1,
		&Prepared{ConversationID: "s_1_2", Recipients: []int64{1, 2}, UnreadCount: 1}, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Seq != 9 || result.UnreadCount != 1 || len(result.Recipients) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if producer.last.Topic != "zim.pipe.toRedis" || producer.last.Key != "s_1_2" {
		t.Fatalf("message = %+v", producer.last)
	}
	if queue.uid != 1 || queue.client != "c1" {
		t.Fatalf("queue = %d/%s", queue.uid, queue.client)
	}
}

func TestPipelineSendErrors(t *testing.T) {
	failing := &fakeProducer{err: errs.ErrServerBusy}
	pipeline := &Pipeline{producer: failing, results: &fakeResultQueue{}, timeout: time.Second}
	if _, err := pipeline.send(context.Background(), 1,
		&Prepared{ConversationID: "s_1_2"}, &SendRequest{ClientMsgID: "c1"}); err == nil {
		t.Fatal("publish error must fail")
	}
	queueErr := &fakeResultQueue{err: errs.ErrServerBusy}
	pipeline = &Pipeline{producer: &fakeProducer{}, results: queueErr, timeout: time.Second}
	if _, err := pipeline.send(context.Background(), 1,
		&Prepared{ConversationID: "s_1_2"}, &SendRequest{ClientMsgID: "c1"}); !errors.Is(err, errs.ErrServerBusy) {
		t.Fatalf("wait err = %v", err)
	}
}

func TestNewPipelineValidation(t *testing.T) {
	if _, err := NewPipeline(nil, &fakeProducer{}, &fakeResultQueue{}, time.Second); err == nil {
		t.Fatal("nil service must fail")
	}
	if _, err := NewPipeline(&Service{}, nil, &fakeResultQueue{}, time.Second); err == nil {
		t.Fatal("nil producer must fail")
	}
	if _, err := NewPipeline(&Service{}, &fakeProducer{}, nil, time.Second); err == nil {
		t.Fatal("nil results must fail")
	}
}

func TestPipelineSendSingleAndGroup(t *testing.T) {
	dir := &fakeDir{friend: true, membership: &Membership{GroupID: 3, MemberIDs: []int64{1, 2, 3}}}
	svc := &Service{dir: dir, now: func() time.Time { return time.Unix(1_700_000_000, 0) }}
	queue := &fakeResultQueue{result: &SendResult{ServerMsgID: "s", Seq: 1}}
	pipeline := &Pipeline{service: svc, producer: &fakeProducer{}, results: queue,
		timeout: time.Second, now: func() time.Time { return time.Unix(1_700_000_000, 0) }}
	if _, err := pipeline.SendSingle(context.Background(), 1,
		&SendRequest{ClientMsgID: "c1", ReceiverID: 2, MsgType: 1, Content: "x"}); err != nil {
		t.Fatal(err)
	}
	if queue.client != "c1" {
		t.Fatalf("client = %s", queue.client)
	}
	if _, err := pipeline.SendGroup(context.Background(), 1,
		&SendRequest{ClientMsgID: "g1", GroupID: 3, MsgType: 1, Content: "x"}); err != nil {
		t.Fatal(err)
	}
}
