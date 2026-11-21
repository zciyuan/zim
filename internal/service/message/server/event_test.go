package message

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/zimfw/pipe"
)

type fakeProducer struct {
	last pipe.Message
	err  error
}

func (f *fakeProducer) Publish(_ context.Context, message pipe.Message) error {
	f.last = message
	return f.err
}

func (f *fakeProducer) Close() error { return nil }

func TestKafkaPublisherRoundTrip(t *testing.T) {
	producer := &fakeProducer{}
	publisher := NewKafkaPublisher(producer, "zim.pipe.toPush")
	event := &Event{
		ServerMsgID: "1", ClientMsgID: "c1", ConversationID: "s_1_2",
		Seq: 3, SendID: 1, RecvID: 2, MsgType: 1, Content: "hi", SendTime: 100,
		Recipients: []int64{1, 2},
	}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if producer.last.Topic != "zim.pipe.toPush" || producer.last.Key != "s_1_2" {
		t.Fatalf("message = %+v", producer.last)
	}
	decoded, err := DecodeEvent(producer.last.Value)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Seq != 3 || len(decoded.Recipients) != 2 {
		t.Fatalf("decoded = %+v", decoded)
	}
}
