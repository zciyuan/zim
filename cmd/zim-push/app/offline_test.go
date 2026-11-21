package app

import (
	"context"
	"testing"

	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	pushoffline "github.com/zciyuan/ZIM/internal/service/pushoffline/server"
	"github.com/zciyuan/ZIM/zimfw/multi"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

type fakeProducer struct {
	messages []pipe.Message
}

func (f *fakeProducer) Publish(_ context.Context, message pipe.Message) error {
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func TestPublishOfflineOnlyForOfflineRecipients(t *testing.T) {
	producer := &fakeProducer{}
	app := &App{offlinePublisher: pushoffline.NewPublisher(producer)}
	event := &messagesvc.Event{
		ServerMsgID: "s1", ConversationID: "g_1", Seq: 9, SendID: 1,
		MsgType: 1, Content: "x", Recipients: []int64{1, 2, 3},
	}
	// uid 2 is online on Android; uid 1 is the sender; uid 3 is fully offline.
	buckets := map[string][]int64{"gate-a": {multi.Key(2, multi.PlatformAndroid, 1)}}

	published := app.publishOffline(context.Background(), event, buckets)
	if published != 1 {
		t.Fatalf("published = %d", published)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("messages = %d", len(producer.messages))
	}
	if producer.messages[0].Key != "g_1" || producer.messages[0].Topic != "zim.pipe.toOfflinePush" {
		t.Fatalf("message = %+v", producer.messages[0])
	}
	decoded, err := pushoffline.Decode(producer.messages[0].Value)
	if err != nil || len(decoded.Uids) != 1 || decoded.Uids[0] != 3 || decoded.Seq != 9 {
		t.Fatalf("decoded = %+v, err = %v", decoded, err)
	}
}

func TestPublishOfflineWithoutPublisher(t *testing.T) {
	app := &App{}
	if got := app.publishOffline(context.Background(), &messagesvc.Event{Recipients: []int64{2}}, nil); got != 0 {
		t.Fatalf("published = %d", got)
	}
}

func TestHandleOffline(t *testing.T) {
	app := &App{}
	if err := app.handleOffline(context.Background(), pipe.Message{Value: []byte("not-json")}); !pipe.IsPoison(err) {
		t.Fatalf("invalid event must be poison, got %v", err)
	}
	if err := app.handleOffline(context.Background(), pipe.Message{Value: []byte(`{"uid":2}`)}); err != nil {
		t.Fatalf("nil dispatcher must not fail: %v", err)
	}
}
