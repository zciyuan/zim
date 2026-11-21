package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"google.golang.org/protobuf/proto"

	msgwire "github.com/zciyuan/ZIM/internal/protocol/message"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/multi"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

type fakeLocator struct {
	mu     sync.Mutex
	routes map[int64]string
	calls  int
}

func (f *fakeLocator) LocateDeviceKeys(_ context.Context, keys []int64) (map[int64]string, error) {
	f.mu.Lock()
	f.calls += len(keys)
	f.mu.Unlock()
	found := make(map[int64]string, len(keys))
	for _, key := range keys {
		if gateID, ok := f.routes[key]; ok {
			found[key] = gateID
		}
	}
	return found, nil
}

func (f *fakeLocator) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type multicastCall struct {
	gateID  string
	targets []int64
	payload []byte
}

type fakePusher struct {
	mu    sync.Mutex
	calls []multicastCall
	err   error
}

func (f *fakePusher) Multicast(_ context.Context, args *cluster.MulticastArgs) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	call := multicastCall{gateID: args.GID, targets: append([]int64(nil), args.Targets...)}
	if data, ok := args.Message.Data.([]byte); ok {
		call.payload = data
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	return int64(len(args.Targets)), nil
}

func (f *fakePusher) snapshot() []multicastCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]multicastCall(nil), f.calls...)
}

type blockingConsumer struct {
	started chan struct{}
}

func (c *blockingConsumer) Topic() string { return "zim.pipe.toPush" }

func (c *blockingConsumer) Consume(ctx context.Context, _ pipe.Handler) error {
	close(c.started)
	<-ctx.Done()
	return ctx.Err()
}

func (c *blockingConsumer) Close() error { return nil }

func newEvent(t *testing.T, recipients []int64) []byte {
	t.Helper()
	event := &messagesvc.Event{
		ServerMsgID: "1", ClientMsgID: "c1", ConversationID: "s_1_2",
		Seq: 1, SendID: 1, RecvID: 2, MsgType: 1, Content: "x", Recipients: recipients,
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestBuildTargetsIncludesSender(t *testing.T) {
	targets := (&App{}).buildTargets(context.Background(), []int64{1, 2})
	if len(targets) != 10 {
		t.Fatalf("targets = %d, want 10", len(targets))
	}
	want := multi.Key(1, 2, 1)
	found := false
	for _, target := range targets {
		if target == want {
			found = true
		}
	}
	if !found {
		t.Fatal("sender devices must be included for multi-device echo")
	}
}

func TestHandlePushesOnlineDevicesByGate(t *testing.T) {
	locator := &fakeLocator{routes: map[int64]string{
		multi.Key(2, 1, 1): "gate-a",
		multi.Key(2, 3, 1): "gate-a",
		multi.Key(1, 2, 1): "gate-b",
		multi.Key(1, 4, 1): "gate-b",
	}}
	pusher := &fakePusher{}
	app := &App{proxy: pusher, locator: locator}

	if err := app.handle(context.Background(), pipe.Message{Value: newEvent(t, []int64{1, 2})}); err != nil {
		t.Fatal(err)
	}

	calls := pusher.snapshot()
	if len(calls) != 2 {
		t.Fatalf("multicast calls = %d, want 2 (one per gate)", len(calls))
	}
	seen := make(map[int64]bool)
	for _, call := range calls {
		if call.gateID == "" {
			t.Fatal("gate ID must be set for direct multicast")
		}
		for _, target := range call.targets {
			if seen[target] {
				t.Fatalf("duplicate target %d", target)
			}
			seen[target] = true
		}
	}
	if len(seen) != 4 {
		t.Fatalf("online targets = %d, want 4", len(seen))
	}
	if !seen[multi.Key(1, 2, 1)] {
		t.Fatal("sender's other device must be delivered")
	}
	if locator.callCount() != 10 {
		t.Fatalf("locate keys = %d, want 10", locator.callCount())
	}
}

func TestHandleCachesOnlineRoutesOnly(t *testing.T) {
	locator := &fakeLocator{routes: map[int64]string{
		multi.Key(2, 1, 1): "gate-a",
		multi.Key(1, 2, 1): "gate-b",
	}}
	pusher := &fakePusher{}
	app := &App{proxy: pusher, locator: locator}
	value := newEvent(t, []int64{1, 2})

	if err := app.handle(context.Background(), pipe.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	first := locator.callCount()
	if first != 10 {
		t.Fatalf("first locate keys = %d", first)
	}
	if err := app.handle(context.Background(), pipe.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	second := locator.callCount() - first
	if second != 8 {
		t.Fatalf("second locate keys = %d, want 8 (offline keys are not cached)", second)
	}
}

func TestHandleAllOffline(t *testing.T) {
	pusher := &fakePusher{}
	app := &App{proxy: pusher, locator: &fakeLocator{routes: map[int64]string{}}}
	if err := app.handle(context.Background(), pipe.Message{Value: newEvent(t, []int64{2})}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.snapshot()) != 0 {
		t.Fatal("no multicast expected when everyone is offline")
	}
}

func TestHandleMulticastFailureInvalidatesCache(t *testing.T) {
	locator := &fakeLocator{routes: map[int64]string{multi.Key(2, 1, 1): "gate-a"}}
	pusher := &fakePusher{err: errors.New("gate down")}
	app := &App{proxy: pusher, locator: locator}
	value := newEvent(t, []int64{2})

	if err := app.handle(context.Background(), pipe.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	if locator.callCount() != 5 {
		t.Fatalf("locate keys = %d", locator.callCount())
	}
	pusher.err = nil
	if err := app.handle(context.Background(), pipe.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	if locator.callCount() != 10 {
		t.Fatalf("stale route must be re-located, keys = %d", locator.callCount())
	}
}

func TestHandleEncodesPushEnvelope(t *testing.T) {
	locator := &fakeLocator{routes: map[int64]string{multi.Key(2, 2, 1): "gate-a"}}
	pusher := &fakePusher{}
	app := &App{proxy: pusher, locator: locator}
	if err := app.handle(context.Background(), pipe.Message{Value: newEvent(t, []int64{2})}); err != nil {
		t.Fatal(err)
	}
	calls := pusher.snapshot()
	if len(calls) != 1 || len(calls[0].payload) == 0 {
		t.Fatalf("calls = %+v", calls)
	}
	envelope := &contract.Envelope{}
	if err := proto.Unmarshal(calls[0].payload, envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Opcode != contract.OpPushMsg {
		t.Fatalf("opcode = %d", envelope.Opcode)
	}
	item := &msgwire.MessageItem{}
	if err := proto.Unmarshal(envelope.Payload, item); err != nil {
		t.Fatal(err)
	}
	if item.GetServerMsgId() != "1" {
		t.Fatalf("item = %+v", item)
	}
}

func TestStartAndClose(t *testing.T) {
	consumer := &blockingConsumer{started: make(chan struct{})}
	app := New(Options{Proxy: &fakePusher{}, Locator: &fakeLocator{routes: map[int64]string{}}, Consumer: consumer})
	app.Start()
	select {
	case <-consumer.started:
	case <-time.After(time.Second):
		t.Fatal("consumer did not start")
	}
	app.Close()
}

func TestStartMultipleConsumers(t *testing.T) {
	first := &blockingConsumer{started: make(chan struct{})}
	second := &blockingConsumer{started: make(chan struct{})}
	app := New(Options{
		Proxy: &fakePusher{}, Locator: &fakeLocator{routes: map[int64]string{}},
		Consumer: first, Consumers: []pipe.Consumer{second},
	})
	app.Start()
	for name, started := range map[string]<-chan struct{}{"first": first.started, "second": second.started} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s consumer did not start", name)
		}
	}
	app.Close()
}

func TestStartWithoutConsumer(t *testing.T) {
	app := New(Options{Proxy: &fakePusher{}})
	app.Start()
	app.Close()
}

func TestHandleErrors(t *testing.T) {
	app := New(Options{Proxy: &fakePusher{}, Locator: &fakeLocator{routes: map[int64]string{}}})
	if err := app.handle(context.Background(), pipe.Message{Value: []byte("not-json")}); !pipe.IsPoison(err) {
		t.Fatalf("invalid event must be poison, got %v", err)
	}
	if err := app.handle(context.Background(), pipe.Message{Value: []byte(
		`{"server_msg_id":"1","conversation_id":"s_1_2","recipients":[2]}`,
	)}); err != nil {
		t.Fatalf("offline push must not fail the consumer: %v", err)
	}
}
