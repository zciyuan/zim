package pushoffline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

type fakeDevices struct {
	targets []*model.Device
	err     error
	calls   int
}

func (f *fakeDevices) Targets(context.Context, []int64) ([]*model.Device, error) {
	f.calls++
	return f.targets, f.err
}

type fakeProvider struct {
	sent    []Notification
	results []Result
	err     error
}

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) Send(_ context.Context, notifications []Notification) ([]Result, error) {
	f.sent = append(f.sent, notifications...)
	if f.err != nil {
		return nil, f.err
	}
	if f.results != nil {
		return f.results, nil
	}
	results := make([]Result, 0, len(notifications))
	for _, notification := range notifications {
		results = append(results, Result{Token: notification.Token, OK: true})
	}
	return results, nil
}

type fakeLimiter struct {
	allowed bool
	err     error
}

func (f *fakeLimiter) Allow(context.Context, int64) (bool, error) {
	return f.allowed, f.err
}

func newEvent() *Event {
	return &Event{UID: 10001, ServerMsgID: "s1", ConversationID: "s_1_2", Seq: 7, SendID: 1, MsgType: 1}
}

func TestDispatcherSendsToActiveDevices(t *testing.T) {
	devices := &fakeDevices{targets: []*model.Device{
		{OwnerID: 10001, Platform: 1, Status: 0, PushToken: "apns-1", PushProvider: "apns"},
		{OwnerID: 10001, Platform: 2, Status: 0, PushToken: "", PushProvider: "fcm"},
		{OwnerID: 10001, Platform: 3, Status: 1, PushToken: "stale"},
	}}
	provider := &fakeProvider{}
	dispatcher := NewDispatcher(devices, provider, nil)

	if err := dispatcher.Handle(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
	if len(provider.sent) != 1 || provider.sent[0].Token != "apns-1" {
		t.Fatalf("sent = %+v", provider.sent)
	}
	if provider.sent[0].Extra["server_msg_id"] != "s1" {
		t.Fatalf("extra = %v", provider.sent[0].Extra)
	}
	if provider.sent[0].Body != defaultBody {
		t.Fatalf("body must not contain message content: %q", provider.sent[0].Body)
	}
}

func TestDispatcherRateLimited(t *testing.T) {
	provider := &fakeProvider{}
	dispatcher := NewDispatcher(&fakeDevices{targets: []*model.Device{
		{OwnerID: 10001, PushToken: "t"},
	}}, provider, &fakeLimiter{allowed: false})

	if err := dispatcher.Handle(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
	if len(provider.sent) != 0 {
		t.Fatal("rate-limited user must not be pushed")
	}
}

func TestDispatcherLimiterFailOpen(t *testing.T) {
	provider := &fakeProvider{}
	dispatcher := NewDispatcher(&fakeDevices{targets: []*model.Device{
		{OwnerID: 10001, PushToken: "t"},
	}}, provider, &fakeLimiter{err: errors.New("redis down")})

	if err := dispatcher.Handle(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
	if len(provider.sent) != 1 {
		t.Fatal("limiter failure must fail open")
	}
}

func TestDispatcherNoDevice(t *testing.T) {
	provider := &fakeProvider{}
	dispatcher := NewDispatcher(&fakeDevices{}, provider, nil)
	if err := dispatcher.Handle(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
	if len(provider.sent) != 0 {
		t.Fatal("no provider call expected")
	}
}

func TestDispatcherDeviceError(t *testing.T) {
	dispatcher := NewDispatcher(&fakeDevices{err: errors.New("pg down")}, &fakeProvider{}, nil)
	if err := dispatcher.Handle(context.Background(), newEvent()); err == nil {
		t.Fatal("device error must surface")
	}
}

func TestDispatcherProviderError(t *testing.T) {
	dispatcher := NewDispatcher(&fakeDevices{targets: []*model.Device{
		{OwnerID: 10001, PushToken: "t"},
	}}, &fakeProvider{err: errors.New("apns down")}, nil)
	if err := dispatcher.Handle(context.Background(), newEvent()); err == nil {
		t.Fatal("provider error must surface")
	}
}

func TestDispatcherCountsInvalidTokens(t *testing.T) {
	provider := &fakeProvider{results: []Result{
		{Token: "a", OK: true}, {Token: "b", Invalid: true}, {Token: "c"},
	}}
	devices := &fakeDevices{targets: []*model.Device{
		{OwnerID: 10001, PushToken: "a"}, {OwnerID: 10001, PushToken: "b"}, {OwnerID: 10001, PushToken: "c"},
	}}
	if err := NewDispatcher(devices, provider, nil).Handle(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLimiter(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	limiter := NewRedisLimiter(client, 3, time.Minute)
	for i := 0; i < 3; i++ {
		allowed, err := limiter.Allow(context.Background(), 7)
		if err != nil || !allowed {
			t.Fatalf("hit %d: allowed = %v, err = %v", i+1, allowed, err)
		}
	}
	allowed, err := limiter.Allow(context.Background(), 7)
	if err != nil || allowed {
		t.Fatalf("4th hit must be limited: allowed = %v, err = %v", allowed, err)
	}
	server.FastForward(time.Minute + time.Second)
	allowed, err = limiter.Allow(context.Background(), 7)
	if err != nil || !allowed {
		t.Fatalf("after window: allowed = %v, err = %v", allowed, err)
	}
}

type fakeProducer struct {
	last pipe.Message
}

func (f *fakeProducer) Publish(_ context.Context, message pipe.Message) error {
	f.last = message
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func TestPublisherRoundTrip(t *testing.T) {
	producer := &fakeProducer{}
	publisher := NewPublisher(producer)
	event := newEvent()
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if producer.last.Topic != "zim.pipe.toOfflinePush" || producer.last.Key != "10001" {
		t.Fatalf("message = %+v", producer.last)
	}
	decoded, err := Decode(producer.last.Value)
	if err != nil || decoded.ServerMsgID != "s1" || decoded.Seq != 7 {
		t.Fatalf("decoded = %+v, err = %v", decoded, err)
	}
}

func TestPublisherNilSafe(t *testing.T) {
	var publisher *Publisher
	if err := publisher.Publish(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher(nil).Publish(context.Background(), newEvent()); err != nil {
		t.Fatal(err)
	}
}

func TestHandleManyUsesOneDeviceQuery(t *testing.T) {
	devices := &fakeDevices{targets: []*model.Device{
		{OwnerID: 1, Platform: 1, PushToken: "a"},
		{OwnerID: 2, Platform: 2, PushToken: "b"},
	}}
	provider := &fakeProvider{}
	dispatcher := NewDispatcher(devices, provider, nil)
	event := &Event{ServerMsgID: "s1", ConversationID: "g_1", Seq: 9, SendID: 1}
	if err := dispatcher.HandleMany(context.Background(), event, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if devices.calls != 1 {
		t.Fatalf("device queries = %d, want 1", devices.calls)
	}
	if len(provider.sent) != 2 {
		t.Fatalf("notifications = %d, want 2", len(provider.sent))
	}
}
