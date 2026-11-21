package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/network"

	"github.com/zciyuan/ZIM/zimfw/contract"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

func TestBindKick(t *testing.T) {
	h := newHarness(t)
	key := multi.Key(10001, multi.PlatformAndroid, 1)

	first := h.dial(t)
	h.bind(t, first, key)

	second := h.dial(t)
	h.bind(t, second, key)

	waitFor(t, func() bool {
		select {
		case <-first.done:
			return true
		default:
			return first.conn.State() == network.ConnClosed
		}
	}, "the first connection was not kicked")
}

func TestMultiDevice(t *testing.T) {
	h := newHarness(t)
	iosKey := multi.Key(10001, multi.PlatformIOS, 1)
	androidKey := multi.Key(10001, multi.PlatformAndroid, 1)

	iosClient := h.dial(t)
	h.bind(t, iosClient, iosKey)
	androidClient := h.dial(t)
	h.bind(t, androidClient, androidKey)

	waitFor(t, func() bool {
		return h.locator.gate(iosKey) == h.gateID && h.locator.gate(androidKey) == h.gateID
	}, "both devices should be bound to the gate")
}

func TestLocateRedirect(t *testing.T) {
	h := newHarness(t)
	key := multi.Key(20001, multi.PlatformPC, 1)
	client := h.dial(t)
	h.bind(t, client, key)

	gid, err := h.node.Proxy().LocateGate(context.Background(), key)
	if err != nil {
		t.Fatalf("LocateGate: %v", err)
	}
	if gid != h.gateID {
		t.Fatalf("gid = %q, want %q", gid, h.gateID)
	}
}

func TestPushAck(t *testing.T) {
	h := newHarness(t)
	key := multi.Key(20002, multi.PlatformWeb, 1)
	client := h.dial(t)
	h.bind(t, client, key)

	message := &cluster.Message{
		Route: contract.OpPushMsg,
		Data:  &contract.Envelope{Opcode: contract.OpPushMsg},
	}
	if err := h.node.Proxy().Push(context.Background(), &cluster.PushArgs{
		Kind:    cluster.User,
		Target:  key,
		Message: message,
		Ack:     true,
	}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	frame := waitRoute(t, client, contract.OpPushMsg)
	if len(frame.body) == 0 {
		t.Fatal("empty push body")
	}
}

func TestChannelPublish(t *testing.T) {
	h := newHarness(t)
	key := multi.Key(20003, multi.PlatformPad, 1)
	client := h.dial(t)
	h.bind(t, client, key)
	h.subscribe(t, client)

	message := &cluster.Message{
		Route: contract.OpPushMsg,
		Data:  &contract.Envelope{Opcode: contract.OpPushMsg},
	}
	total, err := h.node.Proxy().Publish(context.Background(), &cluster.PublishArgs{
		Channel: "room",
		Message: message,
		Ack:     true,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if total != 1 {
		t.Fatalf("publish total = %d, want 1", total)
	}
	waitRoute(t, client, contract.OpPushMsg)
}

func TestGracefulDrain(t *testing.T) {
	h := newHarness(t)
	key := multi.Key(30001, multi.PlatformIOS, 1)
	client := h.dial(t)
	h.bind(t, client, key)

	closed := make(chan struct{})
	go func() {
		h.gate.Close()
		close(closed)
	}()
	time.Sleep(50 * time.Millisecond)

	newClient := ws.NewClient(
		ws.WithClientUrl("ws://"+h.wsAddr+"/ws"),
		ws.WithClientHeartbeatInterval(0),
	)
	conn, err := newClient.Dial()
	if err == nil {
		waitFor(t, func() bool { return conn.State() == network.ConnClosed }, "new connection was not rejected during drain")
	}

	if client.conn.State() != network.ConnOpened {
		t.Fatal("existing connection was closed during drain")
	}
	if err = client.conn.Close(); err != nil {
		t.Fatalf("close existing connection: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(testTimeout):
		t.Fatal("gate did not finish draining")
	}
}
