package testkit

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/cluster/gate"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/core/buffer"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/registry"

	"github.com/zciyuan/ZIM/zimfw/contract"
)

func newMemRegistry() *memRegistry {
	return &memRegistry{
		byName:   make(map[string][]*registry.ServiceInstance),
		watchers: make(map[string]map[*memRegistryWatcher]struct{}),
	}
}

func (r *memRegistry) Name() string { return "testkit-registry" }

func (r *memRegistry) Register(_ context.Context, ins *registry.ServiceInstance) error {
	clone := cloneInstance(ins)
	r.mu.Lock()
	list := r.byName[clone.Name]
	replaced := false
	for i, item := range list {
		if item.ID == clone.ID {
			list[i] = clone
			replaced = true
			break
		}
	}
	if !replaced {
		list = append(list, clone)
	}
	r.byName[clone.Name] = list
	r.notifyLocked(clone.Name)
	r.mu.Unlock()
	return nil
}

func (r *memRegistry) Deregister(_ context.Context, ins *registry.ServiceInstance) error {
	r.mu.Lock()
	list := r.byName[ins.Name]
	for i, item := range list {
		if item.ID == ins.ID {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	r.byName[ins.Name] = list
	r.notifyLocked(ins.Name)
	r.mu.Unlock()
	return nil
}

func (r *memRegistry) Watch(_ context.Context, serviceName string) (registry.Watcher, error) {
	watcher := &memRegistryWatcher{
		ch:   make(chan []*registry.ServiceInstance, 8),
		done: make(chan struct{}),
	}
	r.mu.Lock()
	if r.watchers[serviceName] == nil {
		r.watchers[serviceName] = make(map[*memRegistryWatcher]struct{})
	}
	r.watchers[serviceName][watcher] = struct{}{}
	initial := r.snapshotLocked(serviceName)
	r.mu.Unlock()
	watcher.ch <- initial
	return watcher, nil
}

func (r *memRegistry) Services(_ context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked(serviceName), nil
}

func (r *memRegistry) Close() error { return nil }

func (r *memRegistry) snapshotLocked(serviceName string) []*registry.ServiceInstance {
	list := r.byName[serviceName]
	snapshot := make([]*registry.ServiceInstance, len(list))
	for i, instance := range list {
		snapshot[i] = cloneInstance(instance)
	}
	return snapshot
}

func cloneInstance(in *registry.ServiceInstance) *registry.ServiceInstance {
	out := *in
	out.Events = append([]int(nil), in.Events...)
	out.Routes = append([]registry.Route(nil), in.Routes...)
	out.Services = append([]string(nil), in.Services...)
	if in.Metadata != nil {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for key, value := range in.Metadata {
			out.Metadata[key] = value
		}
	}
	return &out
}

func (r *memRegistry) notifyLocked(serviceName string) {
	snapshot := r.snapshotLocked(serviceName)
	for watcher := range r.watchers[serviceName] {
		select {
		case watcher.ch <- snapshot:
		default:
		}
	}
}

func (w *memRegistryWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case services := <-w.ch:
		return services, nil
	case <-w.done:
		return nil, dueerrors.ErrWatcherStopped
	}
}

func (w *memRegistryWatcher) Stop() error {
	w.once.Do(func() { close(w.done) })
	return nil
}

func newMemLocator() *memLocator {
	return &memLocator{
		gates: make(map[int64]string),
		nodes: make(map[int64]map[string]string),
	}
}

func (l *memLocator) Name() string { return "testkit-locator" }

func (l *memLocator) Watch(context.Context, ...string) (locate.Watcher, error) {
	return &memLocateWatcher{done: make(chan struct{})}, nil
}

func (l *memLocator) BindGate(_ context.Context, uid int64, gid string) error {
	l.mu.Lock()
	l.gates[uid] = gid
	l.mu.Unlock()
	return nil
}

func (l *memLocator) BindNode(_ context.Context, uid int64, name, nid string) error {
	l.mu.Lock()
	if l.nodes[uid] == nil {
		l.nodes[uid] = make(map[string]string)
	}
	l.nodes[uid][name] = nid
	l.mu.Unlock()
	return nil
}

func (l *memLocator) UnbindGate(_ context.Context, uid int64, gid string) error {
	l.mu.Lock()
	if l.gates[uid] == gid {
		delete(l.gates, uid)
	}
	l.mu.Unlock()
	return nil
}

func (l *memLocator) UnbindNode(_ context.Context, uid int64, name, _ string) error {
	l.mu.Lock()
	delete(l.nodes[uid], name)
	l.mu.Unlock()
	return nil
}

func (l *memLocator) LocateGate(_ context.Context, uid int64) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gates[uid], nil
}

func (l *memLocator) LocateNode(_ context.Context, uid int64, name string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nodes[uid][name], nil
}

func (l *memLocator) LocateNodes(_ context.Context, uid int64) (map[string]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nodes[uid], nil
}

func (l *memLocator) Close() error { return nil }

func (l *memLocator) gate(uid int64) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gates[uid]
}

func (w *memLocateWatcher) Next() ([]*locate.Event, error) {
	<-w.done
	return nil, dueerrors.ErrWatcherStopped
}

func (w *memLocateWatcher) Stop() error {
	w.once.Do(func() { close(w.done) })
	return nil
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	contract.InstallPacker()

	h := &harness{
		registry: newMemRegistry(),
		locator:  newMemLocator(),
		wsAddr:   freeAddr(t),
		gateID:   "gate-1",
		nodeID:   "node-1",
	}
	h.server = ws.NewServer(
		ws.WithServerAddr(h.wsAddr),
		ws.WithServerPath("/ws"),
		ws.WithServerHeartbeatInterval(0),
		ws.WithServerAuthorizeTimeout(0),
	)
	h.gate = gate.NewGate(
		gate.WithID(h.gateID),
		gate.WithName(h.gateID),
		gate.WithServer(h.server),
		gate.WithLocator(h.locator),
		gate.WithRegistry(h.registry),
		gate.WithAddr(freeAddr(t)),
		gate.WithExpose(true),
	)
	h.node = node.NewNode(
		node.WithID(h.nodeID),
		node.WithName(h.nodeID),
		node.WithLocator(h.locator),
		node.WithRegistry(h.registry),
		node.WithAddr(freeAddr(t)),
		node.WithExpose(true),
	)
	h.node.Proxy().Router().AddRouteHandler(bindRoute, bindHandler)
	h.node.Proxy().Router().AddRouteHandler(subscribeRoute, subscribeHandler)

	h.gate.Init()
	h.gate.Start()
	h.node.Init()
	h.node.Start()

	waitFor(t, func() bool {
		ok, _ := h.node.Proxy().HasGate(h.gateID)
		return ok
	}, "node did not discover gate")
	waitFor(t, func() bool {
		services, _ := h.registry.Services(context.Background(), cluster.Node.String())
		return len(services) > 0
	}, "registry did not receive node")

	t.Cleanup(func() {
		for _, conn := range h.clients {
			_ = conn.Close()
		}
		h.gate.Close()
		h.gate.Destroy()
		h.node.Destroy()
	})
	return h
}

func (h *harness) dial(t *testing.T) *testClient {
	t.Helper()
	client := ws.NewClient(
		ws.WithClientUrl("ws://"+h.wsAddr+"/ws"),
		ws.WithClientHeartbeatInterval(0),
	)
	tc := &testClient{
		recv: make(chan []byte, 8),
		done: make(chan struct{}),
	}
	client.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		raw := append([]byte(nil), buf.Bytes()...)
		buf.Release()
		tc.recv <- raw
	})
	client.OnDisconnect(func(network.Conn) {
		select {
		case <-tc.done:
		default:
			close(tc.done)
		}
	})
	conn, err := client.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tc.conn = conn
	h.clients = append(h.clients, conn)
	return tc
}

func (h *harness) bind(t *testing.T, client *testClient, key int64) {
	t.Helper()
	body, err := contract.EncodeEnvelope(&contract.Envelope{
		Opcode:  bindRoute,
		Payload: []byte(strconv.FormatInt(key, 10)),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := contract.EncodeFrame(contract.Frame{
		Version: contract.Version1,
		Opcode:  bindRoute,
		Seq:     1,
		Body:    body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.conn.Push(buffer.NewBytes(frame, true)); err != nil {
		t.Fatalf("push bind: %v", err)
	}
	waitFor(t, func() bool { return h.locator.gate(key) == h.gateID }, "device was not bound")
}

func (h *harness) subscribe(t *testing.T, client *testClient) {
	t.Helper()
	body, err := contract.EncodeEnvelope(&contract.Envelope{Opcode: subscribeRoute})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := contract.EncodeFrame(contract.Frame{
		Version: contract.Version1,
		Opcode:  subscribeRoute,
		Seq:     2,
		Body:    body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.conn.Push(buffer.NewBytes(frame, true)); err != nil {
		t.Fatalf("push subscribe: %v", err)
	}
	waitRoute(t, client, subscribeRoute)
}

func bindHandler(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		envelope := &contract.Envelope{}
		res := &contract.Envelope{Opcode: bindRoute}
		err := ctx.Parse(envelope)
		if err == nil {
			key, parseErr := strconv.ParseInt(string(envelope.GetPayload()), 10, 64)
			if parseErr != nil {
				err = parseErr
			} else {
				err = ctx.BindGate(key)
			}
		}
		if err != nil {
			res.Code = 900003
			res.Message = "bind failed"
		}
		_ = ctx.Response(res)
	})
}

func subscribeHandler(ctx node.Context) {
	ctx.Task(func(ctx node.Context) {
		res := &contract.Envelope{Opcode: subscribeRoute}
		if err := ctx.Subscribe("room"); err != nil {
			res.Code = 900003
			res.Message = "subscribe failed"
		}
		_ = ctx.Response(res)
	})
}

func waitRoute(t *testing.T, client *testClient, route int32) receivedFrame {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case raw := <-client.recv:
			frame, err := contract.DecodeFrameBytes(raw)
			if err != nil {
				t.Fatalf("decode frame: %v", err)
			}
			if frame.Opcode == route {
				return receivedFrame{route: frame.Opcode, seq: int32(frame.Seq), body: frame.Body}
			}
		case <-deadline:
			t.Fatalf("frame route %d not received", route)
		}
	}
}

func waitFor(t *testing.T, cond func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(message)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

var _ registry.Registry = (*memRegistry)(nil)
var _ locate.Locator = (*memLocator)(nil)
