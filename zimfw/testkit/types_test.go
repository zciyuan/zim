package testkit

import (
	"sync"
	"time"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/cluster/gate"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/registry"
)

type harness struct {
	gate     *gate.Gate
	node     *node.Node
	server   ws.Server
	registry *memRegistry
	locator  *memLocator

	wsAddr  string
	gateID  string
	nodeID  string
	clients []network.Conn
}

type memRegistry struct {
	mu       sync.Mutex
	byName   map[string][]*registry.ServiceInstance
	watchers map[string]map[*memRegistryWatcher]struct{}
}

type memRegistryWatcher struct {
	ch   chan []*registry.ServiceInstance
	done chan struct{}
	once sync.Once
}

type memLocator struct {
	mu    sync.Mutex
	gates map[int64]string
	nodes map[int64]map[string]string
}

type memLocateWatcher struct {
	done chan struct{}
	once sync.Once
}

type testClient struct {
	conn network.Conn
	recv chan []byte
	done chan struct{}
}

type receivedFrame struct {
	route int32
	seq   int32
	body  []byte
}

const (
	bindRoute      int32 = 1001
	subscribeRoute int32 = 1002
	testTimeout          = 3 * time.Second
)
