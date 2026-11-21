package webtransport

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/core/queue"
	"github.com/dobyte/due/v2/network"
	wt "github.com/quic-go/webtransport-go"
)

// UpgradeHandler validates a WebTransport CONNECT request before the session is
// created. It reports whether the session is allowed.
//
// The handler owns the HTTP response on rejection and must not write a response
// when it returns true.
type UpgradeHandler func(w http.ResponseWriter, r *http.Request) (allowed bool)

// Server is the WebTransport server interface.
type Server interface {
	network.Server
	// OnUpgrade registers the handler invoked for every CONNECT request before
	// the WebTransport session is created.
	OnUpgrade(handler UpgradeHandler)
}

// HeartbeatMechanism selects how the server sends due heartbeat frames.
type HeartbeatMechanism string

// serverOptions is the immutable server configuration.
type serverOptions struct {
	addr               string
	path               string
	certFile           string
	keyFile            string
	maxConnNum         int
	writeTimeout       time.Duration
	writeQueueSize     int
	heartbeatInterval  time.Duration
	heartbeatMechanism HeartbeatMechanism
	authorizeTimeout   time.Duration
	handshakeTimeout   time.Duration
	closeTimeout       time.Duration
	checkOrigin        func(r *http.Request) bool
}

// serverRun is one listen lifecycle. A server can be started again after Stop.
type serverRun struct {
	conn      net.PacketConn
	wt        *wt.Server
	mgr       *connMgr
	done      chan struct{}
	closeOnce sync.Once
	stopping  bool
}

// server implements the due network server contract on top of HTTP/3 and
// WebTransport.
type server struct {
	opts              *serverOptions
	mu                sync.Mutex
	run               *serverRun
	startHandler      network.StartHandler
	stopHandler       network.CloseHandler
	connectHandler    network.ConnectHandler
	disconnectHandler network.DisconnectHandler
	heartbeatHandler  network.HeartbeatHandler
	receiveHandler    network.ReceiveHandler
	handshakeHandler  network.HandshakeHandler
	upgradeHandler    UpgradeHandler
}

// connMgr tracks live sessions and enforces the connection limit.
type connMgr struct {
	server *server
	mu     sync.Mutex
	cid    atomic.Int64
	conns  map[int64]*serverConn
	closed bool
}

// serverConn is one WebTransport session carrying one ZIM bidirectional stream.
type serverConn struct {
	id                int64
	uid               atomic.Int64
	attr              *attr
	state             atomic.Int32
	mgr               *connMgr
	rw                sync.RWMutex
	started           bool
	wgRead            sync.WaitGroup
	wgWrite           sync.WaitGroup
	session           *wt.Session
	stream            *wt.Stream
	queue             *queue.Queue[buffer.Buffer]
	lastHeartbeatTime atomic.Int64
	authorizeTimer    atomic.Pointer[time.Timer]
}

// attr is the connection attribute store used by the due gate.
type attr struct {
	values sync.Map
}
