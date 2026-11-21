// Package webtransport implements a due network.Server on top of HTTP/3 and
// WebTransport. One WebTransport session carries one ZIM connection and one
// bidirectional stream, so the existing [len4][ver1][op2][seq4] frame codec is
// reused without changes.
package webtransport

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	innernet "github.com/dobyte/due/v2/core/net"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/network"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	wt "github.com/quic-go/webtransport-go"
)

var _ Server = (*server)(nil)

// NewServer returns a WebTransport network server.
func NewServer(opts ...ServerOption) Server {
	o := defaultServerOptions()
	for _, opt := range opts {
		opt(o)
	}
	return &server{opts: o}
}

// Addr returns the configured address, or the actual UDP address after Start.
func (s *server) Addr() string {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()

	if run != nil {
		return run.conn.LocalAddr().String()
	}
	return s.opts.addr
}

// Protocol returns the transport name.
func (s *server) Protocol() string {
	return "webtransport"
}

// Start opens the UDP listener and starts serving HTTP/3 WebTransport sessions.
func (s *server) Start() error {
	s.mu.Lock()
	if s.run != nil {
		s.mu.Unlock()
		return errors.ErrIllegalOperation
	}

	cert, err := tls.LoadX509KeyPair(s.opts.certFile, s.opts.keyFile)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	listenNetwork := innernet.ListenNetwork("udp", s.opts.addr)
	addr, err := net.ResolveUDPAddr(listenNetwork, s.opts.addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	conn, err := net.ListenUDP(listenNetwork, addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	tlsConfig := http3.ConfigureTLSConfig(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})
	h3Server := &http3.Server{
		Addr:      s.opts.addr,
		TLSConfig: tlsConfig,
		QUICConfig: &quic.Config{
			HandshakeIdleTimeout: s.opts.handshakeTimeout,
			MaxIdleTimeout:       idleTimeout(s.opts.heartbeatInterval),
		},
	}
	wtServer := &wt.Server{
		H3:                h3Server,
		CheckOrigin:       s.opts.checkOrigin,
		ReorderingTimeout: s.opts.handshakeTimeout,
	}
	run := &serverRun{
		conn: conn,
		wt:   wtServer,
		mgr:  newConnMgr(s),
		done: make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.opts.path, func(w http.ResponseWriter, r *http.Request) {
		s.handleUpgrade(run, w, r)
	})
	h3Server.Handler = mux

	s.run = run
	s.mu.Unlock()

	go s.serve(run)

	if s.startHandler != nil {
		s.startHandler()
	}
	return nil
}

// Stop stops accepting sessions, closes every live session and waits for the
// serving goroutine to exit.
func (s *server) Stop() error {
	s.mu.Lock()
	run := s.run
	if run == nil || run.stopping {
		s.mu.Unlock()
		return errors.ErrIllegalOperation
	}
	run.stopping = true
	s.mu.Unlock()

	// Close sessions and the UDP socket first so that Serve returns, then close
	// the WebTransport server after its serving goroutine has stopped.
	run.mgr.close()
	_ = run.conn.Close()
	<-run.done

	return run.close()
}

// close closes the WebTransport server exactly once.
func (r *serverRun) close() error {
	var err error
	r.closeOnce.Do(func() {
		err = r.wt.Close()
	})
	return err
}

// serve runs the WebTransport server and releases the per-run resources.
func (s *server) serve(run *serverRun) {
	err := run.wt.Serve(run.conn)
	if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) {
		log.Warnf("webtransport server shutdown, err: %v", err)
	}

	_ = run.conn.Close()
	run.mgr.close()
	_ = run.close()

	s.mu.Lock()
	if s.run == run {
		s.run = nil
	}
	s.mu.Unlock()

	if s.stopHandler != nil {
		s.stopHandler()
	}
	close(run.done)
}

// handleUpgrade validates the CONNECT request and starts one session.
func (s *server) handleUpgrade(run *serverRun, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		observeSession("rejected")
		return
	}

	if s.upgradeHandler != nil && !s.upgradeHandler(w, r) {
		observeSession("rejected")
		return
	}

	session, err := run.wt.Upgrade(w, r)
	if err != nil {
		log.Warnf("webtransport upgrade failed: %v", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		observeSession("rejected")
		return
	}

	go s.attach(run, session)
}

// attach binds the first bidirectional stream of a session to one due
// connection. The client must open the stream immediately after CONNECT.
func (s *server) attach(run *serverRun, session *wt.Session) {
	ctx, cancel := context.WithTimeout(session.Context(), s.opts.handshakeTimeout)
	stream, err := session.AcceptStream(ctx)
	cancel()
	if err != nil {
		_ = session.CloseWithError(0, "stream accept failed")
		observeSession("rejected")
		return
	}

	conn := newServerConn(run.mgr, session, stream)
	if !run.mgr.add(conn) {
		_ = session.CloseWithError(0, "connection limit reached")
		observeSession("rejected")
		return
	}

	conn.start()
}

// idleTimeout keeps a QUIC connection alive long enough for the application
// heartbeat to run, and uses a longer timeout when the heartbeat is disabled.
func idleTimeout(heartbeatInterval time.Duration) time.Duration {
	if heartbeatInterval <= 0 {
		return 5 * time.Minute
	}
	return max(30*time.Second, 3*heartbeatInterval)
}

// OnStart registers the handler invoked when the server starts.
func (s *server) OnStart(handler network.StartHandler) {
	s.startHandler = handler
}

// OnStop registers the handler invoked when the server stops.
func (s *server) OnStop(handler network.CloseHandler) {
	s.stopHandler = handler
}

// OnConnect registers the handler invoked when a session is ready.
func (s *server) OnConnect(handler network.ConnectHandler) {
	s.connectHandler = handler
}

// OnDisconnect registers the handler invoked when a session is closed.
func (s *server) OnDisconnect(handler network.DisconnectHandler) {
	s.disconnectHandler = handler
}

// OnReceive registers the handler invoked when a data frame is received.
func (s *server) OnReceive(handler network.ReceiveHandler) {
	s.receiveHandler = handler
}

// OnHandshake registers the handler invoked when a handshake frame is received.
func (s *server) OnHandshake(handler network.HandshakeHandler) {
	s.handshakeHandler = handler
}

// OnHeartbeat registers the handler invoked when a heartbeat is received.
func (s *server) OnHeartbeat(handler network.HeartbeatHandler) {
	s.heartbeatHandler = handler
}

// OnUpgrade registers the handler invoked before a session is created.
func (s *server) OnUpgrade(handler UpgradeHandler) {
	s.upgradeHandler = handler
}
