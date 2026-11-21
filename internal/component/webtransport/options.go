package webtransport

import (
	"net/http"
	"time"

	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
)

const (
	defaultServerAddr               = ":12001"
	defaultServerPath               = "/wt"
	defaultServerMaxConnNum         = 5000
	defaultServerWriteTimeout       = "0s"
	defaultServerWriteQueueSize     = 1024
	defaultServerHeartbeatInterval  = "10s"
	defaultServerHeartbeatMechanism = RespHeartbeat
	defaultServerAuthorizeTimeout   = "0s"
	defaultServerHandshakeTimeout   = "5s"
	defaultServerCloseTimeout       = "5s"
)

const (
	defaultServerAddrKey               = "etc.network.webtransport.server.addr"
	defaultServerPathKey               = "etc.network.webtransport.server.path"
	defaultServerCertFileKey           = "etc.network.webtransport.server.certFile"
	defaultServerKeyFileKey            = "etc.network.webtransport.server.keyFile"
	defaultServerMaxConnNumKey         = "etc.network.webtransport.server.maxConnNum"
	defaultServerWriteTimeoutKey       = "etc.network.webtransport.server.writeTimeout"
	defaultServerWriteQueueSizeKey     = "etc.network.webtransport.server.writeQueueSize"
	defaultServerHeartbeatIntervalKey  = "etc.network.webtransport.server.heartbeatInterval"
	defaultServerHeartbeatMechanismKey = "etc.network.webtransport.server.heartbeatMechanism"
	defaultServerAuthorizeTimeoutKey   = "etc.network.webtransport.server.authorizeTimeout"
	defaultServerHandshakeTimeoutKey   = "etc.network.webtransport.server.handshakeTimeout"
	defaultServerCloseTimeoutKey       = "etc.network.webtransport.server.closeTimeout"
)

const (
	// RespHeartbeat sends a heartbeat only after one is received from the peer.
	RespHeartbeat HeartbeatMechanism = "resp"
	// TickHeartbeat sends a heartbeat at every heartbeat interval.
	TickHeartbeat HeartbeatMechanism = "tick"
)

// ServerOption is a functional option for configuring a WebTransport server.
type ServerOption func(o *serverOptions)

// defaultServerOptions builds the option set from etc first and falls back to
// the compiled defaults.
func defaultServerOptions() *serverOptions {
	opts := &serverOptions{
		addr:               defaultServerAddr,
		path:               defaultServerPath,
		maxConnNum:         defaultServerMaxConnNum,
		writeTimeout:       0,
		writeQueueSize:     defaultServerWriteQueueSize,
		heartbeatInterval:  10 * time.Second,
		heartbeatMechanism: defaultServerHeartbeatMechanism,
		authorizeTimeout:   0,
		handshakeTimeout:   5 * time.Second,
		closeTimeout:       5 * time.Second,
	}

	if addr := etc.Get(defaultServerAddrKey, defaultServerAddr).String(); addr != "" {
		opts.addr = addr
	}
	if path := etc.Get(defaultServerPathKey, defaultServerPath).String(); path != "" {
		opts.path = path
	}
	opts.certFile = etc.Get(defaultServerCertFileKey).String()
	opts.keyFile = etc.Get(defaultServerKeyFileKey).String()

	if maxConnNum := etc.Get(defaultServerMaxConnNumKey, defaultServerMaxConnNum).Int(); maxConnNum > 0 {
		opts.maxConnNum = maxConnNum
	}
	if writeTimeout := etc.Get(defaultServerWriteTimeoutKey, defaultServerWriteTimeout).Duration(); writeTimeout >= 0 {
		opts.writeTimeout = writeTimeout
	}
	if writeQueueSize := etc.Get(defaultServerWriteQueueSizeKey, defaultServerWriteQueueSize).Int(); writeQueueSize > 0 {
		opts.writeQueueSize = writeQueueSize
	}
	if heartbeatInterval := etc.Get(defaultServerHeartbeatIntervalKey, defaultServerHeartbeatInterval).Duration(); heartbeatInterval >= 0 {
		opts.heartbeatInterval = heartbeatInterval
	}

	switch mechanism := HeartbeatMechanism(etc.Get(defaultServerHeartbeatMechanismKey, string(defaultServerHeartbeatMechanism)).String()); mechanism {
	case RespHeartbeat, TickHeartbeat:
		opts.heartbeatMechanism = mechanism
	}

	if authorizeTimeout := etc.Get(defaultServerAuthorizeTimeoutKey, defaultServerAuthorizeTimeout).Duration(); authorizeTimeout >= 0 {
		opts.authorizeTimeout = authorizeTimeout
	}
	if handshakeTimeout := etc.Get(defaultServerHandshakeTimeoutKey, defaultServerHandshakeTimeout).Duration(); handshakeTimeout > 0 {
		opts.handshakeTimeout = handshakeTimeout
	}
	if closeTimeout := etc.Get(defaultServerCloseTimeoutKey, defaultServerCloseTimeout).Duration(); closeTimeout > 0 {
		opts.closeTimeout = closeTimeout
	}

	return opts
}

// WithServerAddr sets the UDP listen address. An empty address is ignored.
func WithServerAddr(addr string) ServerOption {
	return func(o *serverOptions) {
		if addr == "" {
			log.Warnf("the specified webtransport addr is empty and will be ignored")
			return
		}
		o.addr = addr
	}
}

// WithServerPath sets the HTTP/3 CONNECT path. A path without a leading slash
// gets one prepended; an empty path is ignored.
func WithServerPath(path string) ServerOption {
	return func(o *serverOptions) {
		if path == "" {
			log.Warnf("the specified webtransport path is empty and will be ignored")
			return
		}
		if path[0] != '/' {
			path = "/" + path
		}
		o.path = path
	}
}

// WithServerCredentials sets the TLS certificate and private key. WebTransport
// requires HTTP/3, which in turn requires TLS 1.3.
func WithServerCredentials(certFile, keyFile string) ServerOption {
	return func(o *serverOptions) {
		if certFile == "" || keyFile == "" {
			log.Warnf("the specified webtransport certFile or keyFile is empty and will be ignored")
			return
		}
		o.certFile, o.keyFile = certFile, keyFile
	}
}

// WithServerMaxConnNum sets the maximum number of concurrent sessions. A value
// less than or equal to zero is ignored.
func WithServerMaxConnNum(maxConnNum int) ServerOption {
	return func(o *serverOptions) {
		if maxConnNum <= 0 {
			log.Warnf("the specified webtransport maxConnNum is less than zero and will be ignored")
			return
		}
		o.maxConnNum = maxConnNum
	}
}

// WithServerWriteTimeout sets the timeout of one stream write. A negative
// timeout is ignored.
func WithServerWriteTimeout(writeTimeout time.Duration) ServerOption {
	return func(o *serverOptions) {
		if writeTimeout < 0 {
			log.Warnf("the specified webtransport writeTimeout is less than zero and will be ignored")
			return
		}
		o.writeTimeout = writeTimeout
	}
}

// WithServerWriteQueueSize sets the per-connection write queue size. A value
// less than or equal to zero is ignored.
func WithServerWriteQueueSize(writeQueueSize int) ServerOption {
	return func(o *serverOptions) {
		if writeQueueSize <= 0 {
			log.Warnf("the specified webtransport writeQueueSize is less than zero and will be ignored")
			return
		}
		o.writeQueueSize = writeQueueSize
	}
}

// WithServerHeartbeatInterval sets the due heartbeat interval. Zero disables
// the application-level heartbeat; QUIC idle timeout still applies.
func WithServerHeartbeatInterval(heartbeatInterval time.Duration) ServerOption {
	return func(o *serverOptions) {
		if heartbeatInterval < 0 {
			log.Warnf("the specified webtransport heartbeatInterval is less than zero and will be ignored")
			return
		}
		o.heartbeatInterval = heartbeatInterval
	}
}

// WithServerHeartbeatMechanism sets the heartbeat mechanism. It must be
// [RespHeartbeat] or [TickHeartbeat].
func WithServerHeartbeatMechanism(heartbeatMechanism HeartbeatMechanism) ServerOption {
	return func(o *serverOptions) {
		if heartbeatMechanism != RespHeartbeat && heartbeatMechanism != TickHeartbeat {
			log.Warnf("the specified webtransport heartbeatMechanism is %v and will be ignored", heartbeatMechanism)
			return
		}
		o.heartbeatMechanism = heartbeatMechanism
	}
}

// WithServerAuthorizeTimeout sets how long a session may stay unbound. Zero
// disables the check.
func WithServerAuthorizeTimeout(authorizeTimeout time.Duration) ServerOption {
	return func(o *serverOptions) {
		if authorizeTimeout < 0 {
			log.Warnf("the specified webtransport authorizeTimeout is less than zero and will be ignored")
			return
		}
		o.authorizeTimeout = authorizeTimeout
	}
}

// WithServerHandshakeTimeout sets how long the server waits for the client's
// first bidirectional stream. A value less than or equal to zero is ignored.
func WithServerHandshakeTimeout(handshakeTimeout time.Duration) ServerOption {
	return func(o *serverOptions) {
		if handshakeTimeout <= 0 {
			log.Warnf("the specified webtransport handshakeTimeout is less than zero and will be ignored")
			return
		}
		o.handshakeTimeout = handshakeTimeout
	}
}

// WithServerCloseTimeout sets the graceful close drain and retransmission
// budget. A value less than or equal to zero is ignored.
func WithServerCloseTimeout(closeTimeout time.Duration) ServerOption {
	return func(o *serverOptions) {
		if closeTimeout <= 0 {
			log.Warnf("the specified webtransport closeTimeout is less than zero and will be ignored")
			return
		}
		o.closeTimeout = closeTimeout
	}
}

// WithServerCheckOrigin sets the WebTransport origin check. When it is nil the
// library compares the Origin host with the request Host.
func WithServerCheckOrigin(check func(r *http.Request) bool) ServerOption {
	return func(o *serverOptions) {
		if check == nil {
			log.Warnf("the specified webtransport checkOrigin is nil and will be ignored")
			return
		}
		o.checkOrigin = check
	}
}
