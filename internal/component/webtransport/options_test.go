package webtransport

import (
	"net/http"
	"testing"
	"time"
)

func TestDefaultServerOptions(t *testing.T) {
	opts := defaultServerOptions()
	if opts.addr != ":12001" || opts.path != "/wt" {
		t.Fatalf("addr/path = %q/%q", opts.addr, opts.path)
	}
	if opts.maxConnNum != 5000 || opts.writeQueueSize != 1024 {
		t.Fatalf("limits = %d/%d", opts.maxConnNum, opts.writeQueueSize)
	}
	if opts.writeTimeout != 0 || opts.heartbeatInterval != 10*time.Second {
		t.Fatalf("timeouts = %v/%v", opts.writeTimeout, opts.heartbeatInterval)
	}
	if opts.heartbeatMechanism != RespHeartbeat {
		t.Fatalf("heartbeat mechanism = %q", opts.heartbeatMechanism)
	}
	if opts.authorizeTimeout != 0 || opts.handshakeTimeout != 5*time.Second || opts.closeTimeout != 5*time.Second {
		t.Fatalf("timeouts = %v/%v/%v", opts.authorizeTimeout, opts.handshakeTimeout, opts.closeTimeout)
	}
}

func TestServerOptions(t *testing.T) {
	check := func(*http.Request) bool { return true }

	opts := defaultServerOptions()
	WithServerAddr(":9000")(opts)
	WithServerPath("wt")(opts)
	WithServerCredentials("cert.pem", "key.pem")(opts)
	WithServerMaxConnNum(7)(opts)
	WithServerWriteTimeout(time.Second)(opts)
	WithServerWriteQueueSize(9)(opts)
	WithServerHeartbeatInterval(3 * time.Second)(opts)
	WithServerHeartbeatMechanism(TickHeartbeat)(opts)
	WithServerAuthorizeTimeout(4 * time.Second)(opts)
	WithServerHandshakeTimeout(5 * time.Second)(opts)
	WithServerCloseTimeout(6 * time.Second)(opts)
	WithServerCheckOrigin(check)(opts)

	if opts.addr != ":9000" || opts.path != "/wt" {
		t.Fatalf("addr/path = %q/%q", opts.addr, opts.path)
	}
	if opts.certFile != "cert.pem" || opts.keyFile != "key.pem" {
		t.Fatalf("credentials = %q/%q", opts.certFile, opts.keyFile)
	}
	if opts.maxConnNum != 7 || opts.writeQueueSize != 9 {
		t.Fatalf("limits = %d/%d", opts.maxConnNum, opts.writeQueueSize)
	}
	if opts.writeTimeout != time.Second || opts.heartbeatInterval != 3*time.Second {
		t.Fatalf("timeouts = %v/%v", opts.writeTimeout, opts.heartbeatInterval)
	}
	if opts.heartbeatMechanism != TickHeartbeat || opts.authorizeTimeout != 4*time.Second {
		t.Fatalf("heartbeat/auth = %q/%v", opts.heartbeatMechanism, opts.authorizeTimeout)
	}
	if opts.handshakeTimeout != 5*time.Second || opts.closeTimeout != 6*time.Second {
		t.Fatalf("timeouts = %v/%v", opts.handshakeTimeout, opts.closeTimeout)
	}
	if opts.checkOrigin == nil || !opts.checkOrigin(nil) {
		t.Fatal("check origin not applied")
	}
}

func TestServerOptionsIgnoreInvalidValues(t *testing.T) {
	opts := defaultServerOptions()
	WithServerAddr("")(opts)
	WithServerPath("")(opts)
	WithServerCredentials("", "key.pem")(opts)
	WithServerMaxConnNum(0)(opts)
	WithServerWriteTimeout(-time.Second)(opts)
	WithServerWriteQueueSize(0)(opts)
	WithServerHeartbeatInterval(-time.Second)(opts)
	WithServerHeartbeatMechanism("invalid")(opts)
	WithServerAuthorizeTimeout(-time.Second)(opts)
	WithServerHandshakeTimeout(0)(opts)
	WithServerCloseTimeout(0)(opts)
	WithServerCheckOrigin(nil)(opts)

	defaults := defaultServerOptions()
	if opts.addr != defaults.addr || opts.path != defaults.path {
		t.Fatalf("addr/path = %q/%q", opts.addr, opts.path)
	}
	if opts.certFile != "" || opts.keyFile != "" {
		t.Fatalf("credentials = %q/%q", opts.certFile, opts.keyFile)
	}
	if opts.maxConnNum != defaults.maxConnNum || opts.writeQueueSize != defaults.writeQueueSize {
		t.Fatalf("limits = %d/%d", opts.maxConnNum, opts.writeQueueSize)
	}
	if opts.writeTimeout != defaults.writeTimeout || opts.heartbeatInterval != defaults.heartbeatInterval {
		t.Fatalf("timeouts = %v/%v", opts.writeTimeout, opts.heartbeatInterval)
	}
	if opts.heartbeatMechanism != defaults.heartbeatMechanism {
		t.Fatalf("heartbeat mechanism = %q", opts.heartbeatMechanism)
	}
	if opts.authorizeTimeout != defaults.authorizeTimeout || opts.handshakeTimeout != defaults.handshakeTimeout {
		t.Fatalf("timeouts = %v/%v", opts.authorizeTimeout, opts.handshakeTimeout)
	}
	if opts.closeTimeout != defaults.closeTimeout || opts.checkOrigin != nil {
		t.Fatalf("close/check = %v/%v", opts.closeTimeout, opts.checkOrigin != nil)
	}
}
