package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/network/quic/v2"
	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/gate"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/network"
	rediscache "github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/component/ratelimit"
	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/internal/component/webtransport"
	"github.com/zciyuan/ZIM/internal/define"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/contract"
)

const codeHeader = "X-ZIM-Code"

func main() {
	version.HandleFlag()
	contract.InstallPacker()

	issuer := token.MustFromEtc()
	addrs := etc.Get("etc.locate.redis.addrs", []string{"127.0.0.1:6379"}).Strings()
	ticketCache := rediscache.NewClient(&rediscache.Options{Addr: addrs[0]})
	server := newServer()
	limiter := ratelimit.New(etc.Get("etc.zim.gate.perIPRate", 500).Int(), etc.Get("etc.zim.gate.perIPBurst", 1000).Int())
	trusted := parseTrustedProxies(etc.Get("etc.zim.gate.trustedProxies", []string{}).Strings())
	upgradeHandler := newUpgradeHandlerWithProxy(func(raw string, platform int32) error {
		if strings.HasPrefix(raw, "t_") {
			value, getErr := ticketCache.GetDel(context.Background(), "zim:ws:ticket:"+raw).Result()
			if getErr != nil {
				return getErr
			}
			parts := strings.Split(value, ":")
			if len(parts) != 2 || parts[1] != strconv.Itoa(int(platform)) {
				return fmt.Errorf("ticket platform mismatch")
			}
			return nil
		}
		started := time.Now()
		_, err := issuer.Verify(raw, platform)
		metrics.Default.Observe("zim_gate_auth_latency_ms", nil, time.Since(started).Milliseconds())
		return err
	}, trusted, limiter)
	switch srv := server.(type) {
	case ws.Server:
		srv.OnUpgrade(upgradeHandler)
	case webtransport.Server:
		srv.OnUpgrade(webtransport.UpgradeHandler(upgradeHandler))
	}

	locator := redis.NewLocator()
	registry := nacos.NewRegistry()

	component := gate.NewGate(
		gate.WithServer(server),
		gate.WithLocator(locator),
		gate.WithRegistry(registry),
		gate.WithGuardAcceptRate(float64(etc.Get("etc.zim.gate.connRate", 2000).Int()), time.Second),
	)

	container := due.NewContainer()
	container.Add(component, health.New(define.ServiceGate))
	container.Serve()
}

// newServer builds the configured due network server. transport=quic and
// transport=webtransport run the same binary as a second UDP deployment while
// WS stays primary.
func newServer() network.Server {
	origins := etc.Get("etc.zim.gate.origins", "*").String()
	switch etc.Get("etc.network.transport", "ws").String() {
	case "quic":
		return quic.NewServer(
			quic.WithServerAddr(etc.Get("etc.network.quic.server.addr", ":12000").String()),
			quic.WithServerCredentials(
				etc.Get("etc.network.quic.server.certFile", "/etc/zim/quic/tls.crt").String(),
				etc.Get("etc.network.quic.server.keyFile", "/etc/zim/quic/tls.key").String(),
			),
			quic.WithServerMaxConnNum(etc.Get("etc.zim.gate.maxConnNum", 100_000).Int()),
			quic.WithServerHeartbeatInterval(etc.Get("etc.network.quic.server.heartbeatInterval", 15*time.Second).Duration()),
			quic.WithServerWriteTimeout(10*time.Second),
			quic.WithServerWriteQueueSize(256),
		)
	case "webtransport":
		return webtransport.NewServer(
			webtransport.WithServerAddr(etc.Get("etc.network.webtransport.server.addr", ":12001").String()),
			webtransport.WithServerPath(etc.Get("etc.network.webtransport.server.path", "/wt").String()),
			webtransport.WithServerCredentials(
				etc.Get("etc.network.webtransport.server.certFile", "/etc/zim/quic/tls.crt").String(),
				etc.Get("etc.network.webtransport.server.keyFile", "/etc/zim/quic/tls.key").String(),
			),
			// WebTransport costs ~11 goroutines per QUIC connection (one
			// session per browser connection), so cap it below the WS budget.
			webtransport.WithServerMaxConnNum(etc.Get("etc.network.webtransport.server.maxConnNum", 20_000).Int()),
			webtransport.WithServerHeartbeatInterval(etc.Get("etc.network.webtransport.server.heartbeatInterval", 15*time.Second).Duration()),
			webtransport.WithServerHeartbeatMechanism(webtransport.HeartbeatMechanism(
				etc.Get("etc.network.webtransport.server.heartbeatMechanism", string(webtransport.RespHeartbeat)).String(),
			)),
			webtransport.WithServerWriteTimeout(etc.Get("etc.network.webtransport.server.writeTimeout", 10*time.Second).Duration()),
			webtransport.WithServerWriteQueueSize(etc.Get("etc.network.webtransport.server.writeQueueSize", 256).Int()),
			webtransport.WithServerAuthorizeTimeout(etc.Get("etc.network.webtransport.server.authorizeTimeout", 45*time.Second).Duration()),
			webtransport.WithServerHandshakeTimeout(etc.Get("etc.network.webtransport.server.handshakeTimeout", 10*time.Second).Duration()),
			webtransport.WithServerCloseTimeout(etc.Get("etc.network.webtransport.server.closeTimeout", 5*time.Second).Duration()),
			webtransport.WithServerCheckOrigin(func(r *http.Request) bool {
				return originAllowed(origins, r.Header.Get("Origin"))
			}),
		)
	}
	return ws.NewServer(
		ws.WithServerAddr(etc.Get("etc.network.ws.server.addr", ":3553").String()),
		ws.WithServerPath(etc.Get("etc.network.ws.server.path", "/ws").String()),
		ws.WithServerMaxConnNum(etc.Get("etc.network.ws.server.maxConnNum", 100_000).Int()),
		ws.WithServerAuthorizeTimeout(etc.Get("etc.network.ws.server.authorizeTimeout", 45*time.Second).Duration()),
		ws.WithServerHeartbeatInterval(etc.Get("etc.network.ws.server.heartbeatInterval", 15*time.Second).Duration()),
		ws.WithServerWriteTimeout(etc.Get("etc.network.ws.server.writeTimeout", 10*time.Second).Duration()),
		ws.WithServerWriteQueueSize(etc.Get("etc.network.ws.server.writeQueueSize", 256).Int()),
		ws.WithServerCheckOrigin(func(r *http.Request) bool {
			return originAllowed(origins, r.Header.Get("Origin"))
		}),
	)
}

// originAllowed reports whether origin is accepted by the configured list.
func originAllowed(origins, origin string) bool {
	if origins == "*" || origin == "" {
		return true
	}
	for _, allowed := range strings.Split(origins, ",") {
		if strings.TrimSpace(allowed) == origin {
			return true
		}
	}
	return false
}

// upgradeVerifyFunc validates a token for a platform.
type upgradeVerifyFunc func(raw string, platform int32) error

// newUpgradeHandler validates query parameters before the WebSocket upgrade.
//
// The ZIM-02 §2.1 contract asks for an upgrade followed by an error frame so
// browsers can observe the failure. due's public ws.Server only exposes a
// pre-upgrade hook, so W0 rejects with HTTP 401 plus X-ZIM-Code; the error
// frame variant is tracked for T28 (transport hardening). See ADR-0009.
func newUpgradeHandler(verify upgradeVerifyFunc, limiters ...*ratelimit.Limiter) ws.UpgradeHandler {
	return newUpgradeHandlerWithProxy(verify, nil, limiters...)
}

// newUpgradeHandlerWithProxy builds a handler shared by the WS and WebTransport
// upgrade paths. It trusts X-Forwarded-For only when the direct peer belongs to
// a configured proxy CIDR.
func newUpgradeHandlerWithProxy(verify upgradeVerifyFunc, trusted []*net.IPNet, limiters ...*ratelimit.Limiter) ws.UpgradeHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if len(limiters) > 0 && limiters[0] != nil {
			ip := clientIP(r, trusted)
			if ip != nil && !limiters[0].Allow(ip.String()) {
				w.Header().Set("Retry-After", "1")
				writeAuthError(w, http.StatusTooManyRequests, 900001, "rate limited")
				return false
			}
		}
		if protocol := r.Header.Get(define.ProtocolHeader); protocol != "" && protocol != define.Protocol {
			writeAuthError(w, http.StatusUpgradeRequired, 900003, "protocol version not supported")
			return false
		}

		query := r.URL.Query()
		raw := query.Get("token")
		if raw == "" {
			raw = query.Get("ticket")
		}
		if raw == "" {
			writeAuthError(w, http.StatusUnauthorized, 100002, "token invalid")
			return false
		}
		platformRaw := query.Get("platform")
		platform, err := strconv.ParseInt(platformRaw, 10, 32)
		if err != nil || platform <= 0 {
			writeAuthError(w, http.StatusBadRequest, 900003, "platform invalid")
			return false
		}
		if query.Get("sdk") == "" {
			writeAuthError(w, http.StatusBadRequest, 900003, "sdk required")
			return false
		}
		if err = verify(raw, int32(platform)); err != nil {
			writeAuthError(w, http.StatusUnauthorized, 100002, "token invalid")
			return false
		}
		return true
	}
}

func clientIP(r *http.Request, trusted []*net.IPNet) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil {
		return nil
	}
	if !trustedPeer(peer, trusted) {
		return peer
	}
	for _, part := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
		if ip := net.ParseIP(strings.TrimSpace(part)); ip != nil {
			return ip
		}
	}
	return peer
}

func trustedPeer(peer net.IP, trusted []*net.IPNet) bool {
	for _, network := range trusted {
		if network.Contains(peer) {
			return true
		}
	}
	return false
}

func parseTrustedProxies(values []string) []*net.IPNet {
	networks := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		_, network, err := net.ParseCIDR(strings.TrimSpace(value))
		if err == nil {
			networks = append(networks, network)
		}
	}
	return networks
}

func writeAuthError(w http.ResponseWriter, status, code int, message string) {
	w.Header().Set(codeHeader, strconv.Itoa(code))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"code":` + strconv.Itoa(code) + `,"message":"` + message + `"}`))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if hijacker, ok := w.(http.Hijacker); ok {
		if conn, _, err := hijacker.Hijack(); err == nil {
			_ = conn.Close()
		}
	}
}
