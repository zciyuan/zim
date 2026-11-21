package webtransport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/network"
	"github.com/quic-go/quic-go"
	wt "github.com/quic-go/webtransport-go"

	"github.com/zciyuan/ZIM/zimfw/contract"
)

func TestMain(m *testing.M) {
	contract.InstallPacker()
	os.Exit(m.Run())
}

func TestServerRoundTrip(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerPath("/wt"),
		WithServerCredentials(certFile, keyFile),
		WithServerHeartbeatInterval(0),
		WithServerMaxConnNum(4),
	)
	server.OnUpgrade(func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("token") != "tok" {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return false
		}
		return true
	})

	connected := make(chan network.Conn, 1)
	disconnected := make(chan error, 1)
	received := make(chan receivedFrame, 1)
	server.OnConnect(func(conn network.Conn) { connected <- conn })
	server.OnDisconnect(func(conn network.Conn) {
		_, err := conn.RemoteIP()
		disconnected <- err
	})
	server.OnReceive(func(_ network.Conn, buf buffer.Buffer) {
		defer buf.Release()
		frame, err := contract.DecodeFrameBytes(buf.Bytes())
		if err != nil {
			received <- receivedFrame{}
			return
		}
		received <- receivedFrame{
			opcode: frame.Opcode,
			seq:    frame.Seq,
			body:   append([]byte(nil), frame.Body...),
		}
	})

	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	addr := server.Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport := &wt.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test certificate
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	t.Cleanup(func() { _ = transport.Close() })

	response, session, err := transport.Dial(ctx, "https://"+addr+"/wt?token=tok&platform=4&sdk=test", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	defer func() { _ = session.CloseWithError(0, "test done") }()

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	frame, err := contract.EncodeFrame(contract.Frame{Opcode: contract.OpLogin, Seq: 7, Body: []byte("login")})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if _, err = stream.Write(frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}

	var conn network.Conn
	select {
	case conn = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not accept the stream")
	}

	select {
	case got := <-received:
		if got.opcode != contract.OpLogin || got.seq != 7 || string(got.body) != "login" {
			t.Fatalf("received frame = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the frame")
	}

	push, err := contract.EncodeFrame(contract.Frame{Opcode: contract.OpLogin, Seq: 0, Body: []byte("welcome")})
	if err != nil {
		t.Fatalf("encode push: %v", err)
	}
	if err = conn.Push(buffer.NewBytes(push, true)); err != nil {
		t.Fatalf("push: %v", err)
	}

	if err = stream.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	pushed, err := contract.DecodeFrame(stream)
	if err != nil {
		t.Fatalf("read push: %v", err)
	}
	if pushed.Opcode != contract.OpLogin || pushed.Seq != 0 || string(pushed.Body) != "welcome" {
		t.Fatalf("pushed frame = %+v", pushed)
	}

	if err = conn.Close(true); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	select {
	case err = <-disconnected:
		if err != nil {
			t.Fatalf("disconnect hook address: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not report the disconnect")
	}
}

func TestServerRejectsBadToken(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerPath("/wt"),
		WithServerCredentials(certFile, keyFile),
		WithServerHeartbeatInterval(0),
	)
	server.OnUpgrade(func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("token") != "tok" {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return false
		}
		return true
	})
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := &wt.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test certificate
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	t.Cleanup(func() { _ = transport.Close() })

	if _, session, err := transport.Dial(ctx, "https://"+server.Addr()+"/wt?token=bad", nil); err == nil {
		_ = session.CloseWithError(0, "unexpected")
		t.Fatal("bad token must be rejected")
	}
}

func TestServerRejectsOrigin(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerCredentials(certFile, keyFile),
		WithServerHeartbeatInterval(0),
		WithServerCheckOrigin(func(*http.Request) bool { return false }),
	)
	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := &wt.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test certificate
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	t.Cleanup(func() { _ = transport.Close() })

	if _, session, err := transport.Dial(ctx, "https://"+server.Addr()+"/wt", nil); err == nil {
		_ = session.CloseWithError(0, "unexpected")
		t.Fatal("disallowed origin must be rejected")
	}
}

func TestServerLifecycle(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerPath("/wt"),
		WithServerCredentials(certFile, keyFile),
		WithServerHeartbeatInterval(0),
	)
	started := 0
	stopped := 0
	server.OnStart(func() { started++ })
	server.OnStop(func() { stopped++ })

	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if started != 1 {
		t.Fatalf("start hook = %d", started)
	}
	if addr := server.Addr(); addr == "" || strings.HasSuffix(addr, ":0") {
		t.Fatalf("addr = %q", addr)
	}
	if server.Protocol() != "webtransport" {
		t.Fatalf("protocol = %q", server.Protocol())
	}
	if err := server.Start(); !errors.Is(err, errors.ErrIllegalOperation) {
		t.Fatalf("second start = %v", err)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopped != 1 {
		t.Fatalf("stop hook = %d", stopped)
	}
	if err := server.Stop(); !errors.Is(err, errors.ErrIllegalOperation) {
		t.Fatalf("second stop = %v", err)
	}
}

func TestServerStartWithoutCertificate(t *testing.T) {
	server := NewServer(WithServerAddr("127.0.0.1:0"), WithServerCredentials("", ""))
	if err := server.Start(); err == nil {
		t.Fatal("start without a certificate must fail")
	}
	if err := server.Stop(); !errors.Is(err, errors.ErrIllegalOperation) {
		t.Fatalf("stop = %v", err)
	}
}

func TestServerAddrBeforeStart(t *testing.T) {
	server := NewServer(WithServerAddr(":4321"))
	if got := server.Addr(); got != ":4321" {
		t.Fatalf("addr = %q", got)
	}
}

func TestServerHeartbeat(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerCredentials(certFile, keyFile),
		WithServerHeartbeatInterval(time.Second),
		WithServerHeartbeatMechanism(RespHeartbeat),
	)
	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := &wt.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test certificate
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	t.Cleanup(func() { _ = transport.Close() })

	_, session, err := transport.Dial(ctx, "https://"+server.Addr()+"/wt", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = session.CloseWithError(0, "test done") }()

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	heartbeat, err := contract.EncodeFrame(contract.Frame{})
	if err != nil {
		t.Fatalf("encode heartbeat: %v", err)
	}
	if _, err = stream.Write(heartbeat); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}

	if err = stream.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	response, err := contract.DecodeFrame(stream)
	if err != nil {
		t.Fatalf("read heartbeat response: %v", err)
	}
	if response.Opcode != 0 || response.Seq != 0 || len(response.Body) != 0 {
		t.Fatalf("heartbeat response = %+v", response)
	}
}

func TestIdleTimeout(t *testing.T) {
	if got := idleTimeout(0); got != 5*time.Minute {
		t.Fatalf("idle timeout = %v", got)
	}
	if got := idleTimeout(15 * time.Second); got != 45*time.Second {
		t.Fatalf("idle timeout = %v", got)
	}
	if got := idleTimeout(time.Second); got != 30*time.Second {
		t.Fatalf("idle timeout = %v", got)
	}
}

// writeTestCertificate writes a self-signed certificate for the loopback host.
func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err = os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
