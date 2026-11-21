package main

import (
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/dobyte/due/network/ws/v2"

	"github.com/zciyuan/ZIM/internal/component/token"
)

func TestWebSocketHandshake(t *testing.T) {
	issuer, err := token.New("0123456789abcdef", time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := issuer.Issue(10001, 1, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}

	addr := freeTCPAddr(t)
	server := ws.NewServer(
		ws.WithServerAddr(addr),
		ws.WithServerPath("/ws"),
		ws.WithServerHeartbeatInterval(0),
	)
	server.OnUpgrade(newUpgradeHandler(func(raw string, platform int32) error {
		_, err := issuer.Verify(raw, platform)
		return err
	}))
	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() { _ = server.Stop() }()

	query := func(rawToken string) string {
		return "ws://" + addr + "/ws?token=" + url.QueryEscape(rawToken) + "&platform=1&sdk=1.0.0"
	}

	first := ws.NewClient(ws.WithClientUrl(query(pair.AccessToken)), ws.WithClientHeartbeatInterval(0))
	firstConn, err := first.Dial()
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	defer func() { _ = firstConn.Close() }()

	second := ws.NewClient(ws.WithClientUrl(query(pair.AccessToken)), ws.WithClientHeartbeatInterval(0))
	secondConn, err := second.Dial()
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer func() { _ = secondConn.Close() }()

	bad := ws.NewClient(ws.WithClientUrl(query("bad-token")), ws.WithClientHeartbeatInterval(0))
	if _, err = bad.Dial(); err == nil {
		t.Fatal("bad token must be rejected")
	}
}

func freeTCPAddr(t *testing.T) string {
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
