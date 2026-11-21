package main

import (
	"net"
	"net/http/httptest"
	"testing"
)

func TestClientIPTrustedProxy(t *testing.T) {
	trusted := parseTrustedProxies([]string{"10.0.0.0/8"})
	request := httptest.NewRequest("GET", "/ws", nil)
	request.RemoteAddr = "10.1.2.3:1234"
	request.Header.Set("X-Forwarded-For", "203.0.113.9, 10.1.2.3")
	if got := clientIP(request, trusted); got == nil || got.String() != "203.0.113.9" {
		t.Fatalf("clientIP = %v", got)
	}
}

func TestClientIPUntrustedProxyIgnoresHeader(t *testing.T) {
	trusted := parseTrustedProxies([]string{"10.0.0.0/8"})
	request := httptest.NewRequest("GET", "/ws", nil)
	request.RemoteAddr = "198.51.100.7:1234"
	request.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := clientIP(request, trusted); got == nil || got.String() != "198.51.100.7" {
		t.Fatalf("clientIP = %v", got)
	}
}

func TestClientIPNoHeader(t *testing.T) {
	request := httptest.NewRequest("GET", "/ws", nil)
	request.RemoteAddr = "[2001:db8::1]:1234"
	if got := clientIP(request, nil); got == nil || got.String() != "2001:db8::1" {
		t.Fatalf("unexpected parse: %v", got)
	}
}

func TestParseTrustedProxiesSkipsInvalid(t *testing.T) {
	networks := parseTrustedProxies([]string{"10.0.0.0/8", "bad", "::1/128"})
	if len(networks) != 2 {
		t.Fatalf("networks = %v", networks)
	}
	if !networks[0].Contains(net.ParseIP("10.1.2.3")) {
		t.Fatal("10/8 missing")
	}
}
