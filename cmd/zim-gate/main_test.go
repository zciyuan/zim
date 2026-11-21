package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2/etc"

	"github.com/zciyuan/ZIM/internal/component/webtransport"
	"github.com/zciyuan/ZIM/internal/define"
)

func TestUpgradeHandlerRejectsInvalidRequests(t *testing.T) {
	verifyErr := errors.New("bad token")
	cases := []struct {
		name       string
		url        string
		header     string
		verify     upgradeVerifyFunc
		wantStatus int
		wantCode   string
	}{
		{
			name:       "missing token",
			url:        "/ws?platform=2&sdk=1.0.0",
			verify:     func(string, int32) error { return nil },
			wantStatus: http.StatusUnauthorized,
			wantCode:   "100002",
		},
		{
			name:       "bad platform",
			url:        "/ws?token=t&platform=x&sdk=1.0.0",
			verify:     func(string, int32) error { return nil },
			wantStatus: http.StatusBadRequest,
			wantCode:   "900003",
		},
		{
			name:       "missing sdk",
			url:        "/ws?token=t&platform=2",
			verify:     func(string, int32) error { return nil },
			wantStatus: http.StatusBadRequest,
			wantCode:   "900003",
		},
		{
			name:       "wrong protocol",
			url:        "/ws?token=t&platform=2&sdk=1.0.0",
			header:     "zim/2",
			verify:     func(string, int32) error { return nil },
			wantStatus: http.StatusUpgradeRequired,
			wantCode:   "900003",
		},
		{
			name:       "verify fails",
			url:        "/ws?token=t&platform=2&sdk=1.0.0",
			verify:     func(string, int32) error { return verifyErr },
			wantStatus: http.StatusUnauthorized,
			wantCode:   "100002",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, tc.url, nil)
			if tc.header != "" {
				request.Header.Set(define.ProtocolHeader, tc.header)
			}
			handler := newUpgradeHandler(tc.verify)
			if handler(recorder, request) {
				t.Fatal("handler allowed an invalid request")
			}
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tc.wantStatus)
			}
			if got := recorder.Header().Get(codeHeader); got != tc.wantCode {
				t.Fatalf("code header = %q, want %q", got, tc.wantCode)
			}
		})
	}
}

func TestUpgradeHandlerAllowsValidRequest(t *testing.T) {
	var gotToken string
	var gotPlatform int32
	handler := newUpgradeHandler(func(raw string, platform int32) error {
		gotToken, gotPlatform = raw, platform
		return nil
	})
	request := httptest.NewRequest(http.MethodGet, "/ws?token=tok&platform=3&sdk=1.0.0", nil)
	request.Header.Set(define.ProtocolHeader, define.Protocol)
	recorder := httptest.NewRecorder()

	if !handler(recorder, request) {
		t.Fatal("handler rejected a valid request")
	}
	if gotToken != "tok" || gotPlatform != 3 {
		t.Fatalf("verify args = %q/%d", gotToken, gotPlatform)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestUpgradeHandlerAcceptsMissingProtocolHeader(t *testing.T) {
	handler := newUpgradeHandler(func(string, int32) error { return nil })
	request := httptest.NewRequest(http.MethodGet, "/ws?token=tok&platform=3&sdk=1.0.0", nil)
	recorder := httptest.NewRecorder()
	if !handler(recorder, request) {
		t.Fatal("missing X-Protocol must be tolerated for browser clients")
	}
}

func TestNewServer(t *testing.T) {
	server := newServer()
	if server == nil {
		t.Fatal("nil server")
	}
	if got := server.Addr(); got != ":3553" {
		t.Fatalf("Addr = %q", got)
	}
}

func TestNewServerWebTransport(t *testing.T) {
	original := etc.Get("etc.network.transport", "ws").String()
	if err := etc.Set("etc.network.transport", "webtransport"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etc.Set("etc.network.transport", original) })

	server := newServer()
	if server.Protocol() != "webtransport" {
		t.Fatalf("protocol = %q", server.Protocol())
	}
	if got := server.Addr(); got != ":12001" {
		t.Fatalf("Addr = %q", got)
	}
	if _, ok := server.(webtransport.Server); !ok {
		t.Fatalf("server type = %T", server)
	}
}

func TestOriginAllowed(t *testing.T) {
	cases := []struct {
		origins string
		origin  string
		want    bool
	}{
		{"*", "https://a.example", true},
		{"*", "", true},
		{"https://a.example, https://b.example", "https://b.example", true},
		{"https://a.example", "https://c.example", false},
		{"https://a.example", "", true},
	}
	for _, tc := range cases {
		if got := originAllowed(tc.origins, tc.origin); got != tc.want {
			t.Fatalf("originAllowed(%q, %q) = %v, want %v", tc.origins, tc.origin, got, tc.want)
		}
	}
}

func TestWriteAuthError(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeAuthError(recorder, http.StatusUnauthorized, 100002, "token invalid")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Body.String(); got == "" {
		t.Fatal("empty body")
	}
}

var _ ws.UpgradeHandler = newUpgradeHandler(nil)
