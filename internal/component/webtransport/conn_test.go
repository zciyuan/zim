package webtransport

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/network"
)

func TestAttr(t *testing.T) {
	a := &attr{}
	a.Set("k", "v")
	if value, ok := a.Get("k"); !ok || value != "v" {
		t.Fatalf("Get = %v/%v", value, ok)
	}
	visited := 0
	a.Visit(func(key, value any) bool {
		visited++
		if key != "k" || value != "v" {
			t.Fatalf("visit = %v/%v", key, value)
		}
		return true
	})
	if visited != 1 {
		t.Fatalf("visited = %d", visited)
	}
	if !a.Del("k") {
		t.Fatal("Del reported false")
	}
	if _, ok := a.Get("k"); ok {
		t.Fatal("key still present")
	}
	a.Set("k", "v")
	a.Clear()
	if _, ok := a.Get("k"); ok {
		t.Fatal("Clear left the key")
	}
}

func TestServerConnBindAndUnbind(t *testing.T) {
	mgr := newTestConnMgr(2)
	conn := newServerConn(mgr, nil, nil)
	if !mgr.add(conn) {
		t.Fatal("add failed")
	}
	if conn.ID() == 0 {
		t.Fatal("id not assigned")
	}
	if conn.UID() != 0 || conn.State() != network.ConnOpened {
		t.Fatalf("initial state = %d/%d", conn.UID(), conn.State())
	}
	if err := conn.Bind(42); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if conn.UID() != 42 {
		t.Fatalf("uid = %d", conn.UID())
	}
	if err := conn.Unbind(); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if conn.UID() != 0 {
		t.Fatalf("uid = %d", conn.UID())
	}

	conn.Attr().Set("platform", 4)
	if value, ok := conn.Attr().Get("platform"); !ok || value != 4 {
		t.Fatalf("attr = %v/%v", value, ok)
	}

	if err := conn.Push(nil); !errors.Is(err, errors.ErrInvalidMessage) {
		t.Fatalf("push nil = %v", err)
	}
	if err := conn.Push(buffer.NewBytes(nil)); !errors.Is(err, errors.ErrInvalidMessage) {
		t.Fatalf("push empty = %v", err)
	}
	if _, err := conn.LocalAddr(); err == nil {
		t.Fatal("LocalAddr without a session must fail")
	}
	if _, err := conn.RemoteAddr(); err == nil {
		t.Fatal("RemoteAddr without a session must fail")
	}
	if _, err := conn.LocalIP(); err == nil {
		t.Fatal("LocalIP without a session must fail")
	}
	if _, err := conn.RemoteIP(); err == nil {
		t.Fatal("RemoteIP without a session must fail")
	}
	if err := conn.CloseIf(conn.ID()); !errors.Is(err, errors.ErrConnectionClosed) {
		t.Fatalf("CloseIf = %v", err)
	}
	if err := conn.Close(true); !errors.Is(err, errors.ErrConnectionClosed) {
		t.Fatalf("force close = %v", err)
	}
	if conn.State() != network.ConnClosed {
		t.Fatalf("state = %d", conn.State())
	}
	if err := conn.Close(false); !errors.Is(err, errors.ErrConnectionNotOpened) {
		t.Fatalf("grace close = %v", err)
	}
	if err := conn.Bind(1); !errors.Is(err, errors.ErrConnectionClosed) {
		t.Fatalf("bind closed = %v", err)
	}
	if err := conn.Unbind(); !errors.Is(err, errors.ErrConnectionClosed) {
		t.Fatalf("unbind closed = %v", err)
	}
	if err := conn.Push(buffer.NewBytes([]byte{1})); !errors.Is(err, errors.ErrConnectionClosed) {
		t.Fatalf("push closed = %v", err)
	}
	mgr.remove(conn)
}

func TestConnMgrLimitAndClose(t *testing.T) {
	mgr := newTestConnMgr(1)
	first := newServerConn(mgr, nil, nil)
	second := newServerConn(mgr, nil, nil)

	if !mgr.add(first) {
		t.Fatal("first add failed")
	}
	if mgr.add(second) {
		t.Fatal("limit must reject the second connection")
	}
	mgr.remove(first)
	if !mgr.add(second) {
		t.Fatal("second add failed after remove")
	}

	mgr.close()
	if mgr.add(first) {
		t.Fatal("closed manager accepted a connection")
	}
	if second.State() != network.ConnClosed {
		t.Fatalf("state = %d", second.State())
	}
	mgr.close()
}

func TestServerHandleUpgradeRejectsNonConnect(t *testing.T) {
	s := &server{opts: defaultServerOptions()}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/wt", nil)
	s.handleUpgrade(&serverRun{}, recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestServerHandleUpgradeRejectsHandler(t *testing.T) {
	s := &server{opts: defaultServerOptions()}
	s.upgradeHandler = func(w http.ResponseWriter, _ *http.Request) bool {
		w.WriteHeader(http.StatusUnauthorized)
		return false
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodConnect, "/wt", nil)
	s.handleUpgrade(&serverRun{}, recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
	}
}

// newTestConnMgr builds a manager with the requested connection limit.
func newTestConnMgr(maxConnNum int) *connMgr {
	opts := defaultServerOptions()
	opts.maxConnNum = maxConnNum
	return newConnMgr(&server{opts: opts})
}
