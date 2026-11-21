package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler(t *testing.T) {
	component := New("zim-test")
	server := httptest.NewServer(component.Handler())
	defer server.Close()

	cases := []struct {
		path   string
		status int
	}{
		{"/healthz", http.StatusOK},
		{"/readyz", http.StatusOK},
		{"/version", http.StatusOK},
		{"/missing", http.StatusNotFound},
	}
	for _, tc := range cases {
		res, err := http.Get(server.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		if res.StatusCode != tc.status {
			t.Fatalf("GET %s = %d, want %d", tc.path, res.StatusCode, tc.status)
		}
		_ = res.Body.Close()
	}
}

func TestPprofHandler(t *testing.T) {
	component := New("zim-test")
	if component.pprof {
		t.Fatal("pprof must be disabled by default")
	}
	recorder := httptest.NewRecorder()
	component.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("disabled status = %d", recorder.Code)
	}

	component.pprof = true
	recorder = httptest.NewRecorder()
	component.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestName(t *testing.T) {
	if got := New("zim-test").Name(); got != "health" {
		t.Fatalf("Name = %q", got)
	}
}

func TestStartAndClose(t *testing.T) {
	component := New("zim-test")
	component.addr = "127.0.0.1:18099"
	component.Start()
	component.Close()
	component.Close()
}

func TestStartDisabled(t *testing.T) {
	component := New("zim-test")
	component.addr = ""
	component.Start()
	component.Close()
}

func TestReadinessChecks(t *testing.T) {
	h := New("test")
	h.AddCheck("ok", func(context.Context) error { return nil })
	recorder := httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	h.AddCheck("bad", func(context.Context) error { return errors.New("down") })
	recorder = httptest.NewRecorder()
	h.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
}
