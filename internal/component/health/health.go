// Package health exposes liveness, readiness and version endpoints as a due
// component. Every ZIM process adds it to the container; the listening address
// comes from etc.zim.health.addr.
package health

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	"github.com/zciyuan/ZIM/internal/version"
)

// New returns a health component for the named process.
func New(name string) *Health {
	return &Health{
		name:   name,
		addr:   etc.Get("etc.zim.health.addr").String(),
		pprof:  etc.Get("etc.zim.health.pprof", false).Bool(),
		checks: make(map[string]func(context.Context) error),
	}
}

// Name returns the component name.
func (h *Health) Name() string {
	return "health"
}

// Start starts the HTTP server in the background.
func (h *Health) Start() {
	if h.addr == "" {
		log.Warnf("%s health endpoint disabled: etc.zim.health.addr is empty", h.name)
		return
	}

	server := &http.Server{
		Addr:              h.addr,
		Handler:           h.Handler(),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	h.server = server

	go func() {
		log.Infof("%s health endpoint listening on %s, %s", h.name, h.addr, version.String())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("%s health endpoint failed: %v", h.name, err)
		}
	}()
}

// Close gracefully shuts down the HTTP server.
func (h *Health) Close() {
	if h.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.server.Shutdown(ctx); err != nil {
		log.Warnf("%s health endpoint shutdown failed: %v", h.name, err)
	}
	h.server = nil
}

// Handler returns the health HTTP handler. It is exported for tests and for
// embedding into another HTTP server.
func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", h.ready)
	mux.Handle("/metrics", metrics.Default.Handler())
	if h.pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, version.String())
	})
	return mux
}

// AddCheck registers one readiness probe.
func (h *Health) AddCheck(name string, check func(context.Context) error) {
	h.mu.Lock()
	h.checks[name] = check
	h.mu.Unlock()
}

func (h *Health) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	h.mu.RLock()
	checks := make(map[string]func(context.Context) error, len(h.checks))
	for name, check := range h.checks {
		checks[name] = check
	}
	h.mu.RUnlock()
	failed := make([]string, 0)
	for name, check := range checks {
		if err := check(ctx); err != nil {
			failed = append(failed, name)
		}
	}
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "not ready: %v", failed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
