package server

import (
	"context"
	"io"
	"net/http"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/log"
)

// NewWebhook returns the webhook HTTP component. addr comes from
// etc.zim.voiceroom.webhookAddr, e.g. ":8210".
func NewWebhook(svc *Service, addr string) *Webhook {
	return &Webhook{addr: addr, svc: svc}
}

// Name implements due's component interface.
func (w *Webhook) Name() string { return "voiceroom-webhook" }

// Init implements due's component lifecycle.
func (w *Webhook) Init() {}

// Start launches the net/http listener in a background goroutine.
func (w *Webhook) Start() {
	if w.addr == "" {
		log.Warn("voiceroom webhook disabled: etc.zim.voiceroom.webhookAddr is empty")
		return
	}
	server := &http.Server{
		Addr:              w.addr,
		Handler:           http.HandlerFunc(w.handle),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	w.mu.Lock()
	w.server = server
	w.mu.Unlock()
	go func() {
		log.Infof("voiceroom webhook listening on %s", w.addr)
		if err := server.ListenAndServe(); err != nil && !dueerrors.Is(err, http.ErrServerClosed) {
			log.Errorf("voiceroom webhook failed: %v", err)
		}
	}()
}

// Close shuts the listener down.
func (w *Webhook) Close() {
	w.mu.Lock()
	server := w.server
	w.server = nil
	w.mu.Unlock()
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Warnf("voiceroom webhook shutdown failed: %v", err)
	}
}

// Destroy implements due's component lifecycle.
func (w *Webhook) Destroy() { w.Close() }

// handle verifies the LiveKit signature and feeds the event into the seat
// machine. LiveKit only retries failed deliveries; a 200/204 acknowledges.
func (w *Webhook) handle(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil {
		log.Warnf("voiceroom webhook read failed: err = %v", err)
		response.WriteHeader(http.StatusBadRequest)
		return
	}
	if err = w.svc.HandleWebhook(request.Context(), body, request.Header.Get("Authorization")); err != nil {
		log.Warnf("voiceroom webhook rejected: err = %v", err)
		response.WriteHeader(http.StatusUnauthorized)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}
