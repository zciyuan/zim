package health

import (
	"context"
	"net/http"
	"sync"

	"github.com/dobyte/due/v2/component"
)

// Health is the process health component.
type Health struct {
	component.Base
	name   string
	addr   string
	pprof  bool
	server *http.Server
	mu     sync.RWMutex
	checks map[string]func(context.Context) error
}
