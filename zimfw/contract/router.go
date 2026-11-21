package contract

import (
	"github.com/dobyte/due/v2/errors"
)

// ErrRouteAlreadyRegistered reports a duplicate route registration.
var ErrRouteAlreadyRegistered = errors.New("route already registered")

// ErrRouteNotRegistered reports a missing route registration.
var ErrRouteNotRegistered = errors.New("route not registered")

// ErrPushRoute reports an attempt to register a server push as a request.
var ErrPushRoute = errors.New("push route cannot be registered as a handler")

// NewRouter returns an empty route table.
func NewRouter() *Router {
	return &Router{routes: make(map[int32]HandlerFunc)}
}

// Register adds one handler. Push and infrastructure opcodes are rejected.
func (r *Router) Register(opcode int32, handler HandlerFunc) error {
	if handler == nil {
		return errors.ErrInvalidArgument
	}
	if IsPush(opcode) || IsInfra(opcode) {
		return ErrPushRoute
	}
	if _, ok := SegmentOf(opcode); !ok {
		return errors.ErrInvalidArgument
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.routes[opcode]; ok {
		return ErrRouteAlreadyRegistered
	}
	r.routes[opcode] = handler
	return nil
}

// MustRegister registers handler and panics on a programming error. It is for
// startup wiring only.
func (r *Router) MustRegister(opcode int32, handler HandlerFunc) {
	if err := r.Register(opcode, handler); err != nil {
		panic(err)
	}
}

// Lookup returns the handler for opcode.
func (r *Router) Lookup(opcode int32) (HandlerFunc, error) {
	r.mu.RLock()
	handler, ok := r.routes[opcode]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrRouteNotRegistered
	}
	return handler, nil
}

// Len returns the number of registered routes.
func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.routes)
}
