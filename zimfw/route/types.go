package route

// Router maps a user and platform to a due device key and a gateway.
type Router struct {
	locator  Locator
	registry ConnRegistry
	gid      string
}

// Option configures a Router.
type Option func(*Router)

// WithConnRegistry enables the reverse connection index (ADR-0022).
func WithConnRegistry(registry ConnRegistry) Option {
	return func(r *Router) {
		r.registry = registry
	}
}
