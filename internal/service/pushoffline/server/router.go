package pushoffline

import (
	"context"
	"sort"
)

// Router dispatches each notification to the provider selected by the device
// binding, then by platform mapping, then by the default provider.
type Router struct {
	defaultName string
	providers   map[string]Provider
	platforms   map[int16]string
}

// NewRouter returns a provider router. providers must contain the default when
// defaultName is non-empty.
func NewRouter(defaultName string, providers map[string]Provider, platforms map[int16]string) *Router {
	if platforms == nil {
		platforms = map[int16]string{}
	}
	return &Router{defaultName: defaultName, providers: providers, platforms: platforms}
}

// Name returns the router name used in logs.
func (r *Router) Name() string { return "router" }

// Send groups notifications by provider and preserves the input order.
func (r *Router) Send(ctx context.Context, notifications []Notification) ([]Result, error) {
	results := make([]Result, len(notifications))
	groups := make(map[string][]int)
	for index, notification := range notifications {
		name := r.providerName(notification)
		groups[name] = append(groups[name], index)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		provider := r.providers[name]
		indexes := groups[name]
		if provider == nil {
			for _, index := range indexes {
				results[index] = Result{Token: notifications[index].Token, Err: ErrProviderNotConfigured}
			}
			continue
		}
		batch := make([]Notification, 0, len(indexes))
		for _, index := range indexes {
			batch = append(batch, notifications[index])
		}
		batchResults, err := provider.Send(ctx, batch)
		if err != nil {
			for _, index := range indexes {
				results[index] = Result{Token: notifications[index].Token, Err: err}
			}
			continue
		}
		if len(batchResults) != len(batch) {
			for _, index := range indexes {
				results[index] = Result{Token: notifications[index].Token, Err: ErrProviderResultMismatch}
			}
			continue
		}
		for offset, index := range indexes {
			results[index] = batchResults[offset]
		}
	}
	return results, nil
}

func (r *Router) providerName(notification Notification) string {
	if notification.Provider != "" {
		if _, ok := r.providers[notification.Provider]; ok {
			return notification.Provider
		}
	}
	if name := r.platforms[notification.Platform]; name != "" {
		if _, ok := r.providers[name]; ok {
			return name
		}
	}
	return r.defaultName
}
