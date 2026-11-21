package logic

import (
	"context"
	"sync"
	"time"

	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
)

// keyedActor serializes work for one key (currently one uid) on a due Actor.
type keyedActor struct {
	proxy *node.Proxy
	kind  string
	mu    sync.Mutex
	items map[string]*actorEntry
}

type actorEntry struct {
	actor *node.Actor
	refs  int
}

func newKeyedActor(proxy *node.Proxy, kind string) *keyedActor {
	return &keyedActor{proxy: proxy, kind: kind, items: make(map[string]*actorEntry)}
}

func idleProcessor(*node.Actor, ...any) node.Processor { return &node.BaseProcessor{} }

// Do runs fn in the key's actor task queue. Failure to spawn falls back to a
// direct call so login never fails just because actor scheduling is degraded.
func (k *keyedActor) Do(ctx context.Context, key string, fn func() error) error {
	if k == nil || k.proxy == nil {
		return fn()
	}
	k.mu.Lock()
	entry := k.items[key]
	if entry == nil {
		actor, err := k.proxy.Spawn(idleProcessor,
			node.WithActorKind(k.kind),
			node.WithActorID(key),
		)
		if err != nil {
			k.mu.Unlock()
			log.Warnf("spawn actor failed: kind = %s, key = %s, err = %v", k.kind, key, err)
			return fn()
		}
		entry = &actorEntry{actor: actor}
		k.items[key] = entry
		metrics.Default.SetGauge("zim_actor_count", map[string]string{"kind": k.kind}, int64(len(k.items)))
	}
	entry.refs++
	metrics.Default.SetGauge("zim_actor_pending", map[string]string{"kind": k.kind}, int64(entry.refs))
	k.mu.Unlock()

	result := make(chan error, 1)
	err := entry.actor.Invoke(func() { result <- fn() }, true)
	if err != nil {
		k.mu.Lock()
		entry.refs--
		k.mu.Unlock()
		return fn()
	}
	select {
	case callErr := <-result:
		err = callErr
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		metrics.Default.Inc("zim_actor_invoke_total", map[string]string{"kind": k.kind, "result": "error"})
	} else {
		metrics.Default.Inc("zim_actor_invoke_total", map[string]string{"kind": k.kind, "result": "ok"})
	}

	k.mu.Lock()
	entry.refs--
	idle := entry.refs == 0
	k.mu.Unlock()
	if idle {
		time.AfterFunc(30*time.Second, func() {
			k.mu.Lock()
			current := k.items[key]
			if current == entry && entry.refs == 0 {
				delete(k.items, key)
				metrics.Default.SetGauge("zim_actor_count", map[string]string{"kind": k.kind}, int64(len(k.items)))
				metrics.Default.Inc("zim_actor_destroy_total", map[string]string{"kind": k.kind})
				entry.actor.Destroy()
			}
			k.mu.Unlock()
		})
	}
	return err
}
