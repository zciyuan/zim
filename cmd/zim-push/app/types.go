package app

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/component"

	pushoffline "github.com/zciyuan/ZIM/internal/service/pushoffline/server"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	zimroute "github.com/zciyuan/ZIM/zimfw/route"
)

// Pusher is the due push surface used by the app.
type Pusher interface {
	Multicast(ctx context.Context, args *cluster.MulticastArgs) (int64, error)
}

// TargetSource returns the persisted device keys of users.
type TargetSource interface {
	Keys(ctx context.Context, ownerIDs []int64) ([]int64, error)
}

// Options wires the push application.
type Options struct {
	Proxy    Pusher
	Locator  zimroute.BatchLocator
	Consumer pipe.Consumer
	// Consumers are additional same-group toPush consumers in this process.
	// One consumer per process serializes fetch/commit; a small pool matches
	// the Kafka partition parallelism without requiring one pod per partition.
	Consumers        []pipe.Consumer
	Producer         pipe.Producer
	OfflineConsumer  pipe.Consumer
	OfflinePublisher *pushoffline.Publisher
	Offline          *pushoffline.Dispatcher
	Devices          TargetSource
}

// App is the push delivery component.
type App struct {
	component.Base
	proxy            Pusher
	locator          zimroute.BatchLocator
	consumer         pipe.Consumer
	consumers        []pipe.Consumer
	producer         pipe.Producer
	offlineConsumer  pipe.Consumer
	offlinePublisher *pushoffline.Publisher
	offline          *pushoffline.Dispatcher
	devices          TargetSource
	cancel           context.CancelFunc

	initOnce        sync.Once
	cache           *routeCache
	locateBatchSize int
	batchSize       int
	sendConcurrency int
	rpcTimeout      time.Duration
	latencyWarnMs   int64
}

// routeEntry is one cached deviceKey -> gateID mapping.
type routeEntry struct {
	key      int64
	gateID   string
	expireAt time.Time
	element  *list.Element
}

// routeCache is a bounded LRU with a TTL, used as the L1 route cache in §5.4.
type routeCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	now     func() time.Time
	entries map[int64]*routeEntry
	order   *list.List
}
