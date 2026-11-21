package changelog

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
)

// Store persists append-only change rows.
type Store interface {
	Append(ctx context.Context, entry *model.ChangeLog) error
	List(ctx context.Context, uid int64, entity string, since int64, limit int) ([]*model.ChangeLog, error)
}

// SnapshotStore is implemented by stores that support compaction.
type SnapshotStore interface {
	GetSnapshot(ctx context.Context, uid int64, entity string) (*model.ChangeSnapshot, error)
	PutSnapshot(ctx context.Context, snapshot *model.ChangeSnapshot) error
	DeleteLogs(ctx context.Context, uid int64, entity string, version int64) error
	ListAll(ctx context.Context, uid int64, entity string) ([]*model.ChangeLog, error)
	DistinctUIDs(ctx context.Context, entity string) ([]int64, error)
}

// VersionSource returns a monotonically increasing per-user version.
type VersionSource interface {
	Next(ctx context.Context, uid int64) (int64, error)
}

// Indexer is implemented by stores that can create their indexes.
type Indexer interface {
	EnsureIndexes(ctx context.Context, entities []string) error
}

// Service appends and reads version logs.
type Service struct {
	store    Store
	versions VersionSource
	now      func() int64
}

// RedisVersionSource is the Redis INCR version source.
type RedisVersionSource struct {
	client redis.UniversalClient
}
