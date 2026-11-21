package changelog

import (
	"context"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/model"
)

// New returns a version-log service.
func New(store Store, versions VersionSource) *Service {
	return &Service{store: store, versions: versions, now: func() int64 { return time.Now().UnixMilli() }}
}

// NewRedisVersionSource returns the Redis INCR version source.
func NewRedisVersionSource(client redis.UniversalClient) *RedisVersionSource {
	return &RedisVersionSource{client: client}
}

// Next implements VersionSource.
func (s *RedisVersionSource) Next(ctx context.Context, uid int64) (int64, error) {
	return s.client.Incr(ctx, "zim:version:"+strconv.FormatInt(uid, 10)).Result()
}

// Current returns the current version without incrementing it.
func (s *RedisVersionSource) Current(ctx context.Context, uid int64) (int64, error) {
	version, err := s.client.Get(ctx, "zim:version:"+strconv.FormatInt(uid, 10)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return version, err
}

// EnsureIndexes creates the store indexes when supported.
func (s *Service) EnsureIndexes(ctx context.Context, entities []string) error {
	if indexer, ok := s.store.(Indexer); ok {
		return indexer.EnsureIndexes(ctx, entities)
	}
	return nil
}

// Append records one entity change.
func (s *Service) Append(ctx context.Context, uid int64, entity, objID, state string) error {
	version, err := s.versions.Next(ctx, uid)
	if err != nil {
		return err
	}
	return s.store.Append(ctx, &model.ChangeLog{
		UID: uid, Shard: shardOf(objID), Entity: entity, ObjID: objID,
		State: state, Version: version, Mtime: s.now(),
	})
}

// AppendPair records a change for two users, used by bidirectional relations.
func (s *Service) AppendPair(ctx context.Context, a, b int64, entity, objID, state string) error {
	if err := s.Append(ctx, a, entity, objID, state); err != nil {
		return err
	}
	if b == a {
		return nil
	}
	return s.Append(ctx, b, entity, objID, state)
}

// List returns changes after since. When a compacted snapshot is newer than
// since, the latest state of every object is returned before the delta.
func (s *Service) List(ctx context.Context, uid int64, entity string, since int64,
	limit int) ([]*model.ChangeLog, error) {
	store, snapshots := s.store.(SnapshotStore)
	if snapshots {
		if snapshot, err := store.GetSnapshot(ctx, uid, entity); err == nil && snapshot.Version > since {
			entries := make([]*model.ChangeLog, 0, len(snapshot.Objects))
			for _, object := range snapshot.Objects {
				entries = append(entries, &model.ChangeLog{
					UID: uid, Entity: entity, ObjID: object.ObjID, State: object.State,
					Version: object.Version, Mtime: object.Mtime,
				})
			}
			since = snapshot.Version
			delta, err := s.store.List(ctx, uid, entity, since, limit)
			if err != nil {
				return nil, err
			}
			return append(entries, delta...), nil
		}
	}
	return s.store.List(ctx, uid, entity, since, limit)
}

// CompactUser folds one user's log into a snapshot and deletes old rows.
func (s *Service) CompactUser(ctx context.Context, uid int64, entity string) error {
	store, ok := s.store.(SnapshotStore)
	if !ok {
		return nil
	}
	entries, err := store.ListAll(ctx, uid, entity)
	if err != nil || len(entries) == 0 {
		return err
	}
	latest := make(map[string]*model.SnapshotObject, len(entries))
	version := int64(0)
	for _, entry := range entries {
		latest[entry.ObjID] = &model.SnapshotObject{
			ObjID: entry.ObjID, State: entry.State, Version: entry.Version, Mtime: entry.Mtime,
		}
		if entry.Version > version {
			version = entry.Version
		}
	}
	objects := make([]*model.SnapshotObject, 0, len(latest))
	for _, object := range latest {
		objects = append(objects, object)
	}
	if err = store.PutSnapshot(ctx, &model.ChangeSnapshot{
		UID: uid, Entity: entity, Version: version, Objects: objects, Mtime: s.now(),
	}); err != nil {
		return err
	}
	return store.DeleteLogs(ctx, uid, entity, version)
}

// CompactAll compacts every user for the given entities.
func (s *Service) CompactAll(ctx context.Context, entities []string) error {
	store, ok := s.store.(SnapshotStore)
	if !ok {
		return nil
	}
	for _, entity := range entities {
		uids, err := store.DistinctUIDs(ctx, entity)
		if err != nil {
			return err
		}
		for _, uid := range uids {
			if err = s.CompactUser(ctx, uid, entity); err != nil {
				return err
			}
		}
	}
	return nil
}

func shardOf(objID string) int64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(objID))
	return int64(hash.Sum32() % 8)
}
