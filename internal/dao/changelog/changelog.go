// Package changelog is the Mongo append-only version log.
package changelog

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zciyuan/ZIM/internal/model"
)

// New returns a change-log DAO.
func New(db *mongo.Database) *DAO {
	return &DAO{db: db}
}

// EnsureIndexes creates the uid/shard/version index for each entity.
func (d *DAO) EnsureIndexes(ctx context.Context, entities []string) error {
	for _, entity := range entities {
		collection := d.db.Collection(entity + "_change_log")
		if _, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "uid", Value: 1}, {Key: "shard", Value: 1}, {Key: "version", Value: 1}},
		}); err != nil {
			return err
		}
		if _, err := d.db.Collection(entity+"_change_snapshot").Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "uid", Value: 1}}, Options: options.Index().SetUnique(true),
		}); err != nil {
			return err
		}
	}
	return nil
}

// Append inserts one change row.
func (d *DAO) Append(ctx context.Context, entry *model.ChangeLog) error {
	_, err := d.db.Collection(entry.Entity+"_change_log").InsertOne(ctx, bson.M{
		"uid": entry.UID, "shard": entry.Shard, "obj_id": entry.ObjID,
		"state": entry.State, "version": entry.Version, "mtime": entry.Mtime,
	})
	return err
}

// List returns changes after since, ordered by version.
func (d *DAO) List(ctx context.Context, uid int64, entity string, since int64, limit int) ([]*model.ChangeLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	cursor, err := d.db.Collection(entity+"_change_log").Find(ctx,
		bson.M{"uid": uid, "version": bson.M{"$gt": since}},
		options.Find().SetSort(bson.D{{Key: "version", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	entries := make([]*model.ChangeLog, 0, limit)
	for cursor.Next(ctx) {
		entry := &model.ChangeLog{UID: uid, Entity: entity}
		if err = cursor.Decode(entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, cursor.Err()
}

// GetSnapshot returns the compacted snapshot for one user/entity.
func (d *DAO) GetSnapshot(ctx context.Context, uid int64, entity string) (*model.ChangeSnapshot, error) {
	snapshot := &model.ChangeSnapshot{Entity: entity}
	err := d.db.Collection(entity+"_change_snapshot").FindOne(ctx, bson.M{"uid": uid}).Decode(snapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// PutSnapshot upserts one user/entity snapshot.
func (d *DAO) PutSnapshot(ctx context.Context, snapshot *model.ChangeSnapshot) error {
	_, err := d.db.Collection(snapshot.Entity+"_change_snapshot").ReplaceOne(ctx,
		bson.M{"uid": snapshot.UID}, bson.M{
			"uid": snapshot.UID, "version": snapshot.Version,
			"objects": snapshot.Objects, "mtime": snapshot.Mtime,
		}, options.Replace().SetUpsert(true))
	return err
}

// DeleteLogs removes rows at or below version.
func (d *DAO) DeleteLogs(ctx context.Context, uid int64, entity string, version int64) error {
	_, err := d.db.Collection(entity+"_change_log").DeleteMany(ctx,
		bson.M{"uid": uid, "version": bson.M{"$lte": version}})
	return err
}

// ListAll returns all rows for compaction (bounded by 100k).
func (d *DAO) ListAll(ctx context.Context, uid int64, entity string) ([]*model.ChangeLog, error) {
	cursor, err := d.db.Collection(entity+"_change_log").Find(ctx,
		bson.M{"uid": uid},
		options.Find().SetSort(bson.D{{Key: "version", Value: 1}}).SetLimit(100000))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	entries := make([]*model.ChangeLog, 0)
	for cursor.Next(ctx) {
		entry := &model.ChangeLog{UID: uid, Entity: entity}
		if err = cursor.Decode(entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, cursor.Err()
}

// DistinctUIDs returns all users with change rows for an entity.
func (d *DAO) DistinctUIDs(ctx context.Context, entity string) ([]int64, error) {
	values, err := d.db.Collection(entity+"_change_log").Distinct(ctx, "uid", bson.M{})
	if err != nil {
		return nil, err
	}
	uids := make([]int64, 0, len(values))
	for _, value := range values {
		if uid, ok := value.(int64); ok {
			uids = append(uids, uid)
		}
	}
	return uids, nil
}
