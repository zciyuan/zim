package changelog

import (
	"context"
	"errors"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeStore struct {
	entries []*model.ChangeLog
}

func (f *fakeStore) Append(_ context.Context, entry *model.ChangeLog) error {
	f.entries = append(f.entries, entry)
	return nil
}

func (f *fakeStore) List(_ context.Context, uid int64, entity string, since int64, _ int) ([]*model.ChangeLog, error) {
	result := make([]*model.ChangeLog, 0)
	for _, entry := range f.entries {
		if entry.UID == uid && entry.Entity == entity && entry.Version > since {
			result = append(result, entry)
		}
	}
	return result, nil
}

type fakeVersions struct{ next int64 }

func (f *fakeVersions) Next(context.Context, int64) (int64, error) {
	f.next++
	return f.next, nil
}

func TestAppendPairAndList(t *testing.T) {
	store := &fakeStore{}
	service := New(store, &fakeVersions{})
	service.now = func() int64 { return 100 }
	ctx := context.Background()
	if err := service.AppendPair(ctx, 1, 2, model.EntityFriend, "2", model.ChangeInsert); err != nil {
		t.Fatal(err)
	}
	if len(store.entries) != 2 || store.entries[0].Shard != store.entries[1].Shard {
		t.Fatalf("entries = %+v", store.entries)
	}
	items, err := service.List(ctx, 1, model.EntityFriend, 0, 10)
	if err != nil || len(items) != 1 || items[0].State != model.ChangeInsert {
		t.Fatalf("items = %+v, err = %v", items, err)
	}
	if items[0].Version != 1 || items[0].Mtime != 100 {
		t.Fatalf("entry = %+v", items[0])
	}
}

type fakeSnapshotStore struct {
	fakeStore
	snapshot *model.ChangeSnapshot
}

func (f *fakeSnapshotStore) GetSnapshot(context.Context, int64, string) (*model.ChangeSnapshot, error) {
	if f.snapshot == nil {
		return nil, errors.New("no snapshot")
	}
	return f.snapshot, nil
}

func (f *fakeSnapshotStore) PutSnapshot(_ context.Context, snapshot *model.ChangeSnapshot) error {
	f.snapshot = snapshot
	return nil
}

func (f *fakeSnapshotStore) DeleteLogs(context.Context, int64, string, int64) error { return nil }

func (f *fakeSnapshotStore) ListAll(context.Context, int64, string) ([]*model.ChangeLog, error) {
	return f.entries, nil
}

func (f *fakeSnapshotStore) DistinctUIDs(context.Context, string) ([]int64, error) {
	return []int64{1}, nil
}

func TestCompactAndSnapshotList(t *testing.T) {
	store := &fakeSnapshotStore{fakeStore: fakeStore{entries: []*model.ChangeLog{
		{UID: 1, Entity: model.EntityFriend, ObjID: "2", State: model.ChangeUpdate, Version: 1},
		{UID: 1, Entity: model.EntityFriend, ObjID: "3", State: model.ChangeInsert, Version: 2},
		{UID: 1, Entity: model.EntityFriend, ObjID: "2", State: model.ChangeDelete, Version: 3},
	}}}
	service := New(store, &fakeVersions{})
	if err := service.CompactUser(context.Background(), 1, model.EntityFriend); err != nil {
		t.Fatal(err)
	}
	if store.snapshot == nil || store.snapshot.Version != 3 || len(store.snapshot.Objects) != 2 {
		t.Fatalf("snapshot = %+v", store.snapshot)
	}
	entries, err := service.List(context.Background(), 1, model.EntityFriend, 0, 100)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %d, err = %v", len(entries), err)
	}
}
