package logic

import (
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
)

func TestSyncChanges(t *testing.T) {
	entries := []*model.ChangeLog{
		{ObjID: "2", State: model.ChangeInsert, Version: 3, Mtime: 100},
		{ObjID: "2", State: model.ChangeUpdate, Version: 5, Mtime: 101},
	}
	changes, version := syncChanges(entries, 2)
	if version != 5 || len(changes) != 2 {
		t.Fatalf("version = %d, changes = %d", version, len(changes))
	}
	if changes[0].GetState() != model.ChangeInsert || changes[1].GetVersion() != 5 {
		t.Fatalf("changes = %+v", changes)
	}
	if _, emptyVersion := syncChanges(nil, 9); emptyVersion != 9 {
		t.Fatalf("empty version = %d", emptyVersion)
	}
}
