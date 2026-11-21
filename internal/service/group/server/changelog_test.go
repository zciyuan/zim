package server

import (
	"context"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/internal/service/changelog/server"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

type recordingStore struct {
	entries []*model.ChangeLog
}

func (s *recordingStore) Append(_ context.Context, entry *model.ChangeLog) error {
	s.entries = append(s.entries, entry)
	return nil
}

func (s *recordingStore) List(context.Context, int64, string, int64, int) ([]*model.ChangeLog, error) {
	return nil, nil
}

type recordingVersions struct{ next int64 }

func (v *recordingVersions) Next(context.Context, int64) (int64, error) {
	v.next++
	return v.next, nil
}

func TestCreateGroupWritesMemberLogs(t *testing.T) {
	server := NewServer(nil, newFakeRepo())
	store := &recordingStore{}
	server.SetChangeLog(changelog.New(store, &recordingVersions{}))
	if _, err := server.CreateGroup(context.Background(), &pb.CreateGroupArgs{
		OwnerId: 1, Name: "g", MemberIds: []int64{2},
	}); err != nil {
		t.Fatal(err)
	}
	if len(store.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(store.entries))
	}
	for _, entry := range store.entries {
		if entry.Entity != model.EntityGroupMember || entry.State != model.ChangeInsert {
			t.Fatalf("entry = %+v", entry)
		}
	}
}
