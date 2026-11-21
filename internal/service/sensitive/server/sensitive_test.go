package sensitive

import (
	"context"
	"testing"
)

type fakeRepo struct{ words []string }

func (f *fakeRepo) List(context.Context) ([]string, error) { return f.words, nil }

func TestCheck(t *testing.T) {
	service := New(&fakeRepo{words: []string{"badword"}})
	blocked, err := service.Check(context.Background(), "this is badword")
	if err != nil || !blocked {
		t.Fatalf("blocked = %v, err = %v", blocked, err)
	}
	blocked, err = service.Check(context.Background(), "clean")
	if err != nil || blocked {
		t.Fatalf("blocked = %v, err = %v", blocked, err)
	}
}
