package user

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func (f *fakeRow) Scan(dest ...any) error {
	if f.err != nil {
		return f.err
	}
	if !f.apply {
		return nil
	}
	*dest[0].(*int64) = 10001
	*dest[1].(*string) = "alice"
	*dest[2].(*string) = "hash"
	*dest[3].(*string) = "Alice"
	*dest[4].(*string) = "https://example/avatar.png"
	*dest[5].(*int16) = 1
	*dest[6].(*int16) = 0
	*dest[7].(*int64) = 1
	*dest[8].(*int64) = 2
	return nil
}

func (f *fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return f.row
}

func TestFindByUsername(t *testing.T) {
	dao := newWithQuerier(&fakeQuerier{row: &fakeRow{apply: true}})
	user, err := dao.FindByUsername(context.Background(), "alice")
	if err != nil {
		t.Fatalf("FindByUsername: %v", err)
	}
	if user.ID != 10001 || user.Username != "alice" || user.Gender != 1 || user.CreatedAt != 1 {
		t.Fatalf("user = %+v", user)
	}
}

func TestFindByUsernameErrors(t *testing.T) {
	dao := newWithQuerier(&fakeQuerier{row: &fakeRow{err: pgx.ErrNoRows}})
	if _, err := dao.FindByUsername(context.Background(), "alice"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("err = %v", err)
	}

	want := errors.New("db down")
	dao = newWithQuerier(&fakeQuerier{row: &fakeRow{err: want}})
	if _, err := dao.FindByUsername(context.Background(), "alice"); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}
