package voiceroom

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/zciyuan/ZIM/internal/model"
)

type fakeRow struct {
	values []any
	err    error
}

func (f *fakeRow) Scan(dest ...any) error {
	if f.err != nil {
		return f.err
	}
	for i, value := range f.values {
		if i >= len(dest) {
			break
		}
		switch target := dest[i].(type) {
		case *int64:
			*target = value.(int64)
		case *int16:
			*target = value.(int16)
		case *string:
			*target = value.(string)
		}
	}
	return nil
}

type fakeRows struct {
	rows    [][]any
	index   int
	scanErr error
	nextErr error
}

func (f *fakeRows) Close()                                       {}
func (f *fakeRows) Err() error                                   { return f.nextErr }
func (f *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (f *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (f *fakeRows) Next() bool {
	if f.index >= len(f.rows) {
		return false
	}
	f.index++
	return true
}
func (f *fakeRows) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	values := f.rows[f.index-1]
	for i, value := range values {
		if i >= len(dest) {
			break
		}
		switch target := dest[i].(type) {
		case *int64:
			*target = value.(int64)
		case *int16:
			*target = value.(int16)
		case *string:
			*target = value.(string)
		}
	}
	return nil
}
func (f *fakeRows) Values() ([]any, error) { return nil, nil }
func (f *fakeRows) RawValues() [][]byte    { return nil }
func (f *fakeRows) Conn() *pgx.Conn        { return nil }
func (f *fakeRows) TypeMap() *pgtype.Map   { return nil }

type fakePool struct {
	execErr  error
	queryErr error
	row      pgx.Row
	rows     pgx.Rows
	sql      string
	args     []any
}

func (f *fakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.sql, f.args = sql, args
	return pgconn.CommandTag{}, f.execErr
}

func (f *fakePool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.sql, f.args = sql, args
	return f.row
}

func (f *fakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.sql, f.args = sql, args
	return f.rows, f.queryErr
}

func roomValues(roomID int64, status int16) []any {
	return []any{roomID, int16(2), int64(0), int64(1001), "room", "", int16(8), int16(1), "", status, int64(10), int64(0)}
}

func TestCreateAndGet(t *testing.T) {
	pool := &fakePool{}
	dao := newWithPool(pool)
	ctx := context.Background()
	room := &model.VoiceRoom{
		RoomID: 9001, RoomType: model.VoiceRoomTypePublic, OwnerUID: 1001,
		Title: "hello", SeatCount: 8, JoinSeatMode: 1, Status: model.VoiceRoomStatusLive, CreatedAt: 10,
	}
	if err := dao.Create(ctx, room); err != nil {
		t.Fatal(err)
	}
	if pool.sql != createSQL || len(pool.args) != 12 {
		t.Fatalf("sql = %q, args = %v", pool.sql, pool.args)
	}
	pool.row = &fakeRow{values: roomValues(9001, model.VoiceRoomStatusLive)}
	got, err := dao.Get(ctx, 9001)
	if err != nil {
		t.Fatal(err)
	}
	if got.RoomID != 9001 || got.Title != "room" || got.Status != model.VoiceRoomStatusLive {
		t.Fatalf("got = %+v", got)
	}
	pool.row = &fakeRow{err: pgx.ErrNoRows}
	if _, err = dao.Get(ctx, 999); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing room err = %v", err)
	}
	want := errors.New("db down")
	pool.row = &fakeRow{err: want}
	if _, err = dao.Get(ctx, 9001); !errors.Is(err, want) {
		t.Fatalf("db error = %v", err)
	}
}

func TestUpdateStatus(t *testing.T) {
	pool := &fakePool{}
	dao := newWithPool(pool)
	if err := dao.UpdateStatus(context.Background(), 9001, model.VoiceRoomStatusClosed, 99); err != nil {
		t.Fatal(err)
	}
	if pool.sql != updateStatusSQL {
		t.Fatalf("sql = %q", pool.sql)
	}
	pool.execErr = errors.New("down")
	if err := dao.UpdateStatus(context.Background(), 9001, model.VoiceRoomStatusClosed, 99); !errors.Is(err, pool.execErr) {
		t.Fatalf("err = %v", err)
	}
}

func TestListByGroupAndPublic(t *testing.T) {
	pool := &fakePool{}
	dao := newWithPool(pool)
	ctx := context.Background()

	pool.rows = &fakeRows{rows: [][]any{
		roomValues(9001, model.VoiceRoomStatusLive),
		roomValues(9002, model.VoiceRoomStatusClosed),
	}}
	rooms, err := dao.ListByGroup(ctx, 100, 10, 0)
	if err != nil || len(rooms) != 2 || rooms[0].RoomID != 9001 {
		t.Fatalf("rooms = %+v, err = %v", rooms, err)
	}
	if pool.sql != listByGroupSQL || pool.args[0].(int64) != 100 {
		t.Fatalf("sql = %q, args = %v", pool.sql, pool.args)
	}

	pool.rows = &fakeRows{rows: [][]any{roomValues(9003, model.VoiceRoomStatusLive)}}
	rooms, err = dao.ListPublic(ctx, 1, 0)
	if err != nil || len(rooms) != 1 {
		t.Fatalf("public rooms = %+v, err = %v", rooms, err)
	}
	if pool.sql != listPublicSQL {
		t.Fatalf("sql = %q", pool.sql)
	}

	pool.queryErr = errors.New("down")
	if _, err = dao.ListByGroup(ctx, 100, 10, 0); !errors.Is(err, pool.queryErr) {
		t.Fatalf("query err = %v", err)
	}

	pool.queryErr = nil
	pool.rows = &fakeRows{rows: [][]any{roomValues(9004, 1)}, scanErr: errors.New("bad scan")}
	if _, err = dao.ListPublic(ctx, 10, 0); !errors.Is(err, pool.rows.(*fakeRows).scanErr) {
		t.Fatalf("scan err = %v", err)
	}
	pool.rows = &fakeRows{nextErr: errors.New("rows err")}
	if _, err = dao.ListPublic(ctx, 10, 0); !errors.Is(err, pool.rows.(*fakeRows).nextErr) {
		t.Fatalf("rows err = %v", err)
	}
}
