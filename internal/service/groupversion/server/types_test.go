// Package groupversion tests version counters with a fake store.
package groupversion

import "context"

type fakeStore struct {
	member int64
	join   int64
}

func (f *fakeStore) IncrMember(context.Context, int64) (int64, error) {
	f.member++
	return f.member, nil
}
func (f *fakeStore) IncrJoin(context.Context, int64) (int64, error) {
	f.join++
	return f.join, nil
}
func (f *fakeStore) Member(context.Context, int64) (int64, error) { return f.member, nil }
func (f *fakeStore) Join(context.Context, int64) (int64, error)   { return f.join, nil }

var _ Store = (*fakeStore)(nil)
