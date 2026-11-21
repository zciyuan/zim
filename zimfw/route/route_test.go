package route

import (
	"context"
	"testing"

	"github.com/dobyte/due/v2/errors"

	"github.com/zciyuan/ZIM/zimfw/multi"
)

func (f *fakeLocator) LocateGate(_ context.Context, uid int64) (string, error) {
	f.locateKey = uid
	return f.locateGID, f.err
}

func (f *fakeLocator) BindGate(_ context.Context, gid string, cid, uid int64) error {
	f.bindGID, f.bindCID, f.bindKey = gid, cid, uid
	f.locateGID = gid
	return f.err
}

func (f *fakeLocator) UnbindGate(_ context.Context, uid int64) error {
	f.unbindKey = uid
	return f.err
}

func TestNewValidation(t *testing.T) {
	if _, err := New(nil, "g1"); !errors.Is(err, ErrLocatorMissing) {
		t.Fatalf("nil locator err = %v", err)
	}
	if _, err := New(&fakeLocator{}, ""); !errors.Is(err, ErrGatewayMissing) {
		t.Fatalf("empty gid err = %v", err)
	}
	router, err := New(&fakeLocator{}, "g1")
	if err != nil || router.Gateway() != "g1" {
		t.Fatalf("New = %v, %v", router, err)
	}
}

func TestBindLocateUnbind(t *testing.T) {
	locator := &fakeLocator{}
	router, _ := New(locator, "g1")
	ctx := context.Background()

	if err := router.Bind(ctx, 77, 10001, multi.PlatformAndroid, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	want := int64(10001<<13 | 2<<8 | 1)
	if locator.bindGID != "g1" || locator.bindCID != 77 || locator.bindKey != want {
		t.Fatalf("bind args = %q/%d/%d", locator.bindGID, locator.bindCID, locator.bindKey)
	}

	locator.locateGID = "g2"
	gid, err := router.Locate(ctx, 10001, multi.PlatformAndroid, 1)
	if err != nil || gid != "g2" || locator.locateKey != want {
		t.Fatalf("Locate = %q, %v; key = %d", gid, err, locator.locateKey)
	}

	if err := router.Unbind(ctx, 10001, multi.PlatformAndroid, 1); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	if locator.unbindKey != want {
		t.Fatalf("unbind key = %d", locator.unbindKey)
	}
}

func TestRouterValidation(t *testing.T) {
	router, _ := New(&fakeLocator{}, "g1")
	ctx := context.Background()

	if err := router.Bind(ctx, 1, 0, multi.PlatformIOS, 1); !errors.Is(err, multi.ErrInvalidUID) {
		t.Fatalf("Bind uid err = %v", err)
	}
	if err := router.Bind(ctx, 1, 1, 99, 1); !errors.Is(err, multi.ErrInvalidPlatform) {
		t.Fatalf("Bind platform err = %v", err)
	}
	if _, err := router.Locate(ctx, 1, 99, 1); !errors.Is(err, multi.ErrInvalidPlatform) {
		t.Fatalf("Locate err = %v", err)
	}
	if err := router.Unbind(ctx, 0, multi.PlatformIOS, 1); !errors.Is(err, multi.ErrInvalidUID) {
		t.Fatalf("Unbind err = %v", err)
	}
}

func TestLocatorErrorPassThrough(t *testing.T) {
	locator := &fakeLocator{err: errors.ErrNotFoundUserLocation}
	router, _ := New(locator, "g1")
	if _, err := router.Locate(context.Background(), 1, multi.PlatformIOS, 1); !errors.Is(err, errors.ErrNotFoundUserLocation) {
		t.Fatalf("err = %v", err)
	}
}
