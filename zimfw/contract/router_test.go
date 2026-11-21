package contract

import (
	"context"
	"sync"
	"testing"

	"github.com/dobyte/due/v2/errors"
)

func TestRouterRegisterAndLookup(t *testing.T) {
	router := NewRouter()
	handler := func(*Ctx, []byte) ([]byte, error) { return []byte("ok"), nil }
	if err := router.Register(OpLogin, handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if router.Len() != 1 {
		t.Fatalf("Len = %d", router.Len())
	}
	got, err := router.Lookup(OpLogin)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	res, _ := got(&Ctx{Context: context.Background()}, nil)
	if string(res) != "ok" {
		t.Fatalf("res = %q", res)
	}
}

func TestRouterErrors(t *testing.T) {
	router := NewRouter()
	handler := func(*Ctx, []byte) ([]byte, error) { return nil, nil }

	if err := router.Register(OpLogin, nil); !errors.Is(err, errors.ErrInvalidArgument) {
		t.Fatalf("nil handler err = %v", err)
	}
	if err := router.Register(OpPushMsg, handler); !errors.Is(err, ErrPushRoute) {
		t.Fatalf("push route err = %v", err)
	}
	if err := router.Register(OpRateLimited, handler); !errors.Is(err, ErrPushRoute) {
		t.Fatalf("infra route err = %v", err)
	}
	if err := router.Register(1, handler); !errors.Is(err, errors.ErrInvalidArgument) {
		t.Fatalf("unknown segment err = %v", err)
	}
	if err := router.Register(OpLogin, handler); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := router.Register(OpLogin, handler); !errors.Is(err, ErrRouteAlreadyRegistered) {
		t.Fatalf("duplicate err = %v", err)
	}
	if _, err := router.Lookup(OpLogout); !errors.Is(err, ErrRouteNotRegistered) {
		t.Fatalf("lookup err = %v", err)
	}
}

func TestRouterConcurrentLookup(t *testing.T) {
	router := NewRouter()
	handler := func(*Ctx, []byte) ([]byte, error) { return nil, nil }
	if err := router.Register(OpLogin, handler); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := router.Lookup(OpLogin); err != nil {
					t.Errorf("Lookup: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRouterMustRegisterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewRouter().MustRegister(OpLogin, nil)
}
