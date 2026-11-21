package device

import (
	"context"
	"errors"
	"testing"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

type fakeRepository struct {
	upserted    *model.Device
	list        []*model.Device
	targets     []*model.Device
	deactivated bool
	err         error
}

func (f *fakeRepository) Upsert(_ context.Context, device *model.Device) error {
	f.upserted = device
	return f.err
}

func (f *fakeRepository) List(context.Context, int64) ([]*model.Device, error) {
	return f.list, f.err
}

func (f *fakeRepository) Deactivate(context.Context, int64, int16, int64) error {
	f.deactivated = true
	return f.err
}

func (f *fakeRepository) DeactivateSlot(context.Context, int64, int16, int16, int64) error {
	f.deactivated = true
	return f.err
}

func (f *fakeRepository) EnsureSlot(context.Context, int64, int16, string) (int16, error) {
	return 1, f.err
}

func (f *fakeRepository) ListKeys(context.Context, []int64) ([]int64, error) {
	return nil, f.err
}

func (f *fakeRepository) ActiveTargets(context.Context, []int64) ([]*model.Device, error) {
	return f.targets, f.err
}

func TestRegisterDefaults(t *testing.T) {
	repo := &fakeRepository{}
	service := New(repo)
	device := &model.Device{OwnerID: 10001, Platform: 2, DeviceID: "d1"}
	if err := service.Register(context.Background(), device); err != nil {
		t.Fatal(err)
	}
	if repo.upserted == nil || repo.upserted.Status != 0 {
		t.Fatalf("upserted = %+v", repo.upserted)
	}
	if device.CreatedAt == 0 || device.LastActiveAt == 0 {
		t.Fatalf("timestamps = %d/%d", device.CreatedAt, device.LastActiveAt)
	}
}

func TestRegisterValidation(t *testing.T) {
	service := New(&fakeRepository{})
	if err := service.Register(context.Background(), nil); err == nil {
		t.Fatal("nil device must fail")
	}
	if err := service.Register(context.Background(), &model.Device{OwnerID: 0, Platform: 1}); !errors.Is(err, multi.ErrInvalidUID) {
		t.Fatalf("uid err = %v", err)
	}
	if err := service.Register(context.Background(), &model.Device{OwnerID: 1, Platform: 99}); !errors.Is(err, multi.ErrInvalidPlatform) {
		t.Fatalf("platform err = %v", err)
	}
}

func TestRegisterRepoError(t *testing.T) {
	service := New(&fakeRepository{err: errors.New("pg down")})
	if err := service.Register(context.Background(), &model.Device{OwnerID: 1, Platform: 1}); err == nil {
		t.Fatal("repo error must surface")
	}
}

func TestListAndTargets(t *testing.T) {
	repo := &fakeRepository{
		list:    []*model.Device{{OwnerID: 1, Platform: 1}},
		targets: []*model.Device{{OwnerID: 1, Platform: 1, PushToken: "t"}},
	}
	service := New(repo)
	devices, err := service.List(context.Background(), 1)
	if err != nil || len(devices) != 1 {
		t.Fatalf("list = %v, err = %v", devices, err)
	}
	targets, err := service.Targets(context.Background(), []int64{1})
	if err != nil || len(targets) != 1 || targets[0].PushToken != "t" {
		t.Fatalf("targets = %v, err = %v", targets, err)
	}
}

func TestDeactivate(t *testing.T) {
	repo := &fakeRepository{}
	service := New(repo)
	if err := service.Deactivate(context.Background(), 1, 2); err != nil {
		t.Fatal(err)
	}
	if !repo.deactivated {
		t.Fatal("deactivate not called")
	}
	if err := service.Deactivate(context.Background(), 0, 2); !errors.Is(err, multi.ErrInvalidUID) {
		t.Fatalf("uid err = %v", err)
	}
}

func TestDeactivateSlot(t *testing.T) {
	repo := &fakeRepository{}
	service := New(repo)
	if err := service.DeactivateSlot(context.Background(), 10001, 4, 7); err != nil {
		t.Fatal(err)
	}
	if !repo.deactivated {
		t.Fatal("expected the repository to be called")
	}
	if err := service.DeactivateSlot(context.Background(), 10001, 99, 7); !errors.Is(err, multi.ErrInvalidPlatform) {
		t.Fatalf("invalid platform = %v", err)
	}
}
