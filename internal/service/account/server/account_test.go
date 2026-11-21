package account

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

func (f *fakeRepo) FindByUsername(context.Context, string) (*model.User, error) {
	return f.user, f.err
}

func (f *fakeRepo) Create(_ context.Context, user *model.User) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created = user
	return nil
}

func (f *fakeRepo) NextUserID(context.Context) (int64, error) {
	if f.nextErr != nil {
		return 0, f.nextErr
	}
	if f.nextID == 0 {
		return 90001, nil
	}
	return f.nextID, nil
}

func newIssuer(t *testing.T) *token.Issuer {
	t.Helper()
	issuer, err := token.New("0123456789abcdef", time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}

func passwordHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}

func TestLoginSuccess(t *testing.T) {
	repo := &fakeRepo{user: &model.User{ID: 10001, PasswordHash: passwordHash(t, "secret"), Status: 0}}
	service, err := New(repo, newIssuer(t))
	if err != nil {
		t.Fatal(err)
	}
	res, err := service.Login(context.Background(), "alice", "secret", 2)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.UID != 10001 || res.AccessToken == "" || res.RefreshToken == "" || res.ExpiresIn == 0 {
		t.Fatalf("res = %+v", res)
	}
}

func TestLoginFailures(t *testing.T) {
	validHash := passwordHash(t, "secret")
	cases := []struct {
		name     string
		repo     *fakeRepo
		username string
		password string
		platform int32
		want     error
	}{
		{name: "empty username", repo: &fakeRepo{}, username: "", password: "x", platform: 1, want: errs.ErrPasswordIncorrect},
		{name: "empty password", repo: &fakeRepo{}, username: "alice", password: "", platform: 1, want: errs.ErrPasswordIncorrect},
		{name: "bad platform", repo: &fakeRepo{}, username: "alice", password: "x", platform: 9, want: errs.ErrPasswordIncorrect},
		{name: "not found", repo: &fakeRepo{err: pgx.ErrNoRows}, username: "alice", password: "secret", platform: 1, want: errs.ErrPasswordIncorrect},
		{name: "nil user", repo: &fakeRepo{}, username: "alice", password: "secret", platform: 1, want: errs.ErrPasswordIncorrect},
		{name: "wrong password", repo: &fakeRepo{user: &model.User{ID: 1, PasswordHash: validHash}}, username: "alice", password: "bad", platform: 1, want: errs.ErrPasswordIncorrect},
		{name: "disabled", repo: &fakeRepo{user: &model.User{ID: 1, PasswordHash: validHash, Status: model.StatusDisabled}}, username: "alice", password: "secret", platform: 1, want: errs.ErrAccountDisabled},
		{name: "repo error", repo: &fakeRepo{err: errors.New("db down")}, username: "alice", password: "secret", platform: 1, want: errs.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, err := New(tc.repo, newIssuer(t))
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Login(context.Background(), tc.username, tc.password, tc.platform)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestConstructorValidation(t *testing.T) {
	if _, err := New(nil, newIssuer(t)); !errors.Is(err, ErrRepositoryMissing) {
		t.Fatalf("repo err = %v", err)
	}
	if _, err := New(&fakeRepo{}, nil); !errors.Is(err, ErrIssuerMissing) {
		t.Fatalf("issuer err = %v", err)
	}
}
