package account

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

func TestRegisterCreatesAccountAndIssuesTokens(t *testing.T) {
	repo := &fakeRepo{}
	service, err := New(repo, newIssuer(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := service.Register(context.Background(), RegisterRequest{
		Username: "  carol_1  ",
		Password: "password123",
		Nickname: " Carol ",
		Platform: multi.PlatformWeb,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if result.UID <= 0 || result.AccessToken == "" || result.RefreshToken == "" {
		t.Fatalf("result = %+v", result)
	}
	if repo.created == nil {
		t.Fatal("repository was not called")
	}
	if repo.created.Username != "carol_1" || repo.created.Nickname != "Carol" {
		t.Fatalf("created = %+v", repo.created)
	}
	if repo.created.PasswordHash == "password123" || repo.created.PasswordHash == "" {
		t.Fatal("password must be hashed")
	}
	if repo.created.ID != result.UID {
		t.Fatalf("id = %d, uid = %d", repo.created.ID, result.UID)
	}
}

func TestRegisterDefaultsNicknameToUsername(t *testing.T) {
	repo := &fakeRepo{}
	service, _ := New(repo, newIssuer(t))

	if _, err := service.Register(context.Background(), RegisterRequest{
		Username: "dave",
		Password: "password123",
		Platform: multi.PlatformIOS,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if repo.created == nil || repo.created.Nickname != "dave" {
		t.Fatalf("nickname = %+v", repo.created)
	}
}

func TestRegisterValidation(t *testing.T) {
	service, _ := New(&fakeRepo{}, newIssuer(t))
	cases := []struct {
		name string
		req  RegisterRequest
		want error
	}{
		{"short username", RegisterRequest{Username: "ab", Password: "password123", Platform: 4}, errs.ErrUsernameInvalid},
		{"bad first char", RegisterRequest{Username: "1abc", Password: "password123", Platform: 4}, errs.ErrUsernameInvalid},
		{"illegal char", RegisterRequest{Username: "ab-cd", Password: "password123", Platform: 4}, errs.ErrUsernameInvalid},
		{"short password", RegisterRequest{Username: "abcd", Password: "short", Platform: 4}, errs.ErrPasswordInvalid},
		{"blank password", RegisterRequest{Username: "abcd", Password: "        ", Platform: 4}, errs.ErrPasswordInvalid},
		{"bad platform", RegisterRequest{Username: "abcd", Password: "password123", Platform: 0}, errs.ErrPasswordIncorrect},
		{"long nickname", RegisterRequest{Username: "abcd", Password: "password123", Nickname: string(make([]rune, 40)), Platform: 4}, errs.ErrNicknameInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := service.Register(context.Background(), tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRegisterDuplicateUsername(t *testing.T) {
	repo := &fakeRepo{createErr: &pgconn.PgError{Code: uniqueViolation}}
	service, _ := New(repo, newIssuer(t))

	_, err := service.Register(context.Background(), RegisterRequest{
		Username: "taken",
		Password: "password123",
		Platform: 4,
	})
	if !errors.Is(err, errs.ErrUsernameTaken) {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterRepositoryFailure(t *testing.T) {
	repo := &fakeRepo{createErr: errors.New("db down")}
	service, _ := New(repo, newIssuer(t))

	_, err := service.Register(context.Background(), RegisterRequest{
		Username: "alice2",
		Password: "password123",
		Platform: 4,
	})
	if err == nil || errors.Is(err, errs.ErrUsernameTaken) {
		t.Fatalf("err = %v, want internal", err)
	}
}
