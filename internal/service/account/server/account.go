// Package account implements username/password authentication and token
// issuance for the REST login endpoint.
package account

import (
	"context"
	"strings"
	"time"

	"github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

// Errors reported by the service constructor.
var (
	// ErrRepositoryMissing reports a nil user repository.
	ErrRepositoryMissing = errors.New("account repository missing")
	// ErrIssuerMissing reports a nil token issuer.
	ErrIssuerMissing = errors.New("token issuer missing")
)

// Repository reads and creates user credentials.
type Repository interface {
	// FindByUsername returns the user row, or pgx.ErrNoRows when absent.
	FindByUsername(ctx context.Context, username string) (*model.User, error)
	// Create inserts a new user row.
	Create(ctx context.Context, user *model.User) error
	// NextUserID allocates the next sequential account id.
	NextUserID(ctx context.Context) (int64, error)
}

// New returns an account service.
func New(repo Repository, issuer *token.Issuer) (*Service, error) {
	if repo == nil {
		return nil, ErrRepositoryMissing
	}
	if issuer == nil {
		return nil, ErrIssuerMissing
	}
	return &Service{repo: repo, issuer: issuer, now: time.Now}, nil
}

// Login verifies the password and issues an access/refresh pair.
func (s *Service) Login(ctx context.Context, username, password string, platform int32) (*Result, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" || !multi.ValidPlatform(platform) {
		return nil, errs.ErrPasswordIncorrect
	}

	user, err := s.repo.FindByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Do not reveal whether the username exists.
			return nil, errs.ErrPasswordIncorrect
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	if user == nil {
		return nil, errs.ErrPasswordIncorrect
	}
	if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return nil, errs.ErrPasswordIncorrect
	}
	if user.Status == model.StatusDisabled {
		return nil, errs.ErrAccountDisabled
	}

	pair, err := s.issuer.Issue(user.ID, platform, "")
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	return &Result{
		UID:              user.ID,
		AccessToken:      pair.AccessToken,
		RefreshToken:     pair.RefreshToken,
		ExpiresIn:        pair.ExpiresIn,
		RefreshExpiresIn: pair.RefreshExpiresIn,
	}, nil
}
