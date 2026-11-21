package account

import (
	"context"
	"regexp"
	"strings"
	"unicode/utf8"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"

	"github.com/zciyuan/ZIM/internal/model"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
	"github.com/zciyuan/ZIM/zimfw/multi"
)

const (
	// minPasswordRunes / maxPasswordRunes bound the password length (bcrypt
	// silently truncates at 72 bytes, so 64 runes is a safe upper bound).
	minPasswordRunes = 8
	maxPasswordRunes = 64
	// maxNicknameRunes matches the profile update rule.
	maxNicknameRunes = 32
	// uniqueViolation is the PostgreSQL SQLSTATE for a duplicate key.
	uniqueViolation = "23505"
)

// usernamePattern keeps usernames login-safe: a letter, then letters/digits/_.
var usernamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{2,31}$`)

// RegisterRequest is the input of Service.Register.
type RegisterRequest struct {
	Username string
	Password string
	Nickname string
	Platform int32
}

// Register creates an account and issues a token pair, so the sign-up screen
// can enter the app in one round trip.
func (s *Service) Register(ctx context.Context, req RegisterRequest) (*Result, error) {
	username := strings.TrimSpace(req.Username)
	nickname := strings.TrimSpace(req.Nickname)
	if !usernamePattern.MatchString(username) {
		return nil, errs.ErrUsernameInvalid
	}
	if !validPassword(req.Password) {
		return nil, errs.ErrPasswordInvalid
	}
	if !multi.ValidPlatform(req.Platform) {
		return nil, errs.ErrPasswordIncorrect
	}
	if nickname == "" {
		nickname = username
	}
	if utf8.RuneCountInString(nickname) > maxNicknameRunes {
		return nil, errs.ErrNicknameInvalid
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}

	now := s.now().UnixMilli()
	uid, err := s.repo.NextUserID(ctx)
	if err != nil {
		return nil, errs.ErrInternal.WithCause(err)
	}
	user := &model.User{
		ID:           uid,
		Username:     username,
		PasswordHash: string(hash),
		Nickname:     nickname,
		Status:       0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.repo.Create(ctx, user); err != nil {
		var pgErr *pgconn.PgError
		if dueerrors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return nil, errs.ErrUsernameTaken
		}
		return nil, errs.ErrInternal.WithCause(err)
	}

	pair, err := s.issuer.Issue(user.ID, req.Platform, "")
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

// validPassword checks the rune count and rejects all-whitespace passwords.
func validPassword(password string) bool {
	trimmed := strings.TrimSpace(password)
	if trimmed == "" {
		return false
	}
	count := utf8.RuneCountInString(password)
	return count >= minPasswordRunes && count <= maxPasswordRunes
}
