package errs

import (
	"errors"
	"net/http"
	"testing"

	dueerrors "github.com/dobyte/due/v2/errors"
)

func TestErrorBasics(t *testing.T) {
	if ErrPasswordIncorrect.Code().Code() != 100001 {
		t.Fatalf("code = %d", ErrPasswordIncorrect.Code().Code())
	}
	if ErrGroupNotExist.HTTPStatus() != http.StatusNotFound {
		t.Fatalf("status = %d", ErrGroupNotExist.HTTPStatus())
	}
	if ErrGroupNotExist.Message() != "group not exist" {
		t.Fatalf("message = %q", ErrGroupNotExist.Message())
	}
	if got := ErrGroupNotExist.Error(); got != "130001: group not exist" {
		t.Fatalf("error = %q", got)
	}
	if HTTPStatus(nil) != http.StatusOK {
		t.Fatalf("HTTPStatus(nil) = %d", HTTPStatus(nil))
	}
}

func TestErrorCopies(t *testing.T) {
	cause := errors.New("db down")
	withCause := ErrInternal.WithCause(cause)
	if withCause == ErrInternal {
		t.Fatal("WithCause returned the original pointer")
	}
	if !errors.Is(withCause, cause) {
		t.Fatal("cause is not unwrap-able")
	}
	if withCause.Message() != ErrInternal.Message() {
		t.Fatal("WithCause changed the message")
	}

	withMessage := ErrInternal.WithMessage("database down")
	if withMessage.Message() != "database down" {
		t.Fatalf("message = %q", withMessage.Message())
	}
	if withMessage.Code().Message() != "database down" {
		t.Fatalf("code message = %q", withMessage.Code().Message())
	}
	if ErrInternal.Message() != "internal error" {
		t.Fatal("WithMessage mutated the original")
	}
	var nilError *Error
	if nilError.WithCause(cause) != nil || nilError.WithMessage("x") != nil {
		t.Fatal("nil receiver must stay nil")
	}
}

func TestErrorIs(t *testing.T) {
	if !errors.Is(ErrLoginAlias(), ErrUnauthorized) {
		t.Fatal("same code must compare equal")
	}
	if errors.Is(ErrUnauthorized, ErrTokenKicked) {
		t.Fatal("different codes must not compare equal")
	}
	if ErrUnauthorized.Is(nil) {
		t.Fatal("nil target must not match")
	}
}

func ErrLoginAlias() *Error {
	return ErrUnauthorized.WithCause(errors.New("expired"))
}

func TestFrom(t *testing.T) {
	if From(nil) != nil {
		t.Fatal("From(nil) must be nil")
	}
	if got := From(ErrGroupNotExist); got != ErrGroupNotExist {
		t.Fatalf("From known = %p", got)
	}
	wrapped := dueerrors.NewError(dueerrors.Code(ErrGroupNotExist))
	if got := From(wrapped); got.Code().Code() != 130001 || got.HTTPStatus() != http.StatusNotFound {
		t.Fatalf("From due = %+v", got)
	}
	unknown := From(errors.New("boom"))
	if unknown.Code().Code() != 900003 {
		t.Fatalf("unknown code = %d", unknown.Code().Code())
	}
	if !errors.Is(unknown, unknown.Unwrap()) {
		t.Fatal("unknown cause lost")
	}
}

func TestFromUnknownBusinessCode(t *testing.T) {
	custom := New(150099, "custom", http.StatusBadRequest).WithCause(errors.New("cause"))
	got := From(custom)
	if got.Code().Code() != 150099 || got.Message() != "custom" {
		t.Fatalf("got = %+v", got)
	}
}

func TestIsCode(t *testing.T) {
	if !IsCode(ErrLoginAlias(), 100002) {
		t.Fatal("IsCode failed")
	}
	if IsCode(nil, 0) != true {
		t.Fatal("nil error is code 0")
	}
	if IsCode(nil, 100002) {
		t.Fatal("nil error matched non-zero code")
	}
	if IsCode(errors.New("x"), 900003) != true {
		t.Fatal("unknown error should map to internal")
	}
}

func TestStatusBySegment(t *testing.T) {
	cases := map[int]int{
		100001: http.StatusUnauthorized,
		110001: http.StatusNotFound,
		120001: http.StatusConflict,
		130001: http.StatusForbidden,
		140001: http.StatusNotFound,
		150001: http.StatusBadRequest,
		160001: http.StatusConflict,
		170001: http.StatusBadRequest,
		180001: http.StatusConflict,
		190001: http.StatusOK,
		800003: http.StatusBadRequest,
		900003: http.StatusInternalServerError,
	}
	for code, want := range cases {
		if got := statusBySegment(code); got != want {
			t.Fatalf("statusBySegment(%d) = %d, want %d", code, got, want)
		}
	}
	if got := statusBySegment(0); got != http.StatusInternalServerError {
		t.Fatalf("statusBySegment(0) = %d", got)
	}
}

func TestTargetOfflineIsHTTP200(t *testing.T) {
	if ErrTargetOffline.HTTPStatus() != http.StatusOK {
		t.Fatalf("status = %d", ErrTargetOffline.HTTPStatus())
	}
}
