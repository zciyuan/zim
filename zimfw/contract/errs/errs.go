// Package errs is the only error package business code may import. It carries
// a six-digit ZIM code, a stable message, and the HTTP status mapped to the
// code. The package is a thin adapter over due's codes/errors so due call sites
// can still inspect the code.
package errs

import (
	"fmt"
	"net/http"

	"github.com/dobyte/due/v2/codes"
	dueerrors "github.com/dobyte/due/v2/errors"
)

var _ error = (*Error)(nil)

// New returns a business error for the given code, message and HTTP status.
func New(code int, message string, status int) *Error {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	return &Error{
		code:    codes.NewCode(code, message),
		message: message,
		status:  status,
	}
}

// Code returns the six-digit ZIM code. It is also the interface due expects
// from errors.Code and codes.Convert.
func (e *Error) Code() *codes.Code {
	if e == nil {
		return codes.OK
	}
	return e.code
}

// Message returns the stable client-facing message.
func (e *Error) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

// HTTPStatus returns the HTTP status mapped to the code.
func (e *Error) HTTPStatus() int {
	if e == nil {
		return http.StatusOK
	}
	return e.status
}

// Unwrap returns the optional cause.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Error implements error. It deliberately omits the cause so logs that print
// the error cannot leak request payloads.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%d: %s", e.code.Code(), e.message)
}

// Is reports whether target carries the same ZIM code.
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	other, ok := target.(*Error)
	return ok && e.code.Code() == other.code.Code()
}

// WithCause returns a copy carrying cause.
func (e *Error) WithCause(cause error) *Error {
	if e == nil {
		return nil
	}
	clone := *e
	clone.cause = cause
	return &clone
}

// WithMessage returns a copy with the client-facing message replaced.
func (e *Error) WithMessage(message string) *Error {
	if e == nil {
		return nil
	}
	clone := *e
	clone.message = message
	clone.code = e.code.WithMessage(message)
	return &clone
}

// From converts any error to a business error. Known ZIM codes keep their
// message and HTTP status; unknown errors become ErrInternal with the original
// error as cause.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	if business, ok := err.(*Error); ok {
		return business
	}

	code := dueerrors.Code(err)
	if code != nil && code.Code() != codes.OK.Code() {
		if known, ok := byCode(code.Code()); ok {
			return known.WithCause(err)
		}
		return &Error{
			code:    code,
			message: code.Message(),
			status:  statusBySegment(code.Code()),
			cause:   err,
		}
	}

	return ErrInternal.WithCause(err)
}

// IsCode reports whether err carries the given ZIM code.
func IsCode(err error, code int) bool {
	if err == nil {
		return code == CodeOK
	}
	business := From(err)
	return business != nil && business.Code().Code() == code
}

// HTTPStatus returns the HTTP status for err.
func HTTPStatus(err error) int {
	business := From(err)
	if business == nil {
		return http.StatusOK
	}
	return business.HTTPStatus()
}
