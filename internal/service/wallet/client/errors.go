package wallet

import (
	"strconv"
	"strings"

	"github.com/dobyte/due/v2/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// FromRPC converts a mesh RPC error back into the canonical *errs.Error. The
// wallet server embeds the six-digit ZIM code in the gRPC message as
// "code|message"; unknown shapes degrade to ErrInternal with the original
// error as cause. REST handlers should use this before api.Failure.
func FromRPC(err error) *errs.Error {
	if err == nil {
		return nil
	}
	if business, ok := err.(*errs.Error); ok {
		return business
	}

	st, ok := status.FromError(err)
	if !ok {
		return errs.ErrInternal.WithCause(err)
	}

	code, message, ok := splitCodeMessage(st.Message())
	if !ok {
		return errs.ErrInternal.WithCause(err)
	}
	return errs.From(&codedError{
		code:  codes.NewCode(code, message),
		cause: err,
	})
}

// CodeOf returns the six-digit ZIM code carried by an RPC error, or 900003 for
// anything unrecognizable.
func CodeOf(err error) int {
	if err == nil {
		return errs.CodeOK
	}
	if business, ok := err.(*errs.Error); ok {
		return business.Code().Code()
	}
	st, ok := status.FromError(err)
	if !ok {
		return errs.ErrInternal.Code().Code()
	}
	code, _, ok := splitCodeMessage(st.Message())
	if !ok {
		return errs.ErrInternal.Code().Code()
	}
	return code
}

// splitCodeMessage parses the "code|message" wire format.
func splitCodeMessage(message string) (int, string, bool) {
	index := strings.IndexByte(message, '|')
	if index <= 0 {
		return 0, "", false
	}
	code, err := strconv.Atoi(message[:index])
	if err != nil || code == 0 {
		return 0, "", false
	}
	return code, message[index+1:], true
}
