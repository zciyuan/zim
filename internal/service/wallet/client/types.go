package wallet

import "github.com/dobyte/due/v2/codes"

// codedError carries a six-digit ZIM code restored from the mesh RPC error
// message. It implements due's Code() seam so errs.From maps it back to the
// canonical *errs.Error.
type codedError struct {
	code  *codes.Code
	cause error
}

// Error returns the stable message.
func (e *codedError) Error() string {
	if e.code == nil {
		return "wallet rpc error"
	}
	return e.code.Message()
}

// Code returns the restored ZIM code.
func (e *codedError) Code() *codes.Code {
	return e.code
}

// Unwrap returns the underlying transport error.
func (e *codedError) Unwrap() error {
	return e.cause
}
