package errs

import "github.com/dobyte/due/v2/codes"

// Error is an immutable business error.
type Error struct {
	code    *codes.Code
	message string
	status  int
	cause   error
}
