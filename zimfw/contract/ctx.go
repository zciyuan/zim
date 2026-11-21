package contract

// HandlerFunc is the single business handler signature.
type HandlerFunc func(ctx *Ctx, req []byte) (res []byte, err error)
