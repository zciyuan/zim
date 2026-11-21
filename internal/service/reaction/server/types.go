package reaction

import "github.com/zciyuan/ZIM/zimfw/contract/errs"

// Item is one emoji aggregation for a message.
type Item struct {
	Emoji string
	Count int32
	Mine  bool
}

var (
	// ErrInvalidArgument guards empty conversation / non-positive seq or uid.
	ErrInvalidArgument = errs.ErrMessageEmpty
	// ErrInvalidEmoji rejects empty or oversized emoji payloads.
	ErrInvalidEmoji = errs.ErrReactionInvalid
	// ErrTooManyEmojis bounds distinct reactions per message.
	ErrTooManyEmojis = errs.ErrReactionTooMany
)
