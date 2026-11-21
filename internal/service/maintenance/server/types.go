package maintenance

import "time"

// Service runs maintenance tasks.
type Service struct {
	friends   FriendCleaner
	messages  MessageCleaner
	changes   ChangeCompactor
	calls     CallTimeouter
	retention time.Duration
	now       func() time.Time
}
