package account

import (
	"time"

	"github.com/zciyuan/ZIM/internal/component/token"
)

// Result is a successful password login.
type Result struct {
	UID              int64  `json:"uid"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

// Service authenticates users and issues tokens.
type Service struct {
	repo   Repository
	issuer *token.Issuer
	now    func() time.Time
}
