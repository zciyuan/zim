package token

import "time"

// Claims is the JWT payload.
type Claims struct {
	UID      int64  `json:"uid"`
	Platform int32  `json:"platform"`
	SDK      string `json:"sdk,omitempty"`
	Type     string `json:"typ"`
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
	JTI      string `json:"jti"`
}

type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Pair is an access/refresh token pair.
type Pair struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

// Issuer signs and verifies tokens.
type Issuer struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
	now        func() time.Time
	random     func([]byte) error
}
