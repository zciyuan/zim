// Package token implements the ZIM access/refresh token format.
//
// The wire format is JWT (HS256) so standard tooling can inspect it, but the
// implementation is intentionally small and uses only the standard library
// plus due's JSON codec. It carries the claims ZIM needs: uid, platform, sdk,
// type and expiry.
package token

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"

	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"
)

// Token types.
const (
	TypeAccess  = "access"
	TypeRefresh = "refresh"
)

// Errors returned by the token package. They are infrastructure errors; the
// auth service maps them to errs.ErrUnauthorized or errs.ErrPasswordIncorrect.
var (
	// ErrInvalidToken reports a malformed or tampered token.
	ErrInvalidToken = errors.New("invalid token")
	// ErrExpiredToken reports an expired token.
	ErrExpiredToken = errors.New("expired token")
	// ErrWrongTokenType reports an access token used as refresh or vice versa.
	ErrWrongTokenType = errors.New("wrong token type")
	// ErrPlatformMismatch reports a token issued for another platform.
	ErrPlatformMismatch = errors.New("token platform mismatch")
	// ErrWeakSecret reports a secret shorter than 16 bytes.
	ErrWeakSecret = errors.New("token secret too short")
)

// New returns an issuer. Secret must be at least 16 bytes.
func New(secret string, accessTTL, refreshTTL time.Duration) (*Issuer, error) {
	if len(secret) < 16 {
		return nil, ErrWeakSecret
	}
	if accessTTL <= 0 || refreshTTL <= 0 {
		return nil, errors.New("token ttl must be positive")
	}
	return &Issuer{
		secret:     []byte(secret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		now:        time.Now,
		random:     fillRandom,
	}, nil
}

// NewWithClock is the test seam for deterministic expiry checks.
func NewWithClock(secret string, accessTTL, refreshTTL time.Duration, now func() time.Time) (*Issuer, error) {
	issuer, err := New(secret, accessTTL, refreshTTL)
	if err != nil {
		return nil, err
	}
	if now != nil {
		issuer.now = now
	}
	return issuer, nil
}

// Issue creates a new access/refresh pair.
func (i *Issuer) Issue(uid int64, platform int32, sdk string) (*Pair, error) {
	if uid <= 0 {
		return nil, ErrInvalidToken
	}
	now := i.now()
	accessJTI, err := i.jti()
	if err != nil {
		return nil, err
	}
	refreshJTI, err := i.jti()
	if err != nil {
		return nil, err
	}

	access, err := i.sign(Claims{
		UID: uid, Platform: platform, SDK: sdk, Type: TypeAccess,
		IssuedAt: now.UnixMilli(), Expires: now.Add(i.accessTTL).UnixMilli(), JTI: accessJTI,
	})
	if err != nil {
		return nil, err
	}
	refresh, err := i.sign(Claims{
		UID: uid, Platform: platform, SDK: sdk, Type: TypeRefresh,
		IssuedAt: now.UnixMilli(), Expires: now.Add(i.refreshTTL).UnixMilli(), JTI: refreshJTI,
	})
	if err != nil {
		return nil, err
	}

	return &Pair{
		AccessToken:      access,
		RefreshToken:     refresh,
		ExpiresIn:        int64(i.accessTTL / time.Second),
		RefreshExpiresIn: int64(i.refreshTTL / time.Second),
	}, nil
}

// Verify parses and validates an access token for platform.
func (i *Issuer) Verify(accessToken string, platform int32) (*Claims, error) {
	claims, err := i.parse(accessToken)
	if err != nil {
		return nil, err
	}
	if claims.Type != TypeAccess {
		return nil, ErrWrongTokenType
	}
	if platform != 0 && claims.Platform != platform {
		return nil, ErrPlatformMismatch
	}
	if claims.Expires <= i.now().UnixMilli() {
		return nil, ErrExpiredToken
	}
	return claims, nil
}

// Refresh rotates a refresh token and returns a fresh pair.
func (i *Issuer) Refresh(refreshToken string) (*Pair, error) {
	claims, err := i.parse(refreshToken)
	if err != nil {
		return nil, err
	}
	if claims.Type != TypeRefresh {
		return nil, ErrWrongTokenType
	}
	if claims.Expires <= i.now().UnixMilli() {
		return nil, ErrExpiredToken
	}
	return i.Issue(claims.UID, claims.Platform, claims.SDK)
}

// parse verifies the signature and decodes the claims.
func (i *Issuer) parse(raw string) (*Claims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrInvalidToken
	}
	var h header
	if err = json.Unmarshal(headerBytes, &h); err != nil || h.Alg != "HS256" || h.Typ != "JWT" {
		return nil, ErrInvalidToken
	}

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidToken
	}
	mac := hmac.New(sha256.New, i.secret)
	mac.Write([]byte(parts[0]))
	mac.Write([]byte("."))
	mac.Write([]byte(parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, ErrInvalidToken
	}

	claimBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}
	claims := &Claims{}
	if err = json.Unmarshal(claimBytes, claims); err != nil {
		return nil, ErrInvalidToken
	}
	if claims.UID <= 0 || claims.Type == "" || claims.Expires <= 0 {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// sign encodes a JWT with HS256.
func (i *Issuer) sign(claims Claims) (string, error) {
	headerBytes, err := json.Marshal(header{Alg: "HS256", Typ: "JWT"})
	if err != nil {
		return "", ErrInvalidToken
	}
	claimBytes, err := json.Marshal(claims)
	if err != nil {
		return "", ErrInvalidToken
	}
	headerPart := base64.RawURLEncoding.EncodeToString(headerBytes)
	claimPart := base64.RawURLEncoding.EncodeToString(claimBytes)
	signingInput := headerPart + "." + claimPart

	mac := hmac.New(sha256.New, i.secret)
	mac.Write([]byte(signingInput))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + signature, nil
}

// jti returns a random token identifier.
func (i *Issuer) jti() (string, error) {
	var raw [16]byte
	if err := i.random(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func fillRandom(dst []byte) error {
	_, err := rand.Read(dst)
	return err
}

// FromEtc builds an issuer from the due bootstrap configuration. The secret is
// required; there is no production default.
func FromEtc() (*Issuer, error) {
	secret := etc.Get("etc.zim.auth.secret").String()
	if secret == "" {
		return nil, errors.New("auth secret is not configured")
	}
	return New(
		secret,
		etc.Get("etc.zim.auth.accessTTL", 7*24*time.Hour).Duration(),
		etc.Get("etc.zim.auth.refreshTTL", 90*24*time.Hour).Duration(),
	)
}

// MustFromEtc is the process startup variant of FromEtc. It terminates the
// process when the configuration is missing or invalid.
func MustFromEtc() *Issuer {
	issuer, err := FromEtc()
	if err != nil {
		log.Fatalf("create token issuer failed: %v", err)
	}
	return issuer
}
