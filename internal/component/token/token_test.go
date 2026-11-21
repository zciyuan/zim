package token

import (
	"strings"
	"testing"
	"time"

	"github.com/dobyte/due/v2/etc"
)

const secret = "0123456789abcdef"

func newIssuer(t *testing.T) *Issuer {
	t.Helper()
	issuer, err := New(secret, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return issuer
}

func TestIssueVerify(t *testing.T) {
	issuer := newIssuer(t)
	pair, err := issuer.Issue(10001, 2, "1.0.0")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || pair.ExpiresIn != 3600 || pair.RefreshExpiresIn != 86400 {
		t.Fatalf("pair = %+v", pair)
	}
	if strings.Count(pair.AccessToken, ".") != 2 {
		t.Fatalf("access token is not a JWT: %q", pair.AccessToken)
	}
	claims, err := issuer.Verify(pair.AccessToken, 2)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.UID != 10001 || claims.Platform != 2 || claims.SDK != "1.0.0" {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestVerifyWithoutPlatform(t *testing.T) {
	issuer := newIssuer(t)
	pair, _ := issuer.Issue(1, 4, "")
	if _, err := issuer.Verify(pair.AccessToken, 0); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyErrors(t *testing.T) {
	issuer := newIssuer(t)
	pair, _ := issuer.Issue(1, 1, "")

	if _, err := issuer.Verify(pair.AccessToken, 2); err != ErrPlatformMismatch {
		t.Fatalf("platform err = %v", err)
	}
	if _, err := issuer.Verify(pair.RefreshToken, 1); err != ErrWrongTokenType {
		t.Fatalf("type err = %v", err)
	}
	if _, err := issuer.Verify("not-a-token", 1); err != ErrInvalidToken {
		t.Fatalf("malformed err = %v", err)
	}

	parts := strings.Split(pair.AccessToken, ".")
	parts[1] = "AAAA"
	if _, err := issuer.Verify(strings.Join(parts, "."), 1); err != ErrInvalidToken {
		t.Fatalf("tampered err = %v", err)
	}
}

func TestWrongSecret(t *testing.T) {
	issuer := newIssuer(t)
	other, _ := New("fedcba9876543210", time.Hour, time.Hour)
	pair, _ := issuer.Issue(1, 1, "")
	if _, err := other.Verify(pair.AccessToken, 1); err != ErrInvalidToken {
		t.Fatalf("err = %v", err)
	}
}

func TestExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	issuer, err := NewWithClock(secret, time.Second, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	pair, _ := issuer.Issue(1, 1, "")
	if _, err := issuer.Verify(pair.AccessToken, 1); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	now = now.Add(2 * time.Second)
	if _, err := issuer.Verify(pair.AccessToken, 1); err != ErrExpiredToken {
		t.Fatalf("expired err = %v", err)
	}
}

func TestRefresh(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	issuer, _ := NewWithClock(secret, time.Minute, time.Hour, func() time.Time { return now })
	pair, _ := issuer.Issue(10001, 3, "2.0.0")
	now = now.Add(time.Minute)
	next, err := issuer.Refresh(pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if next.AccessToken == pair.AccessToken || next.RefreshToken == pair.RefreshToken {
		t.Fatal("Refresh must rotate both tokens")
	}
	claims, err := issuer.Verify(next.AccessToken, 3)
	if err != nil || claims.UID != 10001 {
		t.Fatalf("claims = %+v, err = %v", claims, err)
	}
	if _, err := issuer.Refresh(pair.AccessToken); err != ErrWrongTokenType {
		t.Fatalf("access-as-refresh err = %v", err)
	}
}

func TestRefreshExpired(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	issuer, _ := NewWithClock(secret, time.Minute, time.Minute, func() time.Time { return now })
	pair, _ := issuer.Issue(1, 1, "")
	now = now.Add(2 * time.Minute)
	if _, err := issuer.Refresh(pair.RefreshToken); err != ErrExpiredToken {
		t.Fatalf("err = %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New("short", time.Hour, time.Hour); err != ErrWeakSecret {
		t.Fatalf("weak secret err = %v", err)
	}
	if _, err := New(secret, 0, time.Hour); err == nil {
		t.Fatal("zero ttl must fail")
	}
	pair, err := newIssuer(t).Issue(0, 1, "")
	if err != ErrInvalidToken || pair != nil {
		t.Fatalf("uid zero = %+v, %v", pair, err)
	}
}

func TestMalformedParts(t *testing.T) {
	issuer := newIssuer(t)
	cases := []string{
		"",
		"a.b",
		"a.b.c.d",
		"!!!.@@@.###",
	}
	for _, raw := range cases {
		if _, err := issuer.Verify(raw, 1); err != ErrInvalidToken {
			t.Fatalf("Verify(%q) err = %v", raw, err)
		}
	}
}

func TestFromEtcMissingSecret(t *testing.T) {
	if etc.Get("etc.zim.auth.secret").String() != "" {
		t.Skip("test environment has an auth secret configured")
	}
	if _, err := FromEtc(); err == nil {
		t.Fatal("expected a missing-secret error")
	}
}
