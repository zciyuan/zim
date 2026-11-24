package wallet

import (
	"testing"
	"time"
)

// TestHOTPRFC4226Vectors 校验 RFC 4226 附录 D 的标准向量：
// 密钥 ASCII "12345678901234567890"，计数器 0..9。
func TestHOTPRFC4226Vectors(t *testing.T) {
	key := []byte("12345678901234567890")
	want := []string{
		"755224", "287082", "359152", "969429", "338314",
		"254676", "287922", "162583", "399871", "520489",
	}
	for counter, expected := range want {
		if got := hotpCode(key, uint64(counter)); got != expected {
			t.Fatalf("counter %d: got %s, want %s", counter, got, expected)
		}
	}
}

// TestTOTPVerifyWindow TOTP ±1 步容差：当前/前后一步的码都能通过，更远的失败。
func TestTOTPVerifyWindow(t *testing.T) {
	secret := "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ" // base32("12345678901234567890")
	unix := int64(59)
	code := totpCodeAt([]byte("12345678901234567890"), unix)

	if !verifyTOTP(secret, unix, code) {
		t.Fatalf("current step rejected")
	}
	if !verifyTOTP(secret, unix+totpPeriodSeconds, code) {
		t.Fatalf("next step rejected")
	}
	if !verifyTOTP(secret, unix-totpPeriodSeconds, code) {
		t.Fatalf("previous step rejected")
	}
	if verifyTOTP(secret, unix+2*totpPeriodSeconds, code) {
		t.Fatalf("two steps ahead accepted")
	}
	if verifyTOTP(secret, unix, "000000") {
		t.Fatalf("zero code accepted")
	}
	if verifyTOTP(secret, unix, "12345") {
		t.Fatalf("5-digit code accepted")
	}
	if verifyTOTP("!!!!not-base32!!!!", unix, code) {
		t.Fatalf("invalid secret accepted")
	}
}

// TestRandomTOTPSecret 生成的密钥可解码且可用于验证自身。
func TestRandomTOTPSecret(t *testing.T) {
	secret, err := randomTOTPSecret()
	if err != nil {
		t.Fatalf("random secret: err = %v", err)
	}
	key, err := decodeTOTPKey(secret)
	if err != nil {
		t.Fatalf("decode: err = %v", err)
	}
	if len(key) != 20 {
		t.Fatalf("key length: got %d, want 20", len(key))
	}
	code := totpCodeAt(key, time.Now().Unix())
	if !verifyTOTP(secret, time.Now().Unix(), code) {
		t.Fatalf("self-verification failed")
	}
}
