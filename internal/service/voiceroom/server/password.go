package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// hashPassword returns "salt$sha256-hex" for PASSWORD rooms. The plaintext
// password is never stored or logged.
func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	sum := sha256.Sum256(append(salt, []byte(password)...))
	return hex.EncodeToString(salt) + "$" + hex.EncodeToString(sum[:]), nil
}

// verifyPassword reports whether password matches the stored hash.
func verifyPassword(stored, password string) bool {
	saltHex, hashHex, found := strings.Cut(stored, "$")
	if !found || saltHex == "" || hashHex == "" {
		return false
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(append(salt, []byte(password)...))
	return hmac.Equal([]byte(hex.EncodeToString(sum[:])), []byte(hashHex))
}
