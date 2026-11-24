package wallet

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
)

// 本文件实现 RFC 4226 (HOTP) 与 RFC 6238 (TOTP) 的最小合规实现，仅用标准库。
// 密钥 base32 编码（无填充）存储；验证窗口 ±1 步（每步 30 秒）以容忍时钟漂移。

const (
	totpPeriodSeconds int64 = 30
	totpDigits        int   = 6
	totpSkewSteps     int   = 1
)

// hotpCode 计算给定计数器下的 HOTP 码（RFC 4226 动态截断）。
func hotpCode(key []byte, counter uint64) string {
	var counterBytes [8]byte
	binary.BigEndian.PutUint64(counterBytes[:], counter)

	mac := hmac.New(sha1.New, key)
	_, _ = mac.Write(counterBytes[:])
	sum := mac.Sum(nil)

	offset := sum[len(sum)-1] & 0x0f
	code := (uint32(sum[offset])&0x7f)<<24 |
		uint32(sum[offset+1])<<16 |
		uint32(sum[offset+2])<<8 |
		uint32(sum[offset+3])
	code %= 1_000_000
	return fmt.Sprintf("%06d", code)
}

// totpCodeAt 计算 unix 秒对应的 TOTP 码。
func totpCodeAt(key []byte, unix int64) string {
	return hotpCode(key, uint64(unix/totpPeriodSeconds))
}

// decodeTOTPKey 解码 base32 密钥；非法输入返回错误。
func decodeTOTPKey(secret string) ([]byte, error) {
	return base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(strings.TrimSpace(secret)))
}

// verifyTOTP 校验 base32 密钥下的 6 位码，允许 ±1 步（30 秒）窗口。
// 比较使用 constant-time，防止时序侧信道。
func verifyTOTP(secret string, unix int64, code string) bool {
	if len(code) != totpDigits {
		return false
	}
	key, err := decodeTOTPKey(secret)
	if err != nil {
		return false
	}
	for step := -totpSkewSteps; step <= totpSkewSteps; step++ {
		expected := totpCodeAt(key, unix+int64(step)*totpPeriodSeconds)
		if subtle.ConstantTimeCompare([]byte(expected), []byte(code)) == 1 {
			return true
		}
	}
	return false
}

// randomTOTPSecret 生成 20 字节随机密钥并 base32 编码（RFC 4226 推荐长度）。
func randomTOTPSecret() (string, error) {
	key := make([]byte, 20)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("generate totp secret: %w", err)
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(key), nil
}
