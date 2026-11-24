package wallet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
)

// 本文件提供本地运行的 Dummy ChainSigner：地址派生/广播/确认数全部 mock，
// 不触碰任何真实链。生产环境用 KMS 实现替换（接口不变）。

// DummySigner 是 ChainSigner 的本地 mock 实现。
type DummySigner struct{}

var _ ChainSigner = DummySigner{}

// DeriveAddress 按链生成格式合法的 mock 地址（可通过 isValidWithdrawAddress）。
// TRC20 用 33 位 '1'-'9' 数字（base58 排除 0/O/I/l），确定性可测。
func (DummySigner) DeriveAddress(_ context.Context, chain string, derivationIdx int32) (string, error) {
	switch chain {
	case walletdao.ChainTRC20:
		// T + 33 位十进制（用 '1' 左填充，避免 '0'），总长 34。
		return "T" + strings.Repeat("1", 33-len(fmt.Sprintf("%d", derivationIdx+1))) +
			fmt.Sprintf("%d", derivationIdx+1), nil
	case walletdao.ChainERC20, walletdao.ChainBEP20:
		return fmt.Sprintf("0x%040x", int64(derivationIdx)+1), nil
	default:
		return "", fmt.Errorf("dummy signer: unsupported chain %s", chain)
	}
}

// Broadcast 返回 mock 交易哈希，永远成功。
func (DummySigner) Broadcast(_ context.Context, _, _ string, _ int64) (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("dummy signer: random: %w", err)
	}
	return "mocktx-" + hex.EncodeToString(random) + "-" + fmt.Sprintf("%d", time.Now().UnixMilli()), nil
}

// GetConfirmations 返回远超所有阈值（20/12/15）的确认数，本地充值即时入账。
func (DummySigner) GetConfirmations(_ context.Context, _, _ string) (int32, error) {
	return 999, nil
}
