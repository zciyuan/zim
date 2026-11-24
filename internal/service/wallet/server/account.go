package wallet

import (
	"context"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// doEnsureAccount 幂等建户：已存在则直接成功，供注册/登录链路兜底调用。
func (s *Service) doEnsureAccount(ctx context.Context, uid int64) error {
	if uid <= 0 {
		return errs.ErrInternal.WithMessage("invalid uid")
	}
	if err := s.repo.CreateAccount(ctx, uid, walletdao.AccountTypeUser, s.now().UnixMilli()); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// doGetAccount 返回账户余额与乐观锁版本。
func (s *Service) doGetAccount(ctx context.Context, uid int64) (*walletdao.Account, error) {
	acc, err := s.repo.GetAccount(ctx, uid)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return nil, errs.ErrWalletAccountNotExist
		}
		return nil, errs.ErrInternal.WithCause(err)
	}
	return acc, nil
}

// doFreezeAccount 管理员冻结账户（只拦出账，不挡入账）。
func (s *Service) doFreezeAccount(ctx context.Context, uid int64) error {
	if uid <= 0 {
		return nil // 系统过渡户永远不可冻结
	}
	if _, err := s.repo.GetAccount(ctx, uid); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return errs.ErrWalletAccountNotExist
		}
		return errs.ErrInternal.WithCause(err)
	}
	if err := s.repo.SetAccountStatus(ctx, uid, walletdao.AccountStatusFrozen, s.now().UnixMilli()); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// doUnfreezeAccount 管理员解冻账户。
func (s *Service) doUnfreezeAccount(ctx context.Context, uid int64) error {
	if uid <= 0 {
		return nil
	}
	if _, err := s.repo.GetAccount(ctx, uid); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return errs.ErrWalletAccountNotExist
		}
		return errs.ErrInternal.WithCause(err)
	}
	if err := s.repo.SetAccountStatus(ctx, uid, walletdao.AccountStatusNormal, s.now().UnixMilli()); err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// doSetPayPassword 设置支付密码（bcrypt 哈希，绝不打印原文/哈希），并触发
// withdraw_freeze_at 冻结窗口：改密后 24h 内禁止提币。
func (s *Service) doSetPayPassword(ctx context.Context, uid int64, password string) error {
	if len(password) < 6 || len(password) > 64 {
		return errs.ErrPayPasswordInvalid
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return errs.ErrInternal.WithCause(err)
	}
	now := s.now().UnixMilli()
	if err := s.repo.SetPayPasswordHash(ctx, uid, string(hash), now+s.cfg.FreezeWindowMs, now); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return errs.ErrWalletAccountNotExist
		}
		return errs.ErrInternal.WithCause(err)
	}
	return nil
}

// doVerifyPayPassword 校验支付密码，失败 200004。
func (s *Service) doVerifyPayPassword(ctx context.Context, uid int64, password string) error {
	acc, err := s.repo.GetAccount(ctx, uid)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return errs.ErrWalletAccountNotExist
		}
		return errs.ErrInternal.WithCause(err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(acc.PayPasswordHash), []byte(password)); err != nil {
		return errs.ErrPayPasswordInvalid
	}
	return nil
}

// doSetTOTP 生成新 TOTP 密钥（仅本次返回给用户绑定），并触发 24h 提币冻结。
// 密钥以 base32 存储；真实 KMS 加密由 M4 的 ChainSigner 对接时替换（TODO）。
func (s *Service) doSetTOTP(ctx context.Context, uid int64) (string, error) {
	secret, err := randomTOTPSecret()
	if err != nil {
		return "", errs.ErrInternal.WithCause(err)
	}
	now := s.now().UnixMilli()
	if err := s.repo.SetTOTPSecret(ctx, uid, secret, now+s.cfg.FreezeWindowMs, now); err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return "", errs.ErrWalletAccountNotExist
		}
		return "", errs.ErrInternal.WithCause(err)
	}
	return secret, nil
}

// doVerifyTOTP 校验 6 位 TOTP（±1 步容差），失败 200005。未开通 2FA 或账户
// 不存在一律视为失败（避免账户枚举）。
func (s *Service) doVerifyTOTP(ctx context.Context, uid int64, code string) error {
	acc, err := s.repo.GetAccount(ctx, uid)
	if err != nil {
		if dueerrors.Is(err, pgx.ErrNoRows) {
			return errs.ErrTOTPInvalid
		}
		return errs.ErrInternal.WithCause(err)
	}
	if acc.TOTPSecretEnc == "" || !verifyTOTP(acc.TOTPSecretEnc, s.now().Unix(), code) {
		return errs.ErrTOTPInvalid
	}
	return nil
}
