-- 0024: USDT 复式账本（ADR-0043）。独立 wallet schema；金额一律 BIGINT 微 USDT。
-- 时间列统一 BIGINT 毫秒。负 uid 为系统过渡账户。

CREATE SCHEMA IF NOT EXISTS wallet;

-- 1. 钱包账户表
CREATE TABLE IF NOT EXISTS wallet.zim_wallet_account (
    uid                BIGINT PRIMARY KEY, -- 负数表示系统过渡账户
    account_type       SMALLINT NOT NULL DEFAULT 1, -- 1=USER 2=SYSTEM
    available_balance  BIGINT NOT NULL DEFAULT 0 CHECK (available_balance >= 0),
    frozen_balance     BIGINT NOT NULL DEFAULT 0 CHECK (frozen_balance >= 0),
    pay_password_hash  VARCHAR(255) NOT NULL DEFAULT '',
    totp_secret_enc    VARCHAR(255) NOT NULL DEFAULT '',
    withdraw_freeze_at BIGINT NOT NULL DEFAULT 0, -- 修改密码/2FA 后 24h 禁止提币截止（毫秒）
    status             SMALLINT NOT NULL DEFAULT 1, -- 1=NORMAL 2=FROZEN
    version            BIGINT NOT NULL DEFAULT 0,   -- 乐观锁版本号
    updated_at         BIGINT NOT NULL DEFAULT 0
);

-- 2. 不可篡改资金流水表（复式记账）
CREATE TABLE IF NOT EXISTS wallet.zim_wallet_ledger (
    id            BIGINT PRIMARY KEY,
    biz_order_id  VARCHAR(64) NOT NULL,
    uid           BIGINT NOT NULL,
    biz_type      SMALLINT NOT NULL, -- 1=DEPOSIT 2=WITHDRAW_FREEZE 3=WITHDRAW_SUCCESS 4=WITHDRAW_REJECT 5=RP_SEND 6=RP_CLAIM 7=RP_REFUND 8=TIP_OUT 9=TIP_IN 10=PLATFORM_FEE
    amount        BIGINT NOT NULL,   -- 正数=入账 负数=出账（微 USDT）
    balance_after BIGINT NOT NULL,
    frozen_after  BIGINT NOT NULL,
    remark        VARCHAR(255) NOT NULL DEFAULT '',
    created_at    BIGINT NOT NULL DEFAULT 0,
    UNIQUE (biz_order_id, uid, biz_type)
);
CREATE INDEX IF NOT EXISTS idx_ledger_uid_time ON wallet.zim_wallet_ledger (uid, created_at DESC);

-- 3. 用户充币地址映射表（HD 钱包派生，私钥由独立 KMS 托管）
CREATE TABLE IF NOT EXISTS wallet.zim_wallet_deposit_address (
    id             BIGINT PRIMARY KEY,
    uid            BIGINT NOT NULL,
    chain          VARCHAR(16) NOT NULL, -- TRC20, ERC20, BEP20
    address        VARCHAR(128) NOT NULL UNIQUE,
    derivation_idx INT NOT NULL,
    created_at     BIGINT NOT NULL DEFAULT 0,
    UNIQUE (uid, chain)
);

-- 4. 链上充值记录表
CREATE TABLE IF NOT EXISTS wallet.zim_chain_deposit (
    id            BIGINT PRIMARY KEY,
    uid           BIGINT NOT NULL,
    chain         VARCHAR(16) NOT NULL,
    tx_hash       VARCHAR(128) NOT NULL,
    log_index     INT NOT NULL DEFAULT 0,
    from_address  VARCHAR(128) NOT NULL,
    to_address    VARCHAR(128) NOT NULL,
    amount        BIGINT NOT NULL CHECK (amount > 0),
    confirmations INT NOT NULL DEFAULT 0,
    status        SMALLINT NOT NULL DEFAULT 0, -- 0=PENDING 1=CREDITED 2=DUST_IGNORED
    created_at    BIGINT NOT NULL DEFAULT 0,
    credited_at   BIGINT NOT NULL DEFAULT 0,
    UNIQUE (chain, tx_hash, log_index)
);

-- 5. 链上提现订单表
CREATE TABLE IF NOT EXISTS wallet.zim_chain_withdrawal (
    id              BIGINT PRIMARY KEY,
    biz_order_id    VARCHAR(64) NOT NULL UNIQUE,
    uid             BIGINT NOT NULL,
    chain           VARCHAR(16) NOT NULL,
    to_address      VARCHAR(128) NOT NULL,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    network_fee     BIGINT NOT NULL DEFAULT 0 CHECK (network_fee >= 0),
    actual_amount   BIGINT NOT NULL CHECK (actual_amount > 0),
    tx_hash         VARCHAR(128) NOT NULL DEFAULT '',
    status          SMALLINT NOT NULL DEFAULT 0, -- 0=PENDING_AUDIT 1=APPROVED_SENDING 2=BROADCASTED 3=SUCCESS 4=REJECTED 5=FAILED_REFUNDED
    audit_admin_id  BIGINT NOT NULL DEFAULT 0,
    audit_remark    VARCHAR(255) NOT NULL DEFAULT '',
    created_at      BIGINT NOT NULL DEFAULT 0,
    updated_at      BIGINT NOT NULL DEFAULT 0
);

-- 6. USDT 红包主表
CREATE TABLE IF NOT EXISTS wallet.zim_red_packet (
    rp_id          BIGINT PRIMARY KEY,
    biz_order_id   VARCHAR(64) NOT NULL UNIQUE,
    sender_uid     BIGINT NOT NULL,
    conv_id        VARCHAR(64) NOT NULL,
    conv_type      SMALLINT NOT NULL, -- 1=单聊 2=群聊
    rp_type        SMALLINT NOT NULL, -- 1=单聊专属 2=群普通 3=群拼手气 4=群专属
    target_uid     BIGINT NOT NULL DEFAULT 0,
    total_amount   BIGINT NOT NULL CHECK (total_amount > 0),
    total_count    INT NOT NULL CHECK (total_count > 0),
    claimed_amount BIGINT NOT NULL DEFAULT 0,
    claimed_count  INT NOT NULL DEFAULT 0,
    refund_amount  BIGINT NOT NULL DEFAULT 0,
    greeting       VARCHAR(128) NOT NULL DEFAULT '恭喜发财，大吉大利',
    status         SMALLINT NOT NULL DEFAULT 1, -- 1=ACTIVE 2=DEPLETED 3=EXPIRED_REFUNDED
    expire_at      BIGINT NOT NULL DEFAULT 0,
    created_at     BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rp_expire_active ON wallet.zim_red_packet (expire_at) WHERE status = 1;

-- 7. 红包领取明细表
CREATE TABLE IF NOT EXISTS wallet.zim_red_packet_claim (
    id           BIGINT PRIMARY KEY,
    rp_id        BIGINT NOT NULL,
    claimer_uid  BIGINT NOT NULL,
    amount       BIGINT NOT NULL CHECK (amount > 0),
    is_best_luck BOOLEAN NOT NULL DEFAULT FALSE,
    claimed_at   BIGINT NOT NULL DEFAULT 0,
    UNIQUE (rp_id, claimer_uid)
);

-- 8. 打赏/礼物订单表
CREATE TABLE IF NOT EXISTS wallet.zim_tip_order (
    tip_id         BIGINT PRIMARY KEY,
    biz_order_id   VARCHAR(64) NOT NULL UNIQUE,
    scene          SMALLINT NOT NULL, -- 1=单聊 2=群聊 3=语音房
    scene_id       VARCHAR(64) NOT NULL,
    sender_uid     BIGINT NOT NULL,
    receiver_uid   BIGINT NOT NULL,
    gift_id        INT NOT NULL DEFAULT 0, -- 0=纯 USDT 打赏
    gift_count     INT NOT NULL DEFAULT 1,
    combo_id       VARCHAR(64) NOT NULL DEFAULT '',
    gross_amount   BIGINT NOT NULL CHECK (gross_amount > 0),
    platform_fee   BIGINT NOT NULL DEFAULT 0,
    net_amount     BIGINT NOT NULL CHECK (net_amount > 0),
    message        VARCHAR(128) NOT NULL DEFAULT '',
    created_at     BIGINT NOT NULL DEFAULT 0
);
