// Package wallet exposes the USDT wallet REST endpoints (ADR-0043,
// ZIM-02 §3.4bis). It is a thin forwarding layer onto the zim-wallet mesh.
package wallet

import (
	"strconv"

	"github.com/dobyte/due/component/http/v2"

	"github.com/zciyuan/ZIM/cmd/zim-api/app/api"
	"github.com/zciyuan/ZIM/cmd/zim-api/app/middleware"
	walletclient "github.com/zciyuan/ZIM/internal/service/wallet/client"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// NewAPI returns the wallet REST module.
func NewAPI(proxy *http.Proxy) *API {
	return &API{proxy: proxy}
}

// balanceRes is the GET /v1/wallet/balance response body.
type balanceRes struct {
	UID              int64 `json:"uid"`
	AvailableBalance int64 `json:"available_balance"`
	FrozenBalance    int64 `json:"frozen_balance"`
}

// Balance returns the caller's available and frozen balances (micro USDT).
func (a *API) Balance(ctx http.Context) error {
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.GetBalance(ctx.Context(), &pb.GetBalanceArgs{Uid: middleware.UID(ctx)})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, &balanceRes{
		UID: reply.GetUid(), AvailableBalance: reply.GetAvailableBalance(),
		FrozenBalance: reply.GetFrozenBalance(),
	})
}

// withdrawReq is the POST /v1/wallet/withdraw request body.
type withdrawReq struct {
	BizOrderID  string `json:"biz_order_id"`
	Chain       string `json:"chain"`
	ToAddress   string `json:"to_address"`
	Amount      int64  `json:"amount"`
	PayPassword string `json:"pay_password"`
	TOTPCode    string `json:"totp_code,omitempty"`
}

// Withdraw freezes funds and creates a chain withdrawal order.
func (a *API) Withdraw(ctx http.Context) error {
	req := &withdrawReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrWithdrawAddressInvalid)
	}
	if req.BizOrderID == "" {
		if id := ctx.Get("X-Request-Id"); id != "" {
			req.BizOrderID = id
		}
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.Withdraw(ctx.Context(), &pb.WithdrawArgs{
		BizOrderId: req.BizOrderID, Uid: middleware.UID(ctx), Chain: req.Chain,
		ToAddress: req.ToAddress, Amount: req.Amount,
		PayPassword: req.PayPassword, TotpCode: req.TOTPCode,
	})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{
		"biz_order_id": reply.GetBizOrderId(), "status": reply.GetStatus(),
		"network_fee": reply.GetNetworkFee(), "actual_amount": reply.GetActualAmount(),
		"tx_hash": reply.GetTxHash(),
	})
}

// DepositAddress returns the caller's deposit address for a chain.
func (a *API) DepositAddress(ctx http.Context) error {
	chain := ctx.Query("chain")
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.GetDepositAddress(ctx.Context(), &pb.GetDepositAddressArgs{
		Uid: middleware.UID(ctx), Chain: chain,
	})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{
		"uid": reply.GetUid(), "chain": reply.GetChain(), "address": reply.GetAddress(),
	})
}

// setPayPasswordReq is the POST /v1/wallet/pay-password request body.
type setPayPasswordReq struct {
	PayPassword string `json:"pay_password"`
}

// SetPayPassword sets or changes the payment password (24h withdraw freeze).
func (a *API) SetPayPassword(ctx http.Context) error {
	req := &setPayPasswordReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrPayPasswordInvalid)
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	if _, err = client.SetPayPassword(ctx.Context(), &pb.SetPayPasswordArgs{
		Uid: middleware.UID(ctx), PayPassword: req.PayPassword,
	}); err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{"ok": true})
}

// BindTOTP provisions a TOTP secret for the caller.
func (a *API) BindTOTP(ctx http.Context) error {
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.SetTOTP(ctx.Context(), &pb.SetTOTPArgs{Uid: middleware.UID(ctx)})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{"secret": reply.GetSecret()})
}

// sendRedPacketReq is the POST /v1/wallet/redpacket/send request body.
type sendRedPacketReq struct {
	BizOrderID  string `json:"biz_order_id"`
	ConvID      string `json:"conv_id"`
	ConvType    int32  `json:"conv_type"`
	RpType      int32  `json:"rp_type"`
	TargetUID   int64  `json:"target_uid,omitempty"`
	TotalAmount int64  `json:"total_amount"`
	TotalCount  int32  `json:"total_count"`
	Greeting    string `json:"greeting,omitempty"`
}

// SendRedPacket freezes the sender's funds and arms the claim queue.
func (a *API) SendRedPacket(ctx http.Context) error {
	req := &sendRedPacketReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrRedPacketAmountInvalid)
	}
	if req.BizOrderID == "" {
		if id := ctx.Get("X-Request-Id"); id != "" {
			req.BizOrderID = id
		}
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.SendRedPacket(ctx.Context(), &pb.SendRedPacketArgs{
		BizOrderId: req.BizOrderID, SenderUid: middleware.UID(ctx), ConvId: req.ConvID,
		ConvType: req.ConvType, RpType: req.RpType, TargetUid: req.TargetUID,
		TotalAmount: req.TotalAmount, TotalCount: req.TotalCount, Greeting: req.Greeting,
	})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, reply.GetPacket())
}

// ClaimRedPacket pops one share for the caller.
func (a *API) ClaimRedPacket(ctx http.Context) error {
	rpID, err := strconv.ParseInt(ctx.Params("rp_id"), 10, 64)
	if err != nil || rpID <= 0 {
		return api.Failure(ctx, errs.ErrRedPacketNotExist)
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.ClaimRedPacket(ctx.Context(), &pb.ClaimRedPacketArgs{
		RpId: rpID, ClaimerUid: middleware.UID(ctx),
	})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{
		"rp_id": reply.GetRpId(), "amount": reply.GetAmount(), "is_best_luck": reply.GetIsBestLuck(),
	})
}

// GetRedPacket returns packet details and claim records.
func (a *API) GetRedPacket(ctx http.Context) error {
	rpID, err := strconv.ParseInt(ctx.Params("rp_id"), 10, 64)
	if err != nil || rpID <= 0 {
		return api.Failure(ctx, errs.ErrRedPacketNotExist)
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.GetRedPacket(ctx.Context(), &pb.GetRedPacketArgs{RpId: rpID})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{
		"packet": reply.GetPacket(), "claims": reply.GetClaims(),
	})
}

// sendTipReq is the POST /v1/wallet/tip request body.
type sendTipReq struct {
	BizOrderID  string `json:"biz_order_id"`
	Scene       int32  `json:"scene"`
	SceneID     string `json:"scene_id"`
	ReceiverUID int64  `json:"receiver_uid"`
	GiftID      int32  `json:"gift_id"`
	GiftCount   int32  `json:"gift_count,omitempty"`
	ComboID     string `json:"combo_id,omitempty"`
	GrossAmount int64  `json:"gross_amount"`
	Message     string `json:"message,omitempty"`
}

// SendTip settles a three-leg tip order.
func (a *API) SendTip(ctx http.Context) error {
	req := &sendTipReq{}
	if err := ctx.Bind().JSON(req); err != nil {
		return api.Failure(ctx, errs.ErrInternal.WithMessage("invalid tip request"))
	}
	if req.BizOrderID == "" {
		if id := ctx.Get("X-Request-Id"); id != "" {
			req.BizOrderID = id
		}
	}
	client, err := walletclient.NewClient(a.proxy.NewMeshClient)
	if err != nil {
		return api.Failure(ctx, errs.ErrServerBusy)
	}
	reply, err := client.SendTip(ctx.Context(), &pb.SendTipArgs{
		BizOrderId: req.BizOrderID, Scene: req.Scene, SceneId: req.SceneID,
		SenderUid: middleware.UID(ctx), ReceiverUid: req.ReceiverUID,
		GiftId: req.GiftID, GiftCount: req.GiftCount, ComboId: req.ComboID,
		GrossAmount: req.GrossAmount, Message: req.Message,
	})
	if err != nil {
		return api.Failure(ctx, failure(err))
	}
	return api.Success(ctx, map[string]any{
		"biz_order_id": reply.GetBizOrderId(), "gross_amount": reply.GetGrossAmount(),
		"platform_fee": reply.GetPlatformFee(), "net_amount": reply.GetNetAmount(),
	})
}
