package wallet

import (
	"context"
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	walletdao "github.com/zciyuan/ZIM/internal/dao/wallet"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// 本文件的 RPC 方法只做「解析 → doXxx → 组装响应」三段式薄封装，业务全部在
// doXxx 中。错误统一经 toRPC 转换：gRPC 消息内嵌六位 ZIM 码（"code|message"），
// 客户端用 client.FromRPC 还原 errs 错误。

// GetBalance handles the RPC.
func (s *Service) GetBalance(ctx context.Context, args *pb.GetBalanceArgs) (*pb.GetBalanceReply, error) {
	acc, err := s.doGetAccount(ctx, args.GetUid())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.GetBalanceReply{
		Uid:              acc.UID,
		AvailableBalance: acc.AvailableBalance,
		FrozenBalance:    acc.FrozenBalance,
		Version:          acc.Version,
	}, nil
}

// EnsureAccount handles the RPC.
func (s *Service) EnsureAccount(ctx context.Context, args *pb.EnsureAccountArgs) (*pb.EnsureAccountReply, error) {
	err := s.doEnsureAccount(ctx, args.GetUid())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.EnsureAccountReply{Uid: args.GetUid(), Created: true}, nil
}

// SetPayPassword handles the RPC.
func (s *Service) SetPayPassword(ctx context.Context, args *pb.SetPayPasswordArgs) (*pb.SetPayPasswordReply, error) {
	err := s.doSetPayPassword(ctx, args.GetUid(), args.GetPayPassword())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.SetPayPasswordReply{Ok: true}, nil
}

// VerifyPayPassword handles the RPC.
func (s *Service) VerifyPayPassword(ctx context.Context, args *pb.VerifyPayPasswordArgs) (*pb.VerifyPayPasswordReply, error) {
	err := s.doVerifyPayPassword(ctx, args.GetUid(), args.GetPayPassword())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.VerifyPayPasswordReply{Ok: true}, nil
}

// SetTOTP handles the RPC.
func (s *Service) SetTOTP(ctx context.Context, args *pb.SetTOTPArgs) (*pb.SetTOTPReply, error) {
	secret, err := s.doSetTOTP(ctx, args.GetUid())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.SetTOTPReply{Secret: secret}, nil
}

// VerifyTOTP handles the RPC.
func (s *Service) VerifyTOTP(ctx context.Context, args *pb.VerifyTOTPArgs) (*pb.VerifyTOTPReply, error) {
	err := s.doVerifyTOTP(ctx, args.GetUid(), args.GetCode())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.VerifyTOTPReply{Ok: true}, nil
}

// Withdraw handles the RPC.
func (s *Service) Withdraw(ctx context.Context, args *pb.WithdrawArgs) (*pb.WithdrawReply, error) {
	result, err := s.doWithdraw(ctx, withdrawArgs{
		BizOrderID:  args.GetBizOrderId(),
		UID:         args.GetUid(),
		Chain:       args.GetChain(),
		ToAddress:   args.GetToAddress(),
		Amount:      args.GetAmount(),
		PayPassword: args.GetPayPassword(),
		TOTPCode:    args.GetTotpCode(),
	})
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.WithdrawReply{
		BizOrderId:   result.Order.BizOrderID,
		Status:       int32(result.Order.Status),
		NetworkFee:   result.Order.NetworkFee,
		ActualAmount: result.Order.ActualAmount,
		TxHash:       result.Order.TxHash,
	}, nil
}

// AuditWithdraw handles the RPC.
func (s *Service) AuditWithdraw(ctx context.Context, args *pb.AuditWithdrawArgs) (*pb.AuditWithdrawReply, error) {
	result, err := s.doAuditWithdraw(ctx, args.GetBizOrderId(), args.GetApprove(), args.GetAdminId(), args.GetRemark())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.AuditWithdrawReply{
		BizOrderId: result.Order.BizOrderID,
		Status:     int32(result.Order.Status),
		TxHash:     result.Order.TxHash,
	}, nil
}

// DepositCallback handles the RPC.
func (s *Service) DepositCallback(ctx context.Context, args *pb.DepositCallbackArgs) (*pb.DepositCallbackReply, error) {
	result, err := s.doDeposit(ctx, depositArgs{
		Chain:         args.GetChain(),
		TxHash:        args.GetTxHash(),
		LogIndex:      args.GetLogIndex(),
		FromAddress:   args.GetFromAddress(),
		ToAddress:     args.GetToAddress(),
		Amount:        args.GetAmount(),
		Confirmations: args.GetConfirmations(),
	})
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.DepositCallbackReply{
		Status:      int32(result.Status),
		CreditedUid: result.CreditedUID,
	}, nil
}

// GetDepositAddress handles the RPC.
func (s *Service) GetDepositAddress(ctx context.Context, args *pb.GetDepositAddressArgs) (*pb.GetDepositAddressReply, error) {
	addr, err := s.doGetDepositAddress(ctx, args.GetUid(), args.GetChain())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.GetDepositAddressReply{Uid: addr.UID, Chain: addr.Chain, Address: addr.Address}, nil
}

// FreezeAccount handles the RPC.
func (s *Service) FreezeAccount(ctx context.Context, args *pb.FreezeAccountArgs) (*pb.FreezeAccountReply, error) {
	err := s.doFreezeAccount(ctx, args.GetUid())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.FreezeAccountReply{Ok: true}, nil
}

// UnfreezeAccount handles the RPC.
func (s *Service) UnfreezeAccount(ctx context.Context, args *pb.UnfreezeAccountArgs) (*pb.UnfreezeAccountReply, error) {
	err := s.doUnfreezeAccount(ctx, args.GetUid())
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.UnfreezeAccountReply{Ok: true}, nil
}

// DailyReconcile handles the RPC.
func (s *Service) DailyReconcile(ctx context.Context, args *pb.DailyReconcileArgs) (*pb.DailyReconcileReply, error) {
	result, err := s.doDailyReconcile(ctx)
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.DailyReconcileReply{
		Ok:          true,
		UserTotal:   result.UserTotal,
		SystemTotal: result.SystemTotal,
		Deposits:    result.Deposits,
		Withdrawals: result.Withdrawals,
		Difference:  result.Difference,
	}, nil
}

// SendRedPacket handles the RPC (M4, ADR-0043 §4.4).
func (s *Service) SendRedPacket(ctx context.Context, args *pb.SendRedPacketArgs) (*pb.SendRedPacketReply, error) {
	packet, err := s.doSendRedPacket(ctx, redPacketArgs{
		BizOrderID: args.GetBizOrderId(), SenderUID: args.GetSenderUid(),
		ConvID: args.GetConvId(), ConvType: int16(args.GetConvType()),
		RpType: int16(args.GetRpType()), TargetUID: args.GetTargetUid(),
		TotalAmount: args.GetTotalAmount(), TotalCount: int(args.GetTotalCount()),
		Greeting: args.GetGreeting(),
	})
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.SendRedPacketReply{Packet: toRedPacketInfo(packet)}, nil
}

// ClaimRedPacket handles the RPC (M4, ADR-0043 §4.4.2).
func (s *Service) ClaimRedPacket(ctx context.Context, args *pb.ClaimRedPacketArgs) (*pb.ClaimRedPacketReply, error) {
	claim, err := s.doClaimRedPacket(ctx, redPacketClaimArgs{RpID: args.GetRpId(), ClaimerUID: args.GetClaimerUid()})
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.ClaimRedPacketReply{RpId: claim.RPID, Amount: claim.Amount, IsBestLuck: claim.IsBestLuck}, nil
}

// GetRedPacket handles the RPC (M4): packet detail plus claim rows.
func (s *Service) GetRedPacket(ctx context.Context, args *pb.GetRedPacketArgs) (*pb.GetRedPacketReply, error) {
	packet, err := s.repo.GetRedPacket(ctx, args.GetRpId())
	if err != nil {
		s.count(err)
		return nil, toRPC(errs.ErrRedPacketNotExist.WithCause(err))
	}
	claims, err := s.repo.ListRedPacketClaims(ctx, args.GetRpId(), 200)
	if err != nil {
		s.count(err)
		return nil, toRPC(errs.ErrInternal.WithCause(err))
	}
	items := make([]*pb.RedPacketClaimInfo, 0, len(claims))
	for _, claim := range claims {
		items = append(items, &pb.RedPacketClaimInfo{
			ClaimerUid: claim.ClaimerUID, Amount: claim.Amount,
			IsBestLuck: claim.IsBestLuck, ClaimedAt: claim.ClaimedAt,
		})
	}
	s.count(nil)
	return &pb.GetRedPacketReply{Packet: toRedPacketInfo(packet), Claims: items}, nil
}

// SendTip handles the RPC (M4, ADR-0043 §4.5).
func (s *Service) SendTip(ctx context.Context, args *pb.SendTipArgs) (*pb.SendTipReply, error) {
	result, err := s.doSendTip(ctx, tipArgs{
		BizOrderID: args.GetBizOrderId(), Scene: int16(args.GetScene()),
		SceneID: args.GetSceneId(), SenderUID: args.GetSenderUid(),
		ReceiverUID: args.GetReceiverUid(), GiftID: args.GetGiftId(),
		GiftCount: args.GetGiftCount(), ComboID: args.GetComboId(),
		GrossAmount: args.GetGrossAmount(), Message: args.GetMessage(),
	})
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.SendTipReply{
		BizOrderId: result.Order.BizOrderID, GrossAmount: result.Order.GrossAmount,
		PlatformFee: result.PlatformFee, NetAmount: result.NetAmount,
	}, nil
}

// RefundExpiredRedPackets handles the RPC (M4, ADR-0043 §4.4.4).
func (s *Service) RefundExpiredRedPackets(ctx context.Context, args *pb.RefundExpiredRedPacketsArgs) (*pb.RefundExpiredRedPacketsReply, error) {
	refunded, err := s.doRefundExpiredRedPackets(ctx, int(args.GetLimit()))
	s.count(err)
	if err != nil {
		return nil, toRPC(err)
	}
	return &pb.RefundExpiredRedPacketsReply{Refunded: int32(refunded)}, nil
}

func toRedPacketInfo(packet *walletdao.RedPacket) *pb.RedPacketInfo {
	return &pb.RedPacketInfo{
		RpId: packet.RPID, BizOrderId: packet.BizOrderID, SenderUid: packet.SenderUID,
		ConvId: packet.ConvID, ConvType: int32(packet.ConvType), RpType: int32(packet.RpType),
		TargetUid: packet.TargetUID, TotalAmount: packet.TotalAmount, TotalCount: packet.TotalCount,
		ClaimedAmount: packet.ClaimedAmount, ClaimedCount: packet.ClaimedCount,
		RefundAmount: packet.RefundAmount, Greeting: packet.Greeting, Status: int32(packet.Status),
		ExpireAt: packet.ExpireAt, CreatedAt: packet.CreatedAt,
	}
}

// toRPC converts a business error into a gRPC status whose message embeds the
// six-digit ZIM code as "code|message" so client.FromRPC can restore it.
func toRPC(err error) error {
	business := errs.From(err)
	return status.Error(rpcCodeOf(business), fmt.Sprintf("%d|%s", business.Code().Code(), business.Message()))
}

// rpcCodeOf maps the business error to a coarse gRPC code; the six-digit ZIM
// code is authoritative and travels in the message.
func rpcCodeOf(business *errs.Error) codes.Code {
	switch business.HTTPStatus() {
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound, http.StatusGone:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusBadRequest, http.StatusPaymentRequired:
		return codes.InvalidArgument
	case http.StatusInternalServerError, http.StatusServiceUnavailable:
		return codes.Internal
	default:
		return codes.Unknown
	}
}
