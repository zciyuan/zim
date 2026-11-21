package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/service/account/server"
	pb "github.com/zciyuan/ZIM/internal/service/auth/pb"
	"github.com/zciyuan/ZIM/zimfw/contract/errs"
)

// SetAccount enables username/password login on the auth mesh.
func (s *Server) SetAccount(service *account.Service) { s.account = service }

// Login validates credentials and returns an access/refresh pair.
func (s *Server) Login(ctx context.Context, args *pb.LoginArgs) (*pb.TokenPair, error) {
	if s.account == nil {
		return nil, status.Error(codes.Unavailable, "account service unavailable")
	}
	result, err := s.account.Login(ctx, args.GetUsername(), args.GetPassword(), args.GetPlatform())
	if err != nil {
		return nil, authError(err)
	}
	return tokenPair(result), nil
}

// Register creates a user and returns tokens.
func (s *Server) Register(ctx context.Context, args *pb.RegisterArgs) (*pb.TokenPair, error) {
	if s.account == nil {
		return nil, status.Error(codes.Unavailable, "account service unavailable")
	}
	result, err := s.account.Register(ctx, account.RegisterRequest{
		Username: args.GetUsername(), Password: args.GetPassword(),
		Nickname: args.GetNickname(), Platform: args.GetPlatform(),
	})
	if err != nil {
		return nil, authError(err)
	}
	return tokenPair(result), nil
}

// Refresh rotates a refresh token.
func (s *Server) Refresh(_ context.Context, args *pb.RefreshArgs) (*pb.TokenPair, error) {
	if s.issuer == nil {
		return nil, status.Error(codes.Unavailable, "token issuer unavailable")
	}
	pair, err := s.issuer.Refresh(args.GetRefreshToken())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "refresh invalid")
	}
	claims, err := s.issuer.Verify(pair.AccessToken, 0)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "refresh invalid")
	}
	return &pb.TokenPair{
		Uid: claims.UID, AccessToken: pair.AccessToken, RefreshToken: pair.RefreshToken,
		ExpiresIn: pair.ExpiresIn, RefreshExpiresIn: pair.RefreshExpiresIn,
	}, nil
}

func tokenPair(result *account.Result) *pb.TokenPair {
	return &pb.TokenPair{
		Uid: result.UID, AccessToken: result.AccessToken, RefreshToken: result.RefreshToken,
		ExpiresIn: result.ExpiresIn, RefreshExpiresIn: result.RefreshExpiresIn,
	}
}

func authError(err error) error {
	switch {
	case isCode(err, 100001):
		return status.Error(codes.Unauthenticated, "password incorrect")
	case isCode(err, 110005):
		return status.Error(codes.AlreadyExists, "username taken")
	case isCode(err, 100004):
		return status.Error(codes.PermissionDenied, "account disabled")
	default:
		return status.Error(codes.Internal, "account operation failed")
	}
}

func isCode(err error, code int) bool {
	return errs.From(err).Code().Code() == code
}
