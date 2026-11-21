// Package server implements the auth mesh service.
package server

import (
	"context"

	"github.com/dobyte/due/v2/cluster/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/auth/pb"
)

var _ pb.AuthServer = (*Server)(nil)

// NewServer returns the auth service.
func NewServer(proxy *mesh.Proxy) *Server {
	return NewServerWithIssuer(proxy, token.MustFromEtc())
}

// NewServerWithIssuer returns the auth service with an explicit issuer. It is
// the test seam; production code uses NewServer.
func NewServerWithIssuer(proxy *mesh.Proxy, issuer *token.Issuer) *Server {
	return &Server{
		proxy:  proxy,
		issuer: issuer,
	}
}

// Init registers the service provider under the discovery name auth.
func (s *Server) Init() {
	s.proxy.AddServiceProvider(define.ServiceNameAuth, &pb.Auth_ServiceDesc, s)
}

// VerifyToken validates an access token. Token failures never include the token
// in the gRPC error.
func (s *Server) VerifyToken(ctx context.Context, args *pb.VerifyTokenArgs) (*pb.VerifyTokenReply, error) {
	if args == nil {
		return nil, status.Error(codes.Unauthenticated, "token invalid")
	}
	rst, err := s.doVerifyToken(verifyTokenArgs{Token: args.GetToken(), Platform: args.GetPlatform()})
	if err != nil {
		return nil, err
	}
	return &pb.VerifyTokenReply{Uid: rst.UID, Platform: rst.Platform, ExpiresAt: rst.ExpiresAt}, nil
}

func (s *Server) doVerifyToken(args verifyTokenArgs) (verifyTokenRst, error) {
	if args.Token == "" {
		return verifyTokenRst{}, status.Error(codes.Unauthenticated, "token invalid")
	}
	claims, err := s.issuer.Verify(args.Token, args.Platform)
	if err != nil {
		return verifyTokenRst{}, status.Error(codes.Unauthenticated, "token invalid")
	}
	return verifyTokenRst{UID: claims.UID, Platform: claims.Platform, ExpiresAt: claims.Expires}, nil
}
