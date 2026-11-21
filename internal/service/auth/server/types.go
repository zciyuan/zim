package server

import (
	"github.com/dobyte/due/v2/cluster/mesh"

	"github.com/zciyuan/ZIM/internal/component/token"
	"github.com/zciyuan/ZIM/internal/service/account/server"
	pb "github.com/zciyuan/ZIM/internal/service/auth/pb"
)

// Server is the auth mesh service provider.
type Server struct {
	pb.UnimplementedAuthServer
	proxy   *mesh.Proxy
	issuer  *token.Issuer
	account *account.Service
}

type verifyTokenArgs struct {
	Token    string
	Platform int32
}

type verifyTokenRst struct {
	UID       int64
	Platform  int32
	ExpiresAt int64
}
