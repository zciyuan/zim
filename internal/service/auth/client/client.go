// Package client is the typed mesh client for the auth service.
package client

import (
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/auth/pb"
)

const target = define.AuthTarget

// NewClient creates an auth mesh client.
func NewClient(fn transport.NewMeshClient) (pb.AuthClient, error) {
	client, err := fn(target)
	if err != nil {
		return nil, err
	}
	return pb.NewAuthClient(client.Client().(grpc.ClientConnInterface)), nil
}
