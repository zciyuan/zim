// Package client is the typed mesh client for the user service.
package client

import (
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/user/pb"
)

// NewClient creates a user mesh client.
func NewClient(fn transport.NewMeshClient) (pb.UserClient, error) {
	client, err := fn(define.UserTarget)
	if err != nil {
		return nil, err
	}
	return pb.NewUserClient(client.Client().(grpc.ClientConnInterface)), nil
}
