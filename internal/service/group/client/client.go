// Package client is the typed mesh client for the group service.
package client

import (
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/group/pb"
)

// NewClient creates a group mesh client.
func NewClient(fn transport.NewMeshClient) (pb.GroupClient, error) {
	client, err := fn(define.GroupTarget)
	if err != nil {
		return nil, err
	}
	return pb.NewGroupClient(client.Client().(grpc.ClientConnInterface)), nil
}
