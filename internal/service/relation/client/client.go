// Package client is the typed mesh client for the relation service.
package client

import (
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/relation/pb"
)

// NewClient creates a relation mesh client.
func NewClient(fn transport.NewMeshClient) (pb.RelationClient, error) {
	client, err := fn(define.RelationTarget)
	if err != nil {
		return nil, err
	}
	return pb.NewRelationClient(client.Client().(grpc.ClientConnInterface)), nil
}
