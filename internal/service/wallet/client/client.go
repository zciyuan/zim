// Package client is the typed mesh client for the wallet service.
package wallet

import (
	"github.com/dobyte/due/v2/transport"
	"google.golang.org/grpc"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/wallet/pb"
)

// NewClient creates a wallet mesh client for discovery target discovery://wallet.
func NewClient(fn transport.NewMeshClient) (pb.WalletClient, error) {
	client, err := fn(define.WalletTarget)
	if err != nil {
		return nil, err
	}
	return pb.NewWalletClient(client.Client().(grpc.ClientConnInterface)), nil
}
