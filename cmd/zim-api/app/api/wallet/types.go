package wallet

import (
	"github.com/dobyte/due/component/http/v2"

	walletclient "github.com/zciyuan/ZIM/internal/service/wallet/client"
)

// API forwards REST calls to the wallet mesh service.
type API struct {
	proxy *http.Proxy
}

// failure wraps the mesh RPC error back into a canonical business error so
// api.Failure reports the six-digit ZIM code instead of a generic 900003.
func failure(err error) error {
	return walletclient.FromRPC(err)
}
