// Command zim-mesh-auth issues and verifies access tokens.
package main

import (
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-mesh-auth/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	postgres "github.com/zciyuan/ZIM/internal/component/postgres"
	"github.com/zciyuan/ZIM/internal/component/token"
	userdao "github.com/zciyuan/ZIM/internal/dao/user"
	"github.com/zciyuan/ZIM/internal/define"
	account "github.com/zciyuan/ZIM/internal/service/account/server"
	"github.com/zciyuan/ZIM/internal/version"
)

func main() {
	version.HandleFlag()
	container := due.NewContainer()

	registry := nacos.NewRegistry()
	transporter := grpc.NewTransporter()

	component := mesh.NewMesh(
		mesh.WithRegistry(registry),
		mesh.WithTransporter(transporter),
	)

	database := postgres.New()
	accountService, err := account.New(userdao.New(database.Pool()), token.MustFromEtc())
	if err != nil {
		log.Fatalf("create account service failed: %v", err)
	}

	app.Init(component.Proxy(), accountService)

	container.Add(component, database, health.New(define.ServiceMeshAuth))

	container.Serve()
}
