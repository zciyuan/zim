// Command zim-wallet serves the USDT wallet mesh (ADR-0043, M3): double-entry
// ledger, withdrawals, deposit callbacks and the daily reconcile over the
// independent wallet schema.
package main

import (
	"context"
	"time"

	lockredis "github.com/dobyte/due/lock/redis/v2"
	"github.com/dobyte/due/registry/nacos/v2"
	"github.com/dobyte/due/transport/grpc/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/cmd/zim-wallet/app"
	"github.com/zciyuan/ZIM/internal/component/health"
	"github.com/zciyuan/ZIM/internal/component/postgres"
	rediscomp "github.com/zciyuan/ZIM/internal/component/redis"
	"github.com/zciyuan/ZIM/internal/define"
	walletsvc "github.com/zciyuan/ZIM/internal/service/wallet/server"
	"github.com/zciyuan/ZIM/internal/version"
	"github.com/zciyuan/ZIM/zimfw/pipe"
)

func main() {
	version.HandleFlag()

	database := postgres.NewWallet()
	if database.Pool() == nil {
		log.Fatal("zim-wallet requires etc.zim.wallet.dsn")
	}
	cache := rediscomp.New()

	component := mesh.NewMesh(
		mesh.WithRegistry(nacos.NewRegistry()),
		mesh.WithTransporter(grpc.NewTransporter()),
	)

	service := walletsvc.NewServer(component.Proxy(), database.Pool(), walletsvc.ConfigFromEtc(), walletsvc.DummySigner{})
	// M4: red packet claim Lua lives in Redis; without it claims fail closed.
	service.SetRedis(cache.Client())
	// ADR-0043 §1.2.C: rpClaim/rpRefund/tipSettled side-effect events.
	if brokers := etc.Get("etc.zim.pipe.brokers", []string{}).Strings(); len(brokers) > 0 {
		service.SetEvents(walletsvc.NewKafkaEvents(pipe.NewKafkaProducer(brokers)))
	}
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := service.EnsureSystemAccounts(initCtx); err != nil {
		initCancel()
		log.Fatalf("ensure wallet system accounts failed: %v", err)
	}
	initCancel()

	app.Init(component.Proxy(), service)

	// M4 maintenance: expired refund + claim compensation + daily reconcile,
	// leader-locked so multiple replicas never double-run (ADR-0043 G-06/G-07).
	sweeper := walletsvc.NewSweeper(service)
	go runMaintenance(sweeper, service)

	container := due.NewContainer()
	container.Add(component, database, cache, health.New(define.ServiceWallet))
	container.Serve()
}

// runMaintenance runs the wallet sweeps under the Redis leader lock. The
// claim-compensation and refund cadence is 1 minute; reconcile is daily.
func runMaintenance(sweeper *walletsvc.Sweeper, service *walletsvc.Service) {
	locker := lockredis.NewMaker().Make("zim:wallet:maintenance")
	ticker := time.NewTicker(etc.Get("etc.zim.wallet.maintenanceInterval", "1m").Duration())
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := locker.TryAcquire(ctx, time.Minute); err != nil {
			cancel()
			continue
		}
		if refunded, err := sweeper.RefundExpired(ctx, 20); err != nil {
			log.Warnf("wallet refund sweep failed: %v", err)
		} else if refunded > 0 {
			log.Infof("wallet refund sweep: refunded = %d", refunded)
		}
		if compensated, err := sweeper.CompensateClaims(ctx, 50); err != nil {
			log.Warnf("wallet claim compensation failed: %v", err)
		} else if compensated > 0 {
			log.Infof("wallet claim compensation: compensated = %d", compensated)
		}
		_ = locker.Release(ctx)
		cancel()
	}
}
