package app

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/component"
	"github.com/dobyte/due/v2/lock"

	burnsvc "github.com/zciyuan/ZIM/internal/service/burn/server"
	callrecord "github.com/zciyuan/ZIM/internal/service/callrecord/server"
	gateclean "github.com/zciyuan/ZIM/internal/service/gateclean/server"
	maintenance "github.com/zciyuan/ZIM/internal/service/maintenance/server"
)

// App is the crontask component.
type App struct {
	component.Base
	service       *maintenance.Service
	locker        lock.Locker
	interval      time.Duration
	recorder      *callrecord.Recorder
	cleaner       *gateclean.Service
	cleanInterval time.Duration
	uploadDir     string
	uploadAge     time.Duration
	burnSweeper   *burnsvc.Sweeper
	burnInterval  time.Duration
	cancel        context.CancelFunc
	done          chan struct{}
}
