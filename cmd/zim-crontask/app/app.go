// Package app runs the periodic zim-crontask maintenance loop.
package app

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/lock"
	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
	callrecord "github.com/zciyuan/ZIM/internal/service/callrecord/server"
	gateclean "github.com/zciyuan/ZIM/internal/service/gateclean/server"
	maintenance "github.com/zciyuan/ZIM/internal/service/maintenance/server"
)

// New returns the crontask application.
func New(service *maintenance.Service, locker lock.Locker, interval time.Duration,
	recorder *callrecord.Recorder, cleaner *gateclean.Service, cleanInterval time.Duration,
	uploadDir string, uploadAge time.Duration) *App {
	if interval <= 0 {
		interval = time.Hour
	}
	if cleanInterval <= 0 {
		cleanInterval = 30 * time.Second
	}
	return &App{service: service, locker: locker, interval: interval, recorder: recorder,
		cleaner: cleaner, cleanInterval: cleanInterval, uploadDir: uploadDir,
		uploadAge: uploadAge, done: make(chan struct{})}
}

// Name returns the component name.
func (a *App) Name() string {
	return "crontask"
}

// Start runs the maintenance loop.
func (a *App) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	go func() {
		defer close(a.done)
		a.runOnce(ctx)
		go a.runRTCTimeouts(ctx)
		go a.runUploadCleanup(ctx)
		maintenanceTicker := time.NewTicker(a.interval)
		defer maintenanceTicker.Stop()
		cleanTicker := time.NewTicker(a.cleanInterval)
		defer cleanTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-maintenanceTicker.C:
				a.runOnce(ctx)
			case <-cleanTicker.C:
				a.runGateClean(ctx)
			}
		}
	}()
}

// Close stops the loop.
func (a *App) Close() {
	if a.cancel != nil {
		a.cancel()
		<-a.done
	}
}

func (a *App) runOnce(ctx context.Context) {
	if a.service == nil {
		return
	}
	if a.locker != nil {
		if err := a.locker.TryAcquire(ctx, 2*time.Minute); err != nil {
			log.Debugf("crontask skipped: lock not acquired: %v", err)
			return
		}
		defer func() {
			if err := a.locker.Release(ctx); err != nil {
				log.Warnf("crontask release lock failed: %v", err)
			}
		}()
	}
	stats, err := a.service.Run(ctx)
	if err != nil {
		log.Errorf("crontask run failed: %v", err)
		return
	}
	log.Infof("crontask done: friend_requests = %d, message_blocks = %d",
		stats.FriendRequests, stats.MessageBlocks)
}

// runGateClean reclaims locations of gateways that are no longer registered.
//
// zim-crontask runs as a single replica (ZIM-01 §4), so the sweep does not take
// an extra leader lock; the reclaim script itself is idempotent.
func (a *App) runGateClean(ctx context.Context) {
	if a.cleaner == nil {
		return
	}
	stats, err := a.cleaner.Run(ctx)
	if err != nil {
		metrics.Default.Inc("zim_gate_reclaim_total", map[string]string{"result": "error"})
		log.Errorf("gate clean failed: %v", err)
		return
	}
	if stats.DeadGates == 0 {
		return
	}
	metrics.Default.Add("zim_gate_reclaim_total", map[string]string{"result": "ok"}, int64(stats.DeadGates))
	metrics.Default.Add("zim_gate_reclaim_locations_total", nil, int64(stats.Deleted))
	log.Infof("gate clean done: live_gates = %d, dead_gates = %d, members = %d, deleted = %d, skipped = %d",
		stats.LiveGates, stats.DeadGates, stats.Members, stats.Deleted, stats.Skipped)
}

func (a *App) runRTCTimeouts(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if calls, err := a.service.TimeoutCalls(ctx); err != nil {
				log.Warnf("rtc timeout sweep failed: %v", err)
			} else if len(calls) > 0 {
				for _, call := range calls {
					if a.recorder != nil {
						if recordErr := a.recorder.Record(ctx, call); recordErr != nil {
							log.Warnf("record missed call failed: call_id=%s err=%v", call.ID, recordErr)
						}
					}
				}
				log.Infof("rtc timeout sweep: calls = %d", len(calls))
			}
		}
	}
}
