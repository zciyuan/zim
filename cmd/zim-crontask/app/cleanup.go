package app

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/component/metrics"
)

const (
	uploadCleanBatch = 500
	uploadCleanWork  = 8
)

func (a *App) runUploadCleanup(ctx context.Context) {
	if a.uploadDir == "" || a.uploadAge <= 0 {
		return
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.cleanUploads()
		}
	}
}

// cleanUploads scans first, then deletes in bounded batches. Deleting while
// walking can invalidate ReadDir results on some filesystems.
func (a *App) cleanUploads() {
	cutoff := time.Now().Add(-a.uploadAge)
	expired := make([]string, 0, uploadCleanBatch)
	err := filepath.WalkDir(a.uploadDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if info.ModTime().Before(cutoff) {
			expired = append(expired, path)
		}
		return nil
	})
	if err != nil {
		log.Warnf("upload cleanup scan failed: %v", err)
		return
	}
	var removed atomic.Int64
	for start := 0; start < len(expired); start += uploadCleanBatch {
		end := start + uploadCleanBatch
		if end > len(expired) {
			end = len(expired)
		}
		batch := expired[start:end]
		jobs := make(chan string, len(batch))
		var workers sync.WaitGroup
		for i := 0; i < uploadCleanWork; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for path := range jobs {
					if removeErr := os.Remove(path); removeErr == nil {
						removed.Add(1)
					} else if !os.IsNotExist(removeErr) {
						log.Warnf("upload cleanup remove failed: path=%s err=%v", path, removeErr)
					}
				}
			}()
		}
		for _, path := range batch {
			jobs <- path
		}
		close(jobs)
		workers.Wait()
	}
	if count := removed.Load(); count > 0 {
		metrics.Default.Add("zim_upload_cleanup_total", nil, count)
		log.Infof("upload cleanup removed=%d", count)
	}
}
