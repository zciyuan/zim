package app

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/log"

	"github.com/zciyuan/ZIM/internal/model"
)

const previewFlushInterval = 250 * time.Millisecond

// queuePreview coalesces conversation preview writes. A 2,000-member group at
// 100 msg/s writes one preview row per 250ms instead of 100 rows/s.
func (a *App) queuePreview(preview *model.Conversation) {
	if preview == nil || preview.ConvID == "" {
		return
	}
	a.previewMu.Lock()
	a.previews[preview.ConvID] = preview
	a.previewMu.Unlock()
}

func (a *App) runPreviewWriter(ctx context.Context) {
	ticker := time.NewTicker(previewFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.flushPreviews()
			return
		case <-ticker.C:
			a.flushPreviews()
		}
	}
}

func (a *App) flushPreviews() {
	a.previewMu.Lock()
	if len(a.previews) == 0 {
		a.previewMu.Unlock()
		return
	}
	batch := make([]*model.Conversation, 0, len(a.previews))
	for _, preview := range a.previews {
		batch = append(batch, preview)
	}
	a.previews = make(map[string]*model.Conversation)
	a.previewMu.Unlock()
	if err := a.conv.TouchPreviewBatch(context.Background(), batch); err != nil {
		log.Warnf("flush conversation previews failed: size = %d, err = %v", len(batch), err)
	}
}
