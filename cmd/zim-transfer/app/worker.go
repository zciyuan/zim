package app

import (
	"context"
	"hash/fnv"
)

// WithWorkers sets the fixed conversation-shard worker count.
func WithWorkers(count int) Option {
	return func(a *App) {
		if count > 0 {
			a.workerCount = count
		}
	}
}

func (a *App) startWorkers(ctx context.Context) {
	a.workers = make([]chan *batchRequest, a.workerCount)
	for index := range a.workers {
		channel := make(chan *batchRequest, 256)
		a.workers[index] = channel
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-channel:
					request.done <- a.consume(request.ctx, request.event, request.fanout)
				}
			}
		}()
	}
}

// workerFor 用会话 ID 哈希固定 worker：同一会话的事件始终在同一 goroutine 串行，
// 保证 seq 分配、落库和 toPush 顺序不被并发打乱。
func (a *App) workerFor(convID string) chan *batchRequest {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(convID))
	return a.workers[int(hash.Sum32())%len(a.workers)]
}
