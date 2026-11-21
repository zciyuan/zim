package app

import (
	"context"
	"testing"
)

func TestWorkerForKeepsConversationOnOneChannel(t *testing.T) {
	app := &App{workerCount: 64}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.startWorkers(ctx)
	first := app.workerFor("s_1_2")
	for i := 0; i < 100; i++ {
		if app.workerFor("s_1_2") != first {
			t.Fatal("conversation moved between workers")
		}
	}
}
