package app

import (
	"context"
	"sync"

	"github.com/dobyte/due/v2/component"

	"github.com/zciyuan/ZIM/internal/model"
	conversationsvc "github.com/zciyuan/ZIM/internal/service/conversation/server"
	messagesvc "github.com/zciyuan/ZIM/internal/service/message/server"
	webhook "github.com/zciyuan/ZIM/internal/service/webhook/server"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/idem"
	"github.com/zciyuan/ZIM/zimfw/pipe"
	"github.com/zciyuan/ZIM/zimfw/seq"
)

// Option configures the transfer pipeline.
type Option func(*App)

// App is the transfer pipeline component.
type App struct {
	component.Base
	repo        messagesvc.Repository
	seq         *seq.Allocator
	idem        idem.Store
	conv        *conversationsvc.Service
	consumers   []pipe.Consumer
	producer    pipe.Producer
	results     messagesvc.ResultQueue
	ids         *snowflake.Generator
	webhook     webhook.Notifier
	cancel      context.CancelFunc
	batchCh     chan *batchRequest
	workers     []chan *batchRequest
	workerCount int
	initialized sync.Map
	seqChecked  sync.Map
	previewMu   sync.Mutex
	previews    map[string]*model.Conversation
}

type batchRequest struct {
	ctx    context.Context
	event  *messagesvc.Event
	fanout bool
	done   chan error
}
