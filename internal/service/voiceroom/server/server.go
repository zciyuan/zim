package server

import (
	"context"
	"time"

	"github.com/dobyte/due/v2/cluster/mesh"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/zciyuan/ZIM/internal/define"
	pb "github.com/zciyuan/ZIM/internal/service/voiceroom/pb"
	"github.com/zciyuan/ZIM/internal/utils/snowflake"
	"github.com/zciyuan/ZIM/zimfw/rtc"
)

var _ pb.VoiceroomServer = (*Service)(nil)

// New returns a voiceroom service. proxy may be nil in unit tests; client and
// media must be set in production and in tests that exercise seat/room logic.
func New(proxy *mesh.Proxy, repo VoiceRoomRepo, client redis.UniversalClient,
	media rtc.Provider, pusher Pusher, ids *snowflake.Generator, cfg Config) *Service {
	if cfg.SeatCount <= 0 {
		cfg.SeatCount = DefaultConfig().SeatCount
	}
	if cfg.ReconnectTimeout <= 0 {
		cfg.ReconnectTimeout = DefaultConfig().ReconnectTimeout
	}
	if cfg.EmptyTimeout <= 0 {
		cfg.EmptyTimeout = DefaultConfig().EmptyTimeout
	}
	if cfg.RoomTTL <= 0 {
		cfg.RoomTTL = DefaultConfig().RoomTTL
	}
	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = DefaultConfig().TokenTTL
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = DefaultConfig().ReapInterval
	}
	if ids == nil {
		ids = snowflake.New()
	}
	if pusher == nil {
		pusher = logPusher{}
	}
	return &Service{
		proxy: proxy, repo: repo, client: client, media: media,
		pusher: pusher, ids: ids, cfg: cfg, now: time.Now,
	}
}

// Name implements due's component interface.
func (s *Service) Name() string { return define.ServiceVoiceroom }

// Init registers the mesh service provider under the "voiceroom" discovery
// name. It is idempotent: the due container invokes component Init() on start
// in addition to the explicit call in cmd/zim-voiceroom/app.
func (s *Service) Init() {
	if s.proxy == nil {
		return
	}
	s.initOnce.Do(func() {
		s.proxy.AddServiceProvider(define.ServiceNameVoiceroom, &pb.Voiceroom_ServiceDesc, s)
	})
}

// Start launches the reconnect/empty reaper loop.
func (s *Service) Start() {
	if s.client == nil {
		return
	}
	s.once.Do(func() {
		s.stop = make(chan struct{})
		go s.reapLoop()
	})
}

// Close stops the reaper loop.
func (s *Service) Close() {
	s.once.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
	})
}

// Destroy implements due's component lifecycle.
func (s *Service) Destroy() { s.Close() }

func (s *Service) reapLoop() {
	ticker := time.NewTicker(s.cfg.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if count, err := s.ReapReconnects(ctx); err != nil {
				log.Warnf("voiceroom reap reconnects failed: err = %v", err)
			} else if count > 0 {
				log.Infof("voiceroom reap reconnects reclaimed %d seats", count)
			}
			if count, err := s.ReapEmptyRooms(ctx); err != nil {
				log.Warnf("voiceroom reap empty rooms failed: err = %v", err)
			} else if count > 0 {
				log.Infof("voiceroom reap empty rooms closed %d rooms", count)
			}
			cancel()
		case <-s.stop:
			return
		}
	}
}

// logPusher is the default no-op pusher; the mesh assembly wires a real one.
type logPusher struct{}

func (logPusher) PushRoom(ctx context.Context, room *Room, opcode int32, payload []byte, uids []int64) {
	log.Warnf("voiceroom push not wired: room_id = %d, route = %d, targets = %d", room.ID, opcode, len(uids))
}
