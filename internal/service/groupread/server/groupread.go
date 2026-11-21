// Package groupread stores per-member group read watermarks with 5s coalescing.
package groupread

import (
	"context"
	"strconv"
	"time"

	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"
)

// New returns a group read service.
func New(client redis.UniversalClient) *Service {
	return &Service{
		client: client, ttl: readTTLSeconds, pending: make(map[int64]map[int64]int64),
	}
}

// Name implements due's component interface.
func (s *Service) Name() string { return "group-read" }

// Init implements due's component lifecycle.
func (s *Service) Init() {}

// Start starts the 5s flusher.
func (s *Service) Start() {
	s.start.Do(func() {
		s.stop = make(chan struct{})
		go s.loop()
	})
}

// Close flushes pending receipts and stops the flusher.
func (s *Service) Close() {
	s.close.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Flush(ctx)
	})
}

// Destroy implements due's component lifecycle.
func (s *Service) Destroy() { s.Close() }

func (s *Service) loop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			s.Flush(ctx)
			cancel()
		case <-s.stop:
			return
		}
	}
}

// Report records the highest read seq for one member; it does not write Redis
// until the next 5s flush.
func (s *Service) Report(_ context.Context, groupID, uid, seq int64) {
	if s.client == nil || groupID <= 0 || uid <= 0 || seq < 0 {
		return
	}
	s.mu.Lock()
	members := s.pending[groupID]
	if members == nil {
		members = make(map[int64]int64)
		s.pending[groupID] = members
	}
	if seq > members[uid] {
		members[uid] = seq
	}
	s.mu.Unlock()
}

// Flush writes all pending receipts with a max-only Lua update.
func (s *Service) Flush(ctx context.Context) {
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[int64]map[int64]int64)
	s.mu.Unlock()

	for groupID, members := range pending {
		for uid, seq := range members {
			if err := setMaxScript.Run(ctx, s.client, []string{readKey(groupID)},
				uid, seq, s.ttl).Err(); err != nil {
				log.Warnf("flush group read failed: gid = %d, uid = %d, err = %v", groupID, uid, err)
				s.mu.Lock()
				if s.pending[groupID] == nil {
					s.pending[groupID] = make(map[int64]int64)
				}
				if seq > s.pending[groupID][uid] {
					s.pending[groupID][uid] = seq
				}
				s.mu.Unlock()
			}
		}
	}
}

// Count returns how many members have read at least seq.
func (s *Service) Count(ctx context.Context, groupID, seq int64) (int, error) {
	s.Flush(ctx)
	values, err := s.client.HGetAll(ctx, readKey(groupID)).Result()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, value := range values {
		readSeq, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr == nil && readSeq >= seq {
			count++
		}
	}
	return count, nil
}
