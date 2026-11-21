// Package reaction stores per-message emoji reactions.
//
// Redis holds the live counters; the authoritative per-user rows belong to the
// message store in a durable deployment (ZIM-02 §Q56). The service is small on
// purpose: one pair of keys per message, atomic add/remove, batched reads for a
// history page.
package reaction

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// ttl keeps counters for a bounded window; message retention owns the truth.
	ttl = 30 * 24 * time.Hour
	// maxEmojis bounds one message's distinct reactions.
	maxEmojis = 20
	// maxEmojiRunes guards against payload abuse.
	maxEmojiRunes = 8
)

// Service keeps reaction counters in Redis.
type Service struct {
	client redis.UniversalClient
}

// New returns a reaction service.
func New(client redis.UniversalClient) *Service {
	return &Service{client: client}
}

func countKey(conversationID string, seq int64) string {
	return fmt.Sprintf("zim:reaction:%s:%d", conversationID, seq)
}

func mineKey(conversationID string, seq, uid int64) string {
	return fmt.Sprintf("zim:reaction:mine:%s:%d:%d", conversationID, seq, uid)
}

// Add records one emoji from uid. It is idempotent per (uid, emoji).
func (s *Service) Add(ctx context.Context, conversationID string, seq, uid int64, emoji string) error {
	if s.client == nil || conversationID == "" || seq <= 0 || uid <= 0 {
		return ErrInvalidArgument
	}
	if emoji == "" || len([]rune(emoji)) > maxEmojiRunes {
		return ErrInvalidEmoji
	}
	counts, mine := countKey(conversationID, seq), mineKey(conversationID, seq, uid)
	existing, err := s.client.HLen(ctx, counts).Result()
	if err != nil {
		return err
	}
	if existing >= maxEmojis {
		member, _ := s.client.SIsMember(ctx, mine, emoji).Result()
		if !member {
			return ErrTooManyEmojis
		}
	}
	if err = addScript.Run(ctx, s.client, []string{counts, mine}, emoji).Err(); err != nil {
		return err
	}
	s.touch(ctx, counts, mine)
	return nil
}

// Remove deletes one emoji from uid. It is idempotent.
func (s *Service) Remove(ctx context.Context, conversationID string, seq, uid int64, emoji string) error {
	if s.client == nil || conversationID == "" || seq <= 0 || uid <= 0 {
		return ErrInvalidArgument
	}
	if emoji == "" {
		return ErrInvalidEmoji
	}
	counts, mine := countKey(conversationID, seq), mineKey(conversationID, seq, uid)
	if err := removeScript.Run(ctx, s.client, []string{counts, mine}, emoji).Err(); err != nil {
		return err
	}
	s.touch(ctx, counts, mine)
	return nil
}

// List returns reactions for a batch of message seqs, including whether uid
// reacted. Missing messages simply have no entry.
func (s *Service) List(ctx context.Context, conversationID string, seqs []int64, uid int64) (map[int64][]Item, error) {
	result := make(map[int64][]Item, len(seqs))
	if s.client == nil || len(seqs) == 0 {
		return result, nil
	}

	countPipe := s.client.Pipeline()
	countCmds := make([]*redis.MapStringStringCmd, len(seqs))
	for i, seq := range seqs {
		countCmds[i] = countPipe.HGetAll(ctx, countKey(conversationID, seq))
	}
	if _, err := countPipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	minePipe := s.client.Pipeline()
	mineCmds := make([]*redis.StringSliceCmd, len(seqs))
	for i, seq := range seqs {
		mineCmds[i] = minePipe.SMembers(ctx, mineKey(conversationID, seq, uid))
	}
	if _, err := minePipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	for i, seq := range seqs {
		counts, err := countCmds[i].Result()
		if err != nil || len(counts) == 0 {
			continue
		}
		mineList, _ := mineCmds[i].Result()
		mineSet := make(map[string]struct{}, len(mineList))
		for _, emoji := range mineList {
			mineSet[emoji] = struct{}{}
		}
		items := make([]Item, 0, len(counts))
		for emoji, raw := range counts {
			count, convErr := strconv.Atoi(raw)
			if convErr != nil || count <= 0 {
				continue
			}
			_, ok := mineSet[emoji]
			items = append(items, Item{Emoji: emoji, Count: int32(count), Mine: ok})
		}
		if len(items) > 0 {
			result[seq] = items
		}
	}
	return result, nil
}

// touch refreshes the TTL on both keys.
func (s *Service) touch(ctx context.Context, keys ...string) {
	pipe := s.client.Pipeline()
	for _, key := range keys {
		pipe.Expire(ctx, key, ttl)
	}
	_, _ = pipe.Exec(ctx)
}
