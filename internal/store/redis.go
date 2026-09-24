package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store applies snapshots to Redis.
type Store struct {
	c *redis.Client
}

func New(c *redis.Client) *Store { return &Store{c: c} }

// Ping fails fast at startup rather than on the first snapshot.
func (s *Store) Ping(ctx context.Context) error {
	return s.c.Ping(ctx).Err()
}

// UpdatesChannel is the doorbell a consumer can subscribe to instead of polling.
const UpdatesChannel = ":updates"

// Apply writes one snapshot in a single MULTI/EXEC, so a consumer never reads
// a half-updated reflector: the keys change together or not at all. The
// doorbell PUBLISH rides in the same transaction, which means a subscriber is
// never woken before the keys it will read are visible, and a commit never
// happens without waking it.
func (s *Store) Apply(ctx context.Context, snap *Snapshot) error {
	tx := s.c.TxPipeline()
	hash := snap.Base + ":reflector"
	// HSet leaves fields a later snapshot drops in place, so delete first: a
	// sponsor or url cleared in the ini should disappear from Redis too.
	tx.Del(ctx, hash)
	tx.HSet(ctx, hash, toPairs(snap.Reflector))
	tx.Expire(ctx, hash, snap.TTL)
	for suffix, payload := range snap.JSON {
		tx.Set(ctx, snap.Base+suffix, payload, snap.TTL)
	}
	// Epoch milliseconds: enough resolution to tell two commits apart, which
	// the reflector's whole-second timestamps are not.
	tx.Publish(ctx, snap.Base+UpdatesChannel, strconv.FormatInt(time.Now().UnixMilli(), 10))
	if _, err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("writing snapshot for %s: %w", snap.Base, err)
	}
	return nil
}

func toPairs(m map[string]string) []any {
	pairs := make([]any, 0, len(m)*2)
	for k, v := range m {
		pairs = append(pairs, k, v)
	}
	return pairs
}
