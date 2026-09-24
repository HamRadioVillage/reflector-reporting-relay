package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/stats"
)

// HeartbeatKey is the relay's own key, one per relay rather than per reflector.
const HeartbeatKey = ":relay"

// Heartbeat is what the relay publishes about itself. Without it, a dashboard
// cannot tell a dead reflector from a dead relay: in both cases the snapshot
// keys simply expire. A fresh heartbeat with a stale lastevent means the relay
// is running and the reflector is not; no heartbeat at all means the relay is
// the problem.
type Heartbeat struct {
	Key    string
	Fields map[string]string
	// TTL outlives a few missed writes and nothing more, so the key's absence
	// is meaningful rather than merely old.
	TTL time.Duration
}

// BuildHeartbeat renders the relay's state as hash fields. Pure, so what a
// dashboard will read is testable without a Redis.
func BuildHeartbeat(prefix string, started, now time.Time, sources []*stats.Source, interval time.Duration) *Heartbeat {
	hb := &Heartbeat{
		Key: prefix + HeartbeatKey,
		TTL: 3 * interval,
		Fields: map[string]string{
			"started":   started.UTC().Format(time.RFC3339),
			"updatedat": now.UTC().Format(time.RFC3339),
			"interval":  interval.String(),
		},
	}

	var newest int64
	callsigns := make([]string, 0, len(sources))
	for _, s := range sources {
		callsigns = append(callsigns, s.Callsign)
		f := s.Callsign + "."
		hb.Fields[f+"received"] = strconv.FormatUint(s.Received.Load(), 10)
		hb.Fields[f+"dropped"] = strconv.FormatUint(s.Dropped.Load(), 10)
		hb.Fields[f+"snapshots"] = strconv.FormatUint(s.Snapshots.Load(), 10)
		hb.Fields[f+"entries"] = strconv.FormatUint(s.Entries.Load(), 10)
		hb.Fields[f+"blankmodules"] = strconv.FormatUint(s.BlankModules.Load(), 10)
		hb.Fields[f+"rediserrors"] = strconv.FormatUint(s.RedisErrors.Load(), 10)
		if ts, ok := s.LastEventTime(); ok {
			hb.Fields[f+"lastevent"] = ts.Format(time.RFC3339)
			if u := ts.Unix(); u > newest {
				newest = u
			}
		}
	}
	// sources lets a consumer discover which reflectors this relay watches
	// without scanning the keyspace.
	hb.Fields["sources"] = joinCallsigns(callsigns)
	if newest > 0 {
		hb.Fields["lastevent"] = time.Unix(newest, 0).UTC().Format(time.RFC3339)
	}
	return hb
}

func joinCallsigns(cs []string) string {
	out := ""
	for i, c := range cs {
		if i > 0 {
			out += ","
		}
		out += c
	}
	return out
}

// WriteHeartbeat replaces the heartbeat hash. DEL first so a source removed from
// the config stops being reported, rather than lingering at its final counts.
func (s *Store) WriteHeartbeat(ctx context.Context, hb *Heartbeat) error {
	tx := s.c.TxPipeline()
	tx.Del(ctx, hb.Key)
	tx.HSet(ctx, hb.Key, toPairs(hb.Fields))
	tx.Expire(ctx, hb.Key, hb.TTL)
	if _, err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("writing heartbeat %s: %w", hb.Key, err)
	}
	return nil
}
