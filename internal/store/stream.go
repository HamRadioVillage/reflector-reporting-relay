package store

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/event"
)

// Stream key suffixes. Two streams, split by what a consumer asks of them:
// :lastheard answers "who has been talking", :events answers "who is linked".
const (
	StreamLastHeard = ":lastheard"
	StreamEvents    = ":events"
)

// Entry is one stream append.
type Entry struct {
	// Base is the reflector's key prefix, e.g. "urfd:URF999".
	Base string
	// Suffix is StreamLastHeard or StreamEvents.
	Suffix string
	// Fields are the flattened event. Values are strings so that XRANGE output
	// needs no per-field type knowledge; an absent value is an absent field
	// rather than an empty one.
	Fields map[string]string
}

// Key is the stream this entry appends to.
func (e *Entry) Key() string { return e.Base + e.Suffix }

// HearingEntry turns a transmission start into a :lastheard append.
func HearingEntry(h *event.Hearing, prefix string) *Entry {
	f := map[string]string{
		"event":    event.TypeHearing,
		"ts":       h.Timestamp,
		"callsign": h.Callsign,
		"protocol": h.Protocol,
	}
	putIf(f, "module", h.Module)
	putIf(f, "repeater", h.Repeater)
	putIf(f, "rpt2", h.Rpt2)
	// Absent unless the transmission genuinely arrived via a peer.
	putIf(f, "via_peer", h.ViaPeer)
	return &Entry{Base: prefix + ":" + h.Reflector, Suffix: StreamLastHeard, Fields: f}
}

// ClosingEntry turns a transmission end into a :lastheard append, so a consumer
// reading one stream sees the whole transmission.
func ClosingEntry(c *event.Closing, prefix string) *Entry {
	f := map[string]string{
		"event":    event.TypeClosing,
		"ts":       c.Timestamp,
		"callsign": c.Callsign,
		"protocol": c.Protocol,
	}
	putIf(f, "module", c.Module)
	putIf(f, "recording", c.Recording)
	return &Entry{Base: prefix + ":" + c.Reflector, Suffix: StreamLastHeard, Fields: f}
}

// ClientEntry turns a link or unlink into an :events append.
func ClientEntry(c *event.Client, prefix string) *Entry {
	f := map[string]string{
		"event":    c.Type,
		"ts":       c.Timestamp,
		"callsign": c.Callsign,
		"protocol": c.Protocol,
	}
	putIf(f, "module", c.Module)
	// The IP is why this keyspace belongs on loopback or a private network.
	putIf(f, "ip", c.IP)
	return &Entry{Base: prefix + ":" + c.Reflector, Suffix: StreamEvents, Fields: f}
}

func putIf(f map[string]string, key, value string) {
	if value != "" {
		f[key] = value
	}
}

// Append writes one entry. The stream id is Redis-assigned, which also supplies
// the ordering urfd's whole-second timestamps cannot: two events in the same
// second still land in arrival order. MAXLEN is approximate so Redis can trim
// on node boundaries instead of walking the stream on every append.
func (s *Store) Append(ctx context.Context, e *Entry, maxLen int64) error {
	values := make(map[string]any, len(e.Fields))
	for k, v := range e.Fields {
		values[k] = v
	}
	err := s.c.XAdd(ctx, &redis.XAddArgs{
		Stream: e.Key(),
		MaxLen: maxLen,
		Approx: true,
		Values: values,
	}).Err()
	if err != nil {
		return fmt.Errorf("appending to %s: %w", e.Key(), err)
	}
	return nil
}
