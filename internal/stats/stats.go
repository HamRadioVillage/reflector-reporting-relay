// Package stats holds the per-source counters the relay publishes in its
// heartbeat. One Source is shared by the goroutine receiving from NNG, the
// goroutine writing Redis, and the one writing the heartbeat, so every field is
// atomic.
package stats

import (
	"sync/atomic"
	"time"
)

// Source counts one reflector's traffic through the relay.
type Source struct {
	// Callsign identifies the source in heartbeat field names.
	Callsign string

	// Received and Dropped are the NNG receive path. Dropped means the relay
	// could not keep up with its own queue -- distinct from urfd dropping on
	// backpressure, which nobody downstream can observe.
	Received atomic.Uint64
	Dropped  atomic.Uint64

	// Snapshots and Entries are successful Redis writes.
	Snapshots atomic.Uint64
	Entries   atomic.Uint64

	// BlankModules counts hearing events whose module could not be recovered
	// from rpt2 either. Non-zero means a reflector without W0CHP/urfd#2 and a
	// protocol whose rpt2 carries no module.
	BlankModules atomic.Uint64

	// RedisErrors counts failed writes. A rising count with a healthy reflector
	// means the relay, not the reflector, is the problem.
	RedisErrors atomic.Uint64

	// LastEvent is when a message last arrived, unix seconds. Zero until the
	// first one. A fresh heartbeat with a stale LastEvent is the signature of a
	// live relay watching a dead reflector.
	LastEvent atomic.Int64
}

func New(callsign string) *Source { return &Source{Callsign: callsign} }

// MarkEvent records the arrival of a message.
func (s *Source) MarkEvent(now time.Time) {
	s.Received.Add(1)
	s.LastEvent.Store(now.Unix())
}

// LastEventTime returns the last arrival and whether there has been one.
func (s *Source) LastEventTime() (time.Time, bool) {
	sec := s.LastEvent.Load()
	if sec == 0 {
		return time.Time{}, false
	}
	return time.Unix(sec, 0).UTC(), true
}
