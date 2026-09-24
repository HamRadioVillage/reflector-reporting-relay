// Package store builds and applies the Redis representation of reflector state.
package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/event"
)

// Snapshot is the complete set of writes one state message produces. Building
// it is pure, so what lands in Redis is testable without a Redis.
type Snapshot struct {
	// Base is the key prefix for this reflector, e.g. "urfd:URF999".
	Base string
	// Reflector is the :reflector hash: the small, human-facing summary.
	Reflector map[string]string
	// JSON maps a key suffix (":config", ":peers", ...) to its payload.
	JSON map[string]string
	// TTL is the expiry every key in this snapshot gets. It is the liveness
	// signal: when urfd stops, the keys expire and consumers see "offline"
	// without inspecting a file's mtime.
	TTL time.Duration
}

// Keys returns the full key names this snapshot writes, hash first.
func (s *Snapshot) Keys() []string {
	keys := []string{s.Base + ":reflector"}
	for suffix := range s.JSON {
		keys = append(keys, s.Base+suffix)
	}
	return keys
}

// BuildSnapshot turns a state event into the writes it implies. ttlFactor
// multiplies the reflector's own broadcast interval, so a snapshot outlives a
// missed broadcast or two but not a stopped reflector.
func BuildSnapshot(st *event.State, prefix string, ttlFactor int) (*Snapshot, error) {
	if st.Reflector == "" {
		return nil, fmt.Errorf("state event has no reflector callsign")
	}
	snap := &Snapshot{
		Base: prefix + ":" + st.Reflector,
		TTL:  time.Duration(ttlFactor) * st.Interval(),
		JSON: make(map[string]string, 5),
	}

	// The :reflector hash is the identity a dashboard shows in a header. Every
	// value comes out of the Configure block except updatedat, which is the
	// event's own timestamp -- the reflector's clock, not ours.
	snap.Reflector = map[string]string{
		"callsign":   st.Reflector,
		"modules":    configString(st.Configure, "Modules"),
		"transcoded": configString(st.Configure, "TranscodedModules"),
		"country":    configString(st.Configure, "Country"),
		"sponsor":    configString(st.Configure, "Sponsor"),
		"url":        configString(st.Configure, "DashboardUrl"),
		"updatedat":  st.Timestamp,
	}
	// No version: urfd's JsonReport() does not publish one. It appears only in
	// the startup log, so the event stream cannot supply it.

	for suffix, payload := range map[string]any{
		":config":        st.Configure,
		":peers":         emptyIfNil(st.Peers),
		":clients":       emptyIfNil(st.Clients),
		":users":         emptyIfNil(st.Users),
		":activetalkers": emptyIfNil(st.ActiveTalkers),
	} {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshalling %s: %w", suffix, err)
		}
		snap.JSON[suffix] = string(b)
	}
	return snap, nil
}

func configString(cfg map[string]any, key string) string {
	if v, ok := cfg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// emptyIfNil keeps an absent array out of Redis as [] rather than null, so a
// consumer can iterate without a nil check.
func emptyIfNil(rows []map[string]any) []map[string]any {
	if rows == nil {
		return []map[string]any{}
	}
	return rows
}
