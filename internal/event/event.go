// Package event decodes the JSON urfd publishes on its NNG PUB socket.
//
// The relay targets the corrected event shape: every event carries type,
// timestamp and reflector, and the hearing event's field names hold what they
// say. A publisher without that shape is rejected rather than repaired -- see
// ErrNoEnvelope.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Event types urfd publishes.
const (
	TypeState            = "state"
	TypeHearing          = "hearing"
	TypeClosing          = "closing"
	TypeClientConnect    = "client_connect"
	TypeClientDisconnect = "client_disconnect"
)

// ErrNoEnvelope means the publisher predates the timestamp/reflector fields.
// The relay does not carry a compatibility path for that shape: it would have
// to supply identity from its own config and stamp receipt time, and both were
// removed deliberately.
var ErrNoEnvelope = errors.New("event carries no timestamp/reflector envelope: reflector needs the NNG event fixes")

// Envelope is the three fields every event carries.
type Envelope struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	// Reflector is the publishing reflector's own callsign. Named reflector,
	// not callsign, because callsign is the station an event is about.
	Reflector string `json:"reflector"`
}

// Time parses the envelope timestamp. Precision is whole seconds, so two
// events in the same second carry no order between them; callers that need a
// tiebreak use the Redis stream id.
func (e Envelope) Time() (time.Time, error) {
	return time.Parse(time.RFC3339, e.Timestamp)
}

// State is the periodic full snapshot.
type State struct {
	Envelope
	Configure     map[string]any   `json:"Configure"`
	Peers         []map[string]any `json:"Peers"`
	Clients       []map[string]any `json:"Clients"`
	Users         []map[string]any `json:"Users"`
	ActiveTalkers []map[string]any `json:"ActiveTalkers"`
}

// Interval returns the reflector's configured broadcast interval, which sets
// the snapshot TTL. Falls back to urfd's own default when absent.
func (s State) Interval() time.Duration {
	if v, ok := s.Configure["DashboardInterval"]; ok {
		if f, ok := v.(float64); ok && f > 0 {
			return time.Duration(f) * time.Second
		}
	}
	return 10 * time.Second
}

// Decode reads one published message. It returns the envelope for dispatch and
// the raw payload, so a caller only unmarshals the types it handles.
func Decode(b []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return env, fmt.Errorf("decoding event: %w", err)
	}
	if env.Type == "" {
		return env, errors.New("event has no type discriminator")
	}
	if env.Timestamp == "" || env.Reflector == "" {
		return env, ErrNoEnvelope
	}
	env.Reflector = TrimCS(env.Reflector)
	return env, nil
}

// DecodeState unmarshals a state payload and trims the padding out of it.
func DecodeState(b []byte) (*State, error) {
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("decoding state: %w", err)
	}
	if s.Timestamp == "" || s.Reflector == "" {
		return nil, ErrNoEnvelope
	}
	s.Reflector = TrimCS(s.Reflector)
	trimRows(s.Peers)
	trimRows(s.Clients)
	trimRows(s.Users)
	trimRows(s.ActiveTalkers)
	if cs, ok := s.Configure["Callsign"].(string); ok {
		s.Configure["Callsign"] = TrimCS(cs)
	}
	return &s, nil
}

// TrimCS removes the padding urfd writes around callsigns. Callsigns arrive
// space-padded to eight characters -- "N0CALL  ", or nine with a module,
// "URF999  M" -- so an untrimmed value matches nothing downstream.
func TrimCS(s string) string { return strings.TrimSpace(s) }

// trimRows trims every string in a state array. These rows hold callsigns,
// module letters and timestamps, none of which want surrounding whitespace.
// A module that trims to empty is an unknown module, not module " ".
func trimRows(rows []map[string]any) {
	for _, row := range rows {
		for k, v := range row {
			if s, ok := v.(string); ok {
				row[k] = TrimCS(s)
			}
		}
	}
}

// SplitCS separates a callsign from its module. urfd pads a callsign to eight
// characters and appends the module as a ninth, so "URF999  M" is the callsign
// URF999 on module M. Anything shorter carries no module.
func SplitCS(s string) (callsign, module string) {
	if len(s) >= 9 {
		return TrimCS(s[:8]), TrimCS(s[8:9])
	}
	return TrimCS(s), ""
}

// Hearing is a transmission starting. One event per transmission, emitted when
// the reflector opens the stream -- not per voice frame, despite what urfd's
// docs/nng.md says.
type Hearing struct {
	Envelope
	Callsign string `json:"callsign"`
	Repeater string `json:"repeater"`
	Rpt2     string `json:"rpt2"`
	ViaPeer  string `json:"via_peer"`
	Module   string `json:"module"`
	Protocol string `json:"protocol"`
}

// Closing is a transmission ending.
type Closing struct {
	Envelope
	Callsign  string `json:"callsign"`
	Module    string `json:"module"`
	Protocol  string `json:"protocol"`
	Recording string `json:"recording"`
}

// Client is a link or unlink: client_connect and client_disconnect.
type Client struct {
	Envelope
	Callsign string `json:"callsign"`
	IP       string `json:"ip"`
	Module   string `json:"module"`
	Protocol string `json:"protocol"`
}

// DecodeHearing applies the ingest rules a hearing event needs:
//
//   - padding is trimmed;
//   - via_peer is dropped when it names this reflector rather than a peer,
//     which M17 does for an ordinary transmission;
//   - a blank module is recovered from rpt2, whose ninth character is the
//     module the transmission was heard on. Four protocols publish a blank
//     module (G3, DMRPlus, IMRS, USRP) because the reflector reads it from the
//     wrong callsign; rpt2 is the same source the upstream fix uses, so this
//     recovers the module rather than merely detecting that it is missing.
func DecodeHearing(b []byte) (*Hearing, error) {
	var h Hearing
	if err := json.Unmarshal(b, &h); err != nil {
		return nil, fmt.Errorf("decoding hearing: %w", err)
	}
	if h.Timestamp == "" || h.Reflector == "" {
		return nil, ErrNoEnvelope
	}
	h.Reflector = TrimCS(h.Reflector)
	h.Callsign = TrimCS(h.Callsign)
	h.Repeater = TrimCS(h.Repeater)
	h.Rpt2 = TrimCS(h.Rpt2)
	h.Module = TrimCS(h.Module)

	peer, _ := SplitCS(h.ViaPeer)
	if peer == "" || peer == h.Reflector {
		h.ViaPeer = ""
	} else {
		h.ViaPeer = TrimCS(h.ViaPeer)
	}

	if h.Module == "" {
		if _, mod := SplitCS(h.Rpt2); mod != "" {
			h.Module = mod
		}
	}
	return &h, nil
}

// DecodeClosing decodes a transmission-end event.
func DecodeClosing(b []byte) (*Closing, error) {
	var c Closing
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decoding closing: %w", err)
	}
	if c.Timestamp == "" || c.Reflector == "" {
		return nil, ErrNoEnvelope
	}
	c.Reflector = TrimCS(c.Reflector)
	c.Callsign = TrimCS(c.Callsign)
	c.Module = TrimCS(c.Module)
	return &c, nil
}

// DecodeClient decodes a link or unlink event.
func DecodeClient(b []byte) (*Client, error) {
	var c Client
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decoding client event: %w", err)
	}
	if c.Timestamp == "" || c.Reflector == "" {
		return nil, ErrNoEnvelope
	}
	c.Reflector = TrimCS(c.Reflector)
	c.Callsign = TrimCS(c.Callsign)
	c.Module = TrimCS(c.Module)
	c.IP = TrimCS(c.IP)
	return &c, nil
}
