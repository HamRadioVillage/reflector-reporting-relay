package store

import (
	"testing"
	"time"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/stats"
)

func TestBuildHeartbeatFields(t *testing.T) {
	started := time.Date(2026, 9, 24, 5, 0, 0, 0, time.UTC)
	now := started.Add(90 * time.Second)

	a := stats.New("URF999")
	a.MarkEvent(started.Add(80 * time.Second))
	a.Snapshots.Add(9)
	a.Entries.Add(4)
	a.Dropped.Add(2)
	a.RedisErrors.Add(1)
	b := stats.New("URF301") // configured but silent so far

	hb := BuildHeartbeat("urfd", started, now, []*stats.Source{a, b}, 10*time.Second)

	if hb.Key != "urfd:relay" {
		t.Errorf("Key = %q, want %q", hb.Key, "urfd:relay")
	}
	// Three missed writes and the key is gone, so absence means something.
	if hb.TTL != 30*time.Second {
		t.Errorf("TTL = %v, want 30s", hb.TTL)
	}
	for field, want := range map[string]string{
		"started":             "2026-09-24T05:00:00Z",
		"updatedat":           "2026-09-24T05:01:30Z",
		"interval":            "10s",
		"sources":             "URF999,URF301",
		"lastevent":           "2026-09-24T05:01:20Z",
		"URF999.received":     "1",
		"URF999.dropped":      "2",
		"URF999.snapshots":    "9",
		"URF999.entries":      "4",
		"URF999.rediserrors":  "1",
		"URF999.blankmodules": "0",
		"URF999.lastevent":    "2026-09-24T05:01:20Z",
		"URF301.received":     "0",
	} {
		if got := hb.Fields[field]; got != want {
			t.Errorf("Fields[%q] = %q, want %q", field, got, want)
		}
	}
	// A source that has heard nothing reports no lastevent rather than the
	// epoch, which would read as 1970 on a dashboard.
	if got, ok := hb.Fields["URF301.lastevent"]; ok {
		t.Errorf("Fields[URF301.lastevent] = %q, want it absent until the first event", got)
	}
}

// A relay that has heard nothing at all still writes a heartbeat: that is the
// difference between "relay is down" and "reflector is quiet".
func TestBuildHeartbeatWithNoTrafficYet(t *testing.T) {
	now := time.Date(2026, 9, 24, 5, 0, 0, 0, time.UTC)
	hb := BuildHeartbeat("urfd", now, now, []*stats.Source{stats.New("URF999")}, 10*time.Second)
	if _, ok := hb.Fields["lastevent"]; ok {
		t.Error("Fields[lastevent] present with no traffic, want absent")
	}
	if hb.Fields["started"] == "" || hb.Fields["updatedat"] == "" {
		t.Error("started/updatedat must be written even with no traffic")
	}
}
