package store

import (
	"os"
	"testing"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/event"
)

func read(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return b
}

// The captured hearing event is an ordinary M17 transmission into the
// reflector, so via_peer names the reflector itself rather than a peer hop.
func TestHearingEntryFromCapture(t *testing.T) {
	h, err := event.DecodeHearing(read(t, "hearing.json"))
	if err != nil {
		t.Fatalf("DecodeHearing: %v", err)
	}
	e := HearingEntry(h, "urfd")
	if e.Key() != "urfd:URF999:lastheard" {
		t.Errorf("Key() = %q, want %q", e.Key(), "urfd:URF999:lastheard")
	}
	for field, want := range map[string]string{
		"event":    "hearing",
		"callsign": "N0CALL",
		"repeater": "N0CALL",
		"rpt2":     "URF999  M",
		"module":   "M",
		"protocol": "M17",
		"ts":       "2026-09-22T21:25:28Z",
	} {
		if got := e.Fields[field]; got != want {
			t.Errorf("Fields[%q] = %q, want %q", field, got, want)
		}
	}
	// Not a peer hop, so the field is absent rather than misleading.
	if got, ok := e.Fields["via_peer"]; ok {
		t.Errorf("Fields[via_peer] = %q, want the field to be absent", got)
	}
}

// A genuine peer hop survives.
func TestHearingKeepsRealPeer(t *testing.T) {
	raw := `{"type":"hearing","timestamp":"2026-09-22T21:25:28Z","reflector":"URF999  ",
	         "callsign":"W0CHP   ","repeater":"W0CHP   ","rpt2":"URF999  A",
	         "via_peer":"URF301  A","module":"A","protocol":"URF"}`
	h, err := event.DecodeHearing([]byte(raw))
	if err != nil {
		t.Fatalf("DecodeHearing: %v", err)
	}
	e := HearingEntry(h, "urfd")
	if got := e.Fields["via_peer"]; got != "URF301  A" {
		t.Errorf("Fields[via_peer] = %q, want %q", got, "URF301  A")
	}
}

// The four protocols that publish a blank module still have the real one in
// rpt2 -- the same source the upstream fix reads. Recover it rather than
// writing an unknown module.
func TestHearingRecoversBlankModuleFromRpt2(t *testing.T) {
	raw := `{"type":"hearing","timestamp":"2026-09-22T21:25:28Z","reflector":"URF999  ",
	         "callsign":"N0CALL  ","repeater":"N0CALL  ","rpt2":"URF999  D",
	         "via_peer":"URF999  ","module":" ","protocol":"DMRPlus"}`
	h, err := event.DecodeHearing([]byte(raw))
	if err != nil {
		t.Fatalf("DecodeHearing: %v", err)
	}
	if h.Module != "D" {
		t.Errorf("Module = %q, want %q recovered from rpt2", h.Module, "D")
	}
	e := HearingEntry(h, "urfd")
	if got := e.Fields["module"]; got != "D" {
		t.Errorf("Fields[module] = %q, want %q", got, "D")
	}
}

// With no module anywhere, the field is omitted rather than written as a space.
func TestHearingOmitsUnrecoverableModule(t *testing.T) {
	raw := `{"type":"hearing","timestamp":"2026-09-22T21:25:28Z","reflector":"URF999  ",
	         "callsign":"N0CALL  ","repeater":"N0CALL  ","rpt2":"URF999  ",
	         "module":" ","protocol":"G3"}`
	h, err := event.DecodeHearing([]byte(raw))
	if err != nil {
		t.Fatalf("DecodeHearing: %v", err)
	}
	if h.Module != "" {
		t.Errorf("Module = %q, want empty", h.Module)
	}
	if _, ok := HearingEntry(h, "urfd").Fields["module"]; ok {
		t.Error("Fields[module] is present, want it omitted when unknown")
	}
}

func TestClientEntryFromCapture(t *testing.T) {
	c, err := event.DecodeClient(read(t, "client_connect.json"))
	if err != nil {
		t.Fatalf("DecodeClient: %v", err)
	}
	e := ClientEntry(c, "urfd")
	if e.Key() != "urfd:URF999:events" {
		t.Errorf("Key() = %q, want %q", e.Key(), "urfd:URF999:events")
	}
	for field, want := range map[string]string{
		"event":    "client_connect",
		"callsign": "N0CALL",
		"ip":       "127.0.0.1",
		"module":   "M",
		"protocol": "M17",
	} {
		if got := e.Fields[field]; got != want {
			t.Errorf("Fields[%q] = %q, want %q", field, got, want)
		}
	}
}

func TestClosingEntry(t *testing.T) {
	raw := `{"type":"closing","timestamp":"2026-09-22T21:25:29Z","reflector":"URF999  ",
	         "callsign":"N0CALL  ","module":"M","protocol":"M17"}`
	c, err := event.DecodeClosing([]byte(raw))
	if err != nil {
		t.Fatalf("DecodeClosing: %v", err)
	}
	e := ClosingEntry(c, "urfd")
	if e.Suffix != StreamLastHeard {
		t.Errorf("Suffix = %q, want %q: a transmission's start and end belong in one stream", e.Suffix, StreamLastHeard)
	}
	if _, ok := e.Fields["recording"]; ok {
		t.Error("Fields[recording] present with no recording configured")
	}
}
