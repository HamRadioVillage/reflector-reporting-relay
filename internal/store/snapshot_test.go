package store

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/event"
)

func realState(t *testing.T) *event.State {
	t.Helper()
	b, err := os.ReadFile("testdata/state.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	st, err := event.DecodeState(b)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	return st
}

func TestBuildSnapshotKeysAndTTL(t *testing.T) {
	snap, err := BuildSnapshot(realState(t), "urfd", 3)
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	if snap.Base != "urfd:URF999" {
		t.Errorf("Base = %q, want %q", snap.Base, "urfd:URF999")
	}
	// The fixture's reflector broadcasts every 3s, so a factor of 3 is 9s.
	if snap.TTL != 9*time.Second {
		t.Errorf("TTL = %v, want 9s", snap.TTL)
	}
	got := snap.Keys()
	sort.Strings(got)
	want := []string{
		"urfd:URF999:activetalkers",
		"urfd:URF999:clients",
		"urfd:URF999:config",
		"urfd:URF999:peers",
		"urfd:URF999:reflector",
		"urfd:URF999:users",
	}
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("keys[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildSnapshotReflectorHash(t *testing.T) {
	snap, err := BuildSnapshot(realState(t), "urfd", 3)
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	for field, want := range map[string]string{
		"callsign":   "URF999",
		"modules":    "ADMSZ",
		"country":    "US",
		"sponsor":    "Local Test",
		"url":        "http://127.0.0.1/",
		"transcoded": "", // Transcoder Port = 0 in the captured run
		"updatedat":  "2026-09-22T21:27:27Z",
	} {
		if got := snap.Reflector[field]; got != want {
			t.Errorf("Reflector[%q] = %q, want %q", field, got, want)
		}
	}
	// urfd's JsonReport() publishes no version, so the hash must not invent one.
	if _, ok := snap.Reflector["version"]; ok {
		t.Error("Reflector has a version field, but the event stream carries no version")
	}
}

func TestBuildSnapshotPayloadsAreTrimmed(t *testing.T) {
	snap, err := BuildSnapshot(realState(t), "urfd", 3)
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	var users []map[string]any
	if err := json.Unmarshal([]byte(snap.JSON[":users"]), &users); err != nil {
		t.Fatalf("unmarshalling :users: %v", err)
	}
	if len(users) != 1 || users[0]["Callsign"] != "N0CALL" {
		t.Errorf(":users = %s, want one entry with a trimmed callsign", snap.JSON[":users"])
	}

	// An absent array is [] rather than null, so consumers can iterate.
	if got := snap.JSON[":peers"]; got != "[]" {
		t.Errorf(":peers = %s, want []", got)
	}
}

func TestBuildSnapshotRequiresReflector(t *testing.T) {
	st := realState(t)
	st.Reflector = ""
	if _, err := BuildSnapshot(st, "urfd", 3); err == nil {
		t.Fatal("BuildSnapshot with no reflector callsign: want an error")
	}
}

func TestBuildSnapshotHonoursPrefix(t *testing.T) {
	snap, err := BuildSnapshot(realState(t), "village", 4)
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	if snap.Base != "village:URF999" {
		t.Errorf("Base = %q, want %q", snap.Base, "village:URF999")
	}
	if snap.TTL != 12*time.Second {
		t.Errorf("TTL = %v, want 12s", snap.TTL)
	}
}
