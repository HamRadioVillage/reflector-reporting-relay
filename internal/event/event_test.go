package event

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

// The fixtures in testdata are messages captured off a live reflector's NNG
// socket, not hand-written examples.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return b
}

func TestDecodeEnvelope(t *testing.T) {
	env, err := Decode(fixture(t, "client_connect.json"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if env.Type != TypeClientConnect {
		t.Errorf("type = %q, want %q", env.Type, TypeClientConnect)
	}
	// The reflector callsign arrives padded to eight characters.
	if env.Reflector != "URF999" {
		t.Errorf("reflector = %q, want %q (padding must be trimmed)", env.Reflector, "URF999")
	}
	ts, err := env.Time()
	if err != nil {
		t.Fatalf("Time: %v", err)
	}
	if ts.IsZero() || ts.Location() != time.UTC {
		t.Errorf("timestamp = %v, want a UTC time", ts)
	}
}

// A publisher without the envelope is refused rather than repaired: supplying
// identity from config and stamping receipt time were both removed on purpose.
func TestDecodeRejectsUnfixedPublisher(t *testing.T) {
	stock := `{"type":"hearing","my":"N0CALL  ","ur":"N0CALL  ","rpt1":"URF999  M","rpt2":"URF999  M","module":"M","protocol":"M17"}`
	if _, err := Decode([]byte(stock)); !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("Decode(stock shape) error = %v, want ErrNoEnvelope", err)
	}
}

func TestDecodeRequiresType(t *testing.T) {
	if _, err := Decode([]byte(`{"timestamp":"2026-09-22T21:25:27Z","reflector":"URF999  "}`)); err == nil {
		t.Fatal("Decode with no type: want an error")
	}
}

func TestDecodeStateTrimsPadding(t *testing.T) {
	st, err := DecodeState(fixture(t, "state.json"))
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	if st.Reflector != "URF999" {
		t.Errorf("reflector = %q, want %q", st.Reflector, "URF999")
	}
	if got := st.Configure["Callsign"]; got != "URF999" {
		t.Errorf("Configure.Callsign = %q, want %q", got, "URF999")
	}
	if len(st.Users) != 1 {
		t.Fatalf("got %d users, want 1", len(st.Users))
	}
	for _, field := range []struct{ key, want string }{
		{"Callsign", "N0CALL"},
		{"Repeater", "N0CALL"},
		{"ViaPeer", "URF999  M"}, // callsign + module: the module is data, not padding
		{"OnModule", "M"},
	} {
		got, _ := st.Users[0][field.key].(string)
		if field.key == "ViaPeer" {
			// "URF999  M" trims to itself: the inner spaces belong to the
			// callsign field width, and a consumer that wants the bare
			// callsign splits on the module rather than trimming.
			if got != "URF999  M" {
				t.Errorf("Users[0].ViaPeer = %q, want %q", got, "URF999  M")
			}
			continue
		}
		if got != field.want {
			t.Errorf("Users[0].%s = %q, want %q", field.key, got, field.want)
		}
	}
	if len(st.Clients) != 1 || st.Clients[0]["Callsign"] != "N0CALL" {
		t.Errorf("Clients = %v, want one trimmed N0CALL", st.Clients)
	}
}

func TestStateIntervalFromConfigure(t *testing.T) {
	st, err := DecodeState(fixture(t, "state.json"))
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	if got := st.Interval(); got != 3*time.Second {
		t.Errorf("Interval() = %v, want 3s (the fixture's DashboardInterval)", got)
	}

	// Absent DashboardInterval falls back to urfd's own default.
	var bare State
	if err := json.Unmarshal([]byte(`{"type":"state","timestamp":"2026-09-22T21:25:30Z","reflector":"URF999  ","Configure":{}}`), &bare); err != nil {
		t.Fatal(err)
	}
	if got := bare.Interval(); got != 10*time.Second {
		t.Errorf("Interval() with no DashboardInterval = %v, want 10s", got)
	}
}

// A module that is a single space is an unknown module, not module " ". Four
// protocols emit that until the rpt2 fix lands upstream.
func TestTrimBlankModuleBecomesEmpty(t *testing.T) {
	if got := TrimCS(" "); got != "" {
		t.Errorf("TrimCS(%q) = %q, want empty", " ", got)
	}
}
