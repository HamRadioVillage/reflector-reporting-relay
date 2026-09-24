package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "relay.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := Load(write(t, "sources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Redis.Addr != "127.0.0.1:6379" || c.Redis.KeyPrefix != "urfd" {
		t.Errorf("redis defaults = %+v", c.Redis)
	}
	if c.Defaults.SnapshotTTLFactor != 3 || c.Defaults.StreamMaxLen != 5000 || c.Defaults.QueueDepth != 4096 {
		t.Errorf("defaults = %+v", c.Defaults)
	}
	if c.Defaults.HeartbeatInterval.Duration != 10*time.Second {
		t.Errorf("heartbeat interval = %v, want 10s", c.Defaults.HeartbeatInterval)
	}
}

func TestLoadParsesDurations(t *testing.T) {
	c, err := Load(write(t, "defaults:\n  heartbeat_interval: 30s\nsources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Defaults.HeartbeatInterval.Duration != 30*time.Second {
		t.Errorf("heartbeat interval = %v, want 30s", c.Defaults.HeartbeatInterval)
	}
}

func TestLoadRejects(t *testing.T) {
	for name, body := range map[string]string{
		"no sources":         "redis:\n  addr: 127.0.0.1:6379\n",
		"no nng address":     "sources:\n  - callsign: URF999\n",
		"no callsign":        "sources:\n  - nng: tcp://127.0.0.1:5555\n",
		"duplicate callsign": "sources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5556\n",
		"ttl factor of 1":    "defaults:\n  snapshot_ttl_factor: 1\nsources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n",
		"sub-second beat":    "defaults:\n  heartbeat_interval: 100ms\nsources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n",
		"bad duration":       "defaults:\n  heartbeat_interval: soon\nsources:\n  - callsign: URF999\n    nng: tcp://127.0.0.1:5555\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(write(t, body)); err == nil {
				t.Errorf("Load(%s): want an error", name)
			}
		})
	}
}
