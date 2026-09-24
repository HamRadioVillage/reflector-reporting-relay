// Package config loads and validates the relay's YAML configuration.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Redis is how the relay reaches its Redis instance. Loopback or a unix socket
// by default: the keyspace holds client IP addresses once event streams land.
type Redis struct {
	Addr      string `yaml:"addr"`
	DB        int    `yaml:"db"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	KeyPrefix string `yaml:"key_prefix"`
}

// Defaults are per-source settings that a source does not override.
type Defaults struct {
	StreamMaxLen      int64 `yaml:"stream_maxlen"`
	SnapshotTTLFactor int   `yaml:"snapshot_ttl_factor"`
	// QueueDepth bounds the hand-off between the NNG receive loop and the
	// Redis writer. The receive loop must never block on Redis: urfd sends
	// with NNG_FLAG_NONBLOCK and discards silently when a subscriber is slow.
	QueueDepth int `yaml:"queue_depth"`
}

// Source is one reflector to subscribe to.
type Source struct {
	// Callsign is an assertion, not the source of truth. Every event carries
	// its own reflector callsign; a mismatch is a misconfiguration.
	Callsign string `yaml:"callsign"`
	NNG      string `yaml:"nng"`
}

type Config struct {
	Redis    Redis    `yaml:"redis"`
	Defaults Defaults `yaml:"defaults"`
	Sources  []Source `yaml:"sources"`
}

// Load reads path, applies defaults, and validates what cannot be defaulted.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	c.applyDefaults()
	return &c, c.validate()
}

func (c *Config) applyDefaults() {
	if c.Redis.Addr == "" {
		c.Redis.Addr = "127.0.0.1:6379"
	}
	if c.Redis.KeyPrefix == "" {
		c.Redis.KeyPrefix = "urfd"
	}
	if c.Defaults.StreamMaxLen == 0 {
		c.Defaults.StreamMaxLen = 5000
	}
	if c.Defaults.SnapshotTTLFactor == 0 {
		c.Defaults.SnapshotTTLFactor = 3
	}
	if c.Defaults.QueueDepth == 0 {
		c.Defaults.QueueDepth = 4096
	}
}

func (c *Config) validate() error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("no sources configured: the relay has nothing to subscribe to")
	}
	seen := make(map[string]string, len(c.Sources))
	for i, s := range c.Sources {
		if s.NNG == "" {
			return fmt.Errorf("sources[%d]: nng address is required", i)
		}
		cs := strings.TrimSpace(s.Callsign)
		if cs == "" {
			return fmt.Errorf("sources[%d] (%s): callsign is required", i, s.NNG)
		}
		if prev, dup := seen[cs]; dup {
			return fmt.Errorf("sources[%d]: callsign %s already used by %s; two reflectors cannot share a keyspace", i, cs, prev)
		}
		seen[cs] = s.NNG
	}
	if c.Defaults.SnapshotTTLFactor < 2 {
		return fmt.Errorf("defaults.snapshot_ttl_factor is %d: a factor below 2 expires snapshots between broadcasts", c.Defaults.SnapshotTTLFactor)
	}
	return nil
}
