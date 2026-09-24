// Command relay subscribes to one or more urfd reflectors and mirrors their
// state into Redis.
//
// Each state broadcast rewrites the six snapshot keys in one transaction with a
// TTL and rings a doorbell on <base>:updates; transmissions append to
// :lastheard and links append to :events; and the relay publishes its own
// counters to <prefix>:relay so that a dead relay and a dead reflector do not
// look alike.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/config"
	"github.com/HamRadioVillage/reflector-reporting-relay/internal/event"
	"github.com/HamRadioVillage/reflector-reporting-relay/internal/nngsub"
	"github.com/HamRadioVillage/reflector-reporting-relay/internal/stats"
	"github.com/HamRadioVillage/reflector-reporting-relay/internal/store"
)

func main() {
	cfgPath := flag.String("config", "relay.yaml", "path to the relay config file")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		DB:       cfg.Redis.DB,
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
	})
	defer rdb.Close()

	st := store.New(rdb)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = st.Ping(pingCtx)
	cancel()
	if err != nil {
		log.Fatalf("redis at %s: %v", cfg.Redis.Addr, err)
	}
	log.Printf("redis %s db %d, key prefix %q", cfg.Redis.Addr, cfg.Redis.DB, cfg.Redis.KeyPrefix)

	started := time.Now()
	counters := make([]*stats.Source, 0, len(cfg.Sources))

	var wg sync.WaitGroup
	for _, src := range cfg.Sources {
		counter := stats.New(src.Callsign)
		counters = append(counters, counter)
		sub, err := nngsub.Dial(src.NNG, cfg.Defaults.QueueDepth, counter)
		if err != nil {
			log.Fatalf("source %s: %v", src.Callsign, err)
		}
		log.Printf("source %s: subscribed to %s (queue %d)", src.Callsign, src.NNG, cfg.Defaults.QueueDepth)
		wg.Add(1)
		go func(src config.Source, sub *nngsub.Subscriber, counter *stats.Source) {
			defer wg.Done()
			r := &relay{cfg: cfg, src: src, store: st, stats: counter}
			if err := sub.Run(ctx, r.handle); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("source %s: %v", src.Callsign, err)
			}
			log.Printf("source %s: stopped after %d events (%d dropped, %d snapshots, %d stream entries, %d blank modules, %d redis errors)",
				src.Callsign, counter.Received.Load(), counter.Dropped.Load(), counter.Snapshots.Load(),
				counter.Entries.Load(), counter.BlankModules.Load(), counter.RedisErrors.Load())
		}(src, sub, counter)
	}

	// The heartbeat runs on its own clock, not on event arrival: a relay
	// watching a silent reflector still has to prove it is alive.
	wg.Add(1)
	go func() {
		defer wg.Done()
		beat := func() {
			hb := store.BuildHeartbeat(cfg.Redis.KeyPrefix, started, time.Now(), counters, cfg.Defaults.HeartbeatInterval.Duration)
			writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := st.WriteHeartbeat(writeCtx, hb); err != nil {
				log.Printf("heartbeat: %v", err)
			}
		}
		beat() // once at startup, so the key exists before the first interval
		t := time.NewTicker(cfg.Defaults.HeartbeatInterval.Duration)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				beat()
			}
		}
	}()
	log.Printf("heartbeat %s:relay every %s (expires after %s)", cfg.Redis.KeyPrefix,
		cfg.Defaults.HeartbeatInterval.Duration, 3*cfg.Defaults.HeartbeatInterval.Duration)

	<-ctx.Done()
	log.Print("shutting down")
	wg.Wait()
	os.Exit(0)
}

// relay handles one source's messages. Run calls handle on a single goroutine,
// so these fields need no locking.
type relay struct {
	cfg   *config.Config
	src   config.Source
	store *store.Store
	stats *stats.Source

	warnedShape    bool
	warnedMismatch map[string]bool
	warnedType     map[string]bool
}

func (r *relay) handle(msg []byte) {
	env, err := event.Decode(msg)
	if err != nil {
		if errors.Is(err, event.ErrNoEnvelope) {
			// Say this once per source, not once per event: a reflector
			// without the fixes will produce one of these every interval.
			if !r.warnedShape {
				r.warnedShape = true
				log.Printf("source %s: %v -- ignoring its events (see docs/design.md)", r.src.Callsign, err)
			}
			return
		}
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}

	if env.Reflector != r.src.Callsign {
		// The event's own callsign is the source of truth, but a socket
		// delivering someone else's events means the config is wrong, and
		// merging them into the configured keyspace would hide that.
		if r.warnedMismatch == nil {
			r.warnedMismatch = make(map[string]bool)
		}
		if !r.warnedMismatch[env.Reflector] {
			r.warnedMismatch[env.Reflector] = true
			log.Printf("source %s (%s): events are stamped %q -- check the config; skipping them",
				r.src.Callsign, r.src.NNG, env.Reflector)
		}
		return
	}

	switch env.Type {
	case event.TypeState:
		r.writeSnapshot(msg)
	case event.TypeHearing, event.TypeClosing, event.TypeClientConnect, event.TypeClientDisconnect:
		r.appendEntry(env, msg)
	default:
		// A message type this relay predates. Counted by the subscriber and
		// otherwise ignored, rather than guessed at.
		if r.warnedType == nil {
			r.warnedType = make(map[string]bool)
		}
		if !r.warnedType[env.Type] {
			r.warnedType[env.Type] = true
			log.Printf("source %s: ignoring unknown event type %q", r.src.Callsign, env.Type)
		}
	}
}

func (r *relay) writeSnapshot(msg []byte) {
	state, err := event.DecodeState(msg)
	if err != nil {
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	snap, err := store.BuildSnapshot(state, r.cfg.Redis.KeyPrefix, r.cfg.Defaults.SnapshotTTLFactor)
	if err != nil {
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.store.Apply(ctx, snap); err != nil {
		r.stats.RedisErrors.Add(1)
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	if r.stats.Snapshots.Add(1) == 1 {
		log.Printf("source %s: first snapshot written to %s:* (ttl %s), doorbell on %s%s",
			r.src.Callsign, snap.Base, snap.TTL, snap.Base, store.UpdatesChannel)
	}
}

func (r *relay) appendEntry(env event.Envelope, msg []byte) {
	var entry *store.Entry
	switch env.Type {
	case event.TypeHearing:
		h, err := event.DecodeHearing(msg)
		if err != nil {
			log.Printf("source %s: %v", r.src.Callsign, err)
			return
		}
		if h.Module == "" {
			// Neither the module field nor rpt2 carried one. Left empty rather
			// than guessed; a consumer falls back to the state snapshot.
			r.stats.BlankModules.Add(1)
		}
		entry = store.HearingEntry(h, r.cfg.Redis.KeyPrefix)
	case event.TypeClosing:
		c, err := event.DecodeClosing(msg)
		if err != nil {
			log.Printf("source %s: %v", r.src.Callsign, err)
			return
		}
		entry = store.ClosingEntry(c, r.cfg.Redis.KeyPrefix)
	default: // client_connect, client_disconnect
		c, err := event.DecodeClient(msg)
		if err != nil {
			log.Printf("source %s: %v", r.src.Callsign, err)
			return
		}
		entry = store.ClientEntry(c, r.cfg.Redis.KeyPrefix)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.store.Append(ctx, entry, r.cfg.Defaults.StreamMaxLen); err != nil {
		r.stats.RedisErrors.Add(1)
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	r.stats.Entries.Add(1)
}
