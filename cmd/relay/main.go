// Command relay subscribes to one or more urfd reflectors and mirrors their
// state into Redis.
//
// Implemented so far: the snapshot writer. Each state broadcast rewrites the
// six snapshot keys in one transaction with a TTL. Event streams, the
// heartbeat and the drop-counter key are not written yet; events other than
// state are counted and discarded.
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

	var wg sync.WaitGroup
	for _, src := range cfg.Sources {
		sub, err := nngsub.Dial(src.NNG, cfg.Defaults.QueueDepth)
		if err != nil {
			log.Fatalf("source %s: %v", src.Callsign, err)
		}
		log.Printf("source %s: subscribed to %s (queue %d)", src.Callsign, src.NNG, cfg.Defaults.QueueDepth)
		wg.Add(1)
		go func(src config.Source, sub *nngsub.Subscriber) {
			defer wg.Done()
			r := &relay{cfg: cfg, src: src, sub: sub, store: st}
			if err := sub.Run(ctx, r.handle); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("source %s: %v", src.Callsign, err)
			}
			log.Printf("source %s: stopped after %d events (%d dropped, %d snapshots written)",
				src.Callsign, sub.Stats.Received.Load(), sub.Stats.Dropped.Load(), r.snapshots)
		}(src, sub)
	}

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
	sub   *nngsub.Subscriber
	store *store.Store

	snapshots      uint64
	warnedShape    bool
	warnedMismatch map[string]bool
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

	if env.Type != event.TypeState {
		// Streams are the next phase; until then these are counted by the
		// subscriber and dropped here.
		return
	}

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
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	r.snapshots++
	if r.snapshots == 1 {
		log.Printf("source %s: first snapshot written to %s:* (ttl %s)", r.src.Callsign, snap.Base, snap.TTL)
	}
}
