// Command relay subscribes to one or more urfd reflectors and mirrors their
// state into Redis.
//
// Implemented so far: the snapshot writer and the event streams. Each state
// broadcast rewrites the six snapshot keys in one transaction with a TTL;
// transmissions append to :lastheard and links append to :events. The heartbeat
// and drop-counter key are not written yet.
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
			log.Printf("source %s: stopped after %d events (%d dropped, %d snapshots, %d stream entries, %d blank modules)",
				src.Callsign, sub.Stats.Received.Load(), sub.Stats.Dropped.Load(), r.snapshots, r.appended, r.blankModules)
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
	appended       uint64
	blankModules   uint64
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
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	r.snapshots++
	if r.snapshots == 1 {
		log.Printf("source %s: first snapshot written to %s:* (ttl %s)", r.src.Callsign, snap.Base, snap.TTL)
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
			r.blankModules++
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
		log.Printf("source %s: %v", r.src.Callsign, err)
		return
	}
	r.appended++
}
