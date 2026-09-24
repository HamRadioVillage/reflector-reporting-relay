// Package nngsub subscribes to a urfd reflector's NNG PUB socket.
//
// The receive path does exactly one thing: read a message, hand it to a
// bounded channel, loop. urfd sends with NNG_FLAG_NONBLOCK and discards
// silently when a subscriber is slow, so nothing that can block -- JSON
// decoding, Redis I/O -- belongs on this goroutine. When the channel is full
// the relay drops and counts the drop. Losing events is acceptable; losing
// them invisibly is not.
package nngsub

import (
	"context"
	"fmt"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	"github.com/HamRadioVillage/reflector-reporting-relay/internal/stats"

	// Transports urfd's NNGAddr can name.
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

// Subscriber is one reflector's event stream.
type Subscriber struct {
	addr  string
	sock  mangos.Socket
	queue chan []byte
	stats *stats.Source
}

// Dial connects a SUB socket subscribed to everything. urfd publishes a handful
// of message types on one socket and the relay wants all of them, so filtering
// happens on type after decode, not on a prefix here.
func Dial(addr string, depth int, st *stats.Source) (*Subscriber, error) {
	sock, err := sub.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("creating SUB socket: %w", err)
	}
	if err := sock.SetOption(mangos.OptionSubscribe, []byte{}); err != nil {
		sock.Close()
		return nil, fmt.Errorf("subscribing: %w", err)
	}
	// Reconnect on its own: a reflector restart should not need a relay restart.
	if err := sock.SetOption(mangos.OptionReconnectTime, time.Second); err != nil {
		sock.Close()
		return nil, fmt.Errorf("setting reconnect time: %w", err)
	}
	if err := sock.DialOptions(addr, map[string]any{
		mangos.OptionDialAsynch: true, // start before the reflector is up
	}); err != nil {
		sock.Close()
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	return &Subscriber{addr: addr, sock: sock, queue: make(chan []byte, depth), stats: st}, nil
}

func (s *Subscriber) Addr() string { return s.addr }

// Run reads until ctx is cancelled, calling handle for each message in arrival
// order. A single handler goroutine is deliberate: ordering between a state
// snapshot and the events around it is the reason the reflector queues its own
// publishes, and parallel handlers would throw it away again.
func (s *Subscriber) Run(ctx context.Context, handle func([]byte)) error {
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		defer close(s.queue)
		for {
			msg, err := s.sock.Recv()
			if err != nil {
				if ctx.Err() != nil || err == mangos.ErrClosed {
					return
				}
				// A transient receive error should not take the relay down;
				// mangos redials underneath us.
				continue
			}
			s.stats.MarkEvent(time.Now())
			select {
			case s.queue <- msg:
			default:
				// The relay could not keep up with itself. Counted here and
				// published in the heartbeat, because a silently lossy relay
				// is the one failure a consumer cannot detect.
				s.stats.Dropped.Add(1)
			}
		}
	}()

	go func() {
		<-ctx.Done()
		s.sock.Close()
	}()

	for msg := range s.queue {
		handle(msg)
	}
	<-recvDone
	return ctx.Err()
}
