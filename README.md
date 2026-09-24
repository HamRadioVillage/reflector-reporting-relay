# reflector-reporting-relay

Subscribes to a [urfd](https://github.com/w0chp/urfd) reflector's NNG event
socket and relays reflector state and history into Redis.

urfd broadcasts JSON over NNG PUB — connect, disconnect, hearing, closing, and a
periodic full state snapshot. That is a good event bus, but PUB/SUB keeps no
history, offers nothing to a subscriber that just started, and cannot tell a
quiet reflector from a dead one. This relay sits downstream and fixes all three:
snapshot keys with a TTL, bounded event streams you can replay, and a heartbeat
of its own so "relay down" and "reflector down" look different.

The reflector needs no changes. Nothing here runs inside the process that routes
voice frames.

**Status:** snapshots and event history both work. Each `state` broadcast
rewrites six Redis keys in one transaction with a TTL, so a stopped reflector
reads as offline without anyone polling a file's mtime, and transmissions and
links append to two streams that outlive it. The heartbeat, the doorbell and the
drop-counter key are next. See [docs/design.md](docs/design.md).

## Running it

```sh
go build ./cmd/relay
cp relay.example.yaml relay.yaml   # edit the callsign and NNG address
./relay -config relay.yaml
```

Keys it writes, for callsign `URF999` and the default prefix:

```
urfd:URF999:reflector       HASH    callsign, modules, country, sponsor, url, updatedat
urfd:URF999:config          JSON    the reflector's Configure block
urfd:URF999:peers           JSON    [] when nothing is linked
urfd:URF999:clients         JSON
urfd:URF999:users           JSON    last heard, callsigns trimmed of padding
urfd:URF999:activetalkers   JSON    who is keyed up right now

urfd:URF999:lastheard       STREAM  hearing + closing, one entry per event
urfd:URF999:events          STREAM  client_connect + client_disconnect
```

The six snapshot keys expire after `snapshot_ttl_factor` × the reflector's own
broadcast interval, so their absence is the "reflector is gone" signal. The two
streams have no TTL — they are the history, and they survive a reflector that
does not. `XADD` uses `MAXLEN ~ stream_maxlen`.

Reading history back:

```sh
redis-cli XREVRANGE urfd:URF999:lastheard + - COUNT 20   # a history page
redis-cli XREAD BLOCK 0 STREAMS urfd:URF999:lastheard $  # a live feed
```

Stream ids are Redis-assigned, which also supplies ordering that urfd's
whole-second `timestamp` cannot: two events in the same second still read back in
arrival order.

## How it fits

```
urfd ──NNG PUB──▶ relay ──▶ Redis ──▶ dashboard / exporter / bot
```

## Requirements

- Go 1.24+ (no cgo, no libnng — [mangos](https://go.nanomsg.org/mangos) is a
  pure-Go NNG implementation)
- Redis 6+ (ACL support; Streams need only Redis 5)
- A urfd built from `w0chp/urfd` or later, with `[Dashboard] Enable = true`,
  **carrying the NNG event fixes** in [W0CHP/urfd#1](https://github.com/W0CHP/urfd/pull/1)
  and [#2](https://github.com/W0CHP/urfd/pull/2). The relay needs the `timestamp`
  and `reflector` fields those add; it refuses events without them rather than
  guessing, and says so once per source in the log.

## Security note, up front

urfd's NNG publisher has **no authentication and no encryption**, and the event
stream carries **client IP addresses**. Leave `NNGAddr` on `127.0.0.1` and run
the relay on the reflector host. To feed a central dashboard, run one relay per
reflector locally and point them all at a shared Redis over a VPN — do not widen
the NNG listener to `0.0.0.0`. Redis has the same caveat: loopback, unix socket,
or a private network.

## License

GPL-3.0, matching urfd.
