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

**Status:** working for snapshots, history, self-reporting and several
reflectors at once. Each `state`
broadcast rewrites six Redis keys in one transaction with a TTL and rings a
doorbell; transmissions and links append to two streams that outlive the
reflector; and the relay publishes its own counters so a dead relay and a dead
reflector look different. One relay can watch several reflectors into one
keyspace. Packaging remains. See [docs/design.md](docs/design.md).

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

urfd:relay                  HASH    the relay's own counters, one per relay
urfd:URF999:updates         PUBSUB  epoch-ms, published on each snapshot commit
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

## Telling the two failure modes apart

Both a dead reflector and a dead relay end with the snapshot keys expiring, so
the relay reports on itself:

```sh
redis-cli HGETALL urfd:relay
redis-cli SUBSCRIBE urfd:URF999:updates   # or wait to be told
```

| Symptom | Diagnosis |
|---|---|
| snapshots gone, `urfd:relay` fresh, its `lastevent` frozen | the reflector is down |
| snapshots gone, `urfd:relay` gone | the relay is down |
| snapshots fine, `URF999.dropped` or `URF999.rediserrors` climbing | the relay is running and losing data |

The streams outlive both.

## Watching several reflectors

List each one under `sources`. They share a keyspace, separated by callsign, and
each gets its own streams and doorbell channel:

```yaml
sources:
  - callsign: URF999
    nng: tcp://127.0.0.1:5555
  - callsign: URF301
    nng: tcp://127.0.0.1:5556
```

Run one relay per *host* where a reflector lives, rather than one relay reaching
across the network to several — the NNG listener has no authentication and the
event stream carries client IPs, so it stays on loopback. Several relays can
write to one shared Redis over a VPN.

The `callsign` is checked against what arrives. A source pointed at the wrong
reflector logs once and writes nothing, which reads in the heartbeat as
`<CS>.received` climbing while `<CS>.snapshots` stays at zero.

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
