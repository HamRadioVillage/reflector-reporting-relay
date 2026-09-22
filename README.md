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

**Status:** design complete, implementation not started. See
[docs/design.md](docs/design.md).

## How it fits

```
urfd ──NNG PUB──▶ relay ──▶ Redis ──▶ dashboard / exporter / bot
```

## Requirements

- Go 1.22+ (no cgo, no libnng — [mangos](https://go.nanomsg.org/mangos) is a
  pure-Go NNG implementation)
- Redis 6+ (ACL support; Streams need only Redis 5)
- A urfd built from `w0chp/urfd` or later, with `[Dashboard] Enable = true`

## Security note, up front

urfd's NNG publisher has **no authentication and no encryption**, and the event
stream carries **client IP addresses**. Leave `NNGAddr` on `127.0.0.1` and run
the relay on the reflector host. To feed a central dashboard, run one relay per
reflector locally and point them all at a shared Redis over a VPN — do not widen
the NNG listener to `0.0.0.0`. Redis has the same caveat: loopback, unix socket,
or a private network.

## License

GPL-3.0, matching urfd.
