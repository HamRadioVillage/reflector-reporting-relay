# reflector-reporting-relay — design

**Status:** draft
**Supersedes:** the earlier "Redis-backed reporting for urfd" proposal, which
proposed building a Redis exporter *into* the reflector. That is now the wrong
layer — see §3.

---

## Part A — The relay

### 1. Where things stand

`HamRadioVillage/urfd` is fast-forwarded to `w0chp/urfd` @ `cee46d1`, 97 commits
past `nostar/urfd`. That brings in every PR nostar left unmerged since December
2025, including **#20, an NNG JSON publisher in the reflector core**.

urfd now opens an NNG **PUB** socket and broadcasts JSON. Configured under a new
`[Dashboard]` section:

```ini
[Dashboard]
Enable   = true
NNGAddr  = tcp://127.0.0.1:5555
Interval = 10
NNGDebug = false
```

Five message types, all with a `type` discriminator:

| `type` | Emitted from | Payload fields |
|---|---|---|
| `client_connect` | `CClients::AddClient()` — Clients.cpp:71 | `callsign`, `ip`, `protocol`, `module` |
| `client_disconnect` | `CClients::RemoveClient()` — Clients.cpp:100 | `callsign`, `ip`, `protocol`, `module` |
| `hearing` | `CUsers::Hearing()` — Users.cpp:77 | `callsign`, `repeater`, `rpt2`, `via_peer`, `module`, `protocol` |
| `closing` | `CUsers::Closing()` — Users.cpp:90 | `callsign`, `module`, `protocol`, `recording` (optional) |
| `state` | `MaintenanceThread()` — Reflector.cpp:405, every `Interval` s | full `JsonReport()`: `Configure`, `Peers`, `Clients`, `Users`, `ActiveTalkers` |

Every message also carries `type`, `timestamp` and `reflector` — the publishing
reflector's own callsign, named `reflector` because `callsign` already means the
station an event is *about*. All of this is confirmed on the wire against a
reflector carrying the fixes; see §4.4.

Sends use `nng_send(..., NNG_FLAG_NONBLOCK)` — fire-and-forget, dropped under
backpressure. `ActiveTalkers` (Reflector.cpp:528) is new and genuinely useful:
live per-module TX state the XML export never carried.

### 2. What the publisher gives us, and what it does not

**Gives us:** off-box delivery, many simultaneous subscribers, push instead of
poll, structured JSON instead of the pseudo-XML that needed a bespoke PHP
substring scanner, and sub-second event latency. That is most of what the
original Redis proposal was chasing.

**Does not give us**, by the nature of PUB/SUB:

* **History.** A subscriber that is not connected when an event fires misses it
  permanently. There is no replay.
* **State on connect.** A subscriber that starts up must wait up to `Interval`
  seconds before it knows anything at all.
* **Liveness.** A silent socket and a dead reflector look identical.
* **Aggregation.** Watching N reflectors means managing N sockets and merging
  by hand.
* **Queryability.** "What did W0CHP work in the last hour" is not answerable.

### 3. Why a separate process

The original proposal put a Redis writer in the reflector. With the NNG
publisher already merged, that would mean a second in-core exporter: another
link-time dependency, a second set of emission call sites in the same hot paths,
and the same under-lock publishing problem doubled.

The publisher already does the genuinely hard part — low-overhead, non-blocking
event emission from inside the reflector. Everything Redis adds is
*downstream* of that, and none of it needs to live in a C++ process that routes
voice frames. `docs/nng.md` in the urfd tree already diagrams a "Middle Tier"
sitting between the publisher and the dashboard. This is that tier.

So: **urfd stays the event bus; the relay is the state store and historian.**
One Go binary, no reflector changes required, no `-Werror` risk, and it can be
developed, restarted and rolled back without touching a running reflector.

### 4. Design

#### 4.1 Shape

A single static binary, `relay`. It dials one or more urfd publishers as an NNG
**SUB**, validates and trims what arrives, and writes Redis. It is a client of both ends
and a server to nothing.

```
urfd ──NNG PUB──▶ relay ──▶ Redis ──▶ dashboard / exporter / bot
                    │
                    └─ heartbeat key, so "relay down" ≠ "reflector down"
```

#### 4.2 The one hard constraint

**The NNG socket must never block.** urfd sends with `NNG_FLAG_NONBLOCK` and
drops on backpressure — silently, with no error the reflector surfaces. If the
relay is slow to read, urfd discards events and nobody learns that data was
lost.

So the receive path does exactly one thing: read a message, push it onto a
buffered channel, loop. All JSON decoding and all Redis I/O happens on separate
goroutines. If the channel fills — Redis is down, or slow — the relay drops
*and counts* the drop, exposing the count in its heartbeat key. Losing events
is acceptable; losing them invisibly is not.

#### 4.3 Redis data model

Configurable prefix, default `urfd`. With callsign `URF123` the base is
`urfd:URF123`.

**Snapshot keys** — rewritten from each `state` message, `TTL = 3 × Interval`:

| Key | Type | Source |
|---|---|---|
| `urfd:URF123:reflector` | HASH | callsign, modules, transcoded modules, country, sponsor, url, `updatedat` |
| `urfd:URF123:config` | STRING (JSON) | the `Configure` block |
| `urfd:URF123:peers` | STRING (JSON array) | `Peers` |
| `urfd:URF123:clients` | STRING (JSON array) | `Clients` |
| `urfd:URF123:users` | STRING (JSON array) | `Users` |
| `urfd:URF123:activetalkers` | STRING (JSON array) | `ActiveTalkers` |

Written in one `MULTI`/`EXEC` so a reader never sees a half-updated snapshot.
TTL is the liveness signal: if urfd stops, the keys expire and every consumer
sees "offline" without inspecting a file's mtime.

**Streams** — `XADD` with `MAXLEN ~`, length configurable:

| Key | Fed by | Fields |
|---|---|---|
| `urfd:URF123:lastheard` | `hearing`, `closing` | `callsign`, `repeater`, `rpt2`, `via_peer`, `module`, `protocol`, `recording`, `ts` |
| `urfd:URF123:events` | `client_connect`, `client_disconnect` | `event`, `callsign`, `ip`, `protocol`, `module`, `ts` |

This is the payoff. urfd's in-memory last-heard list is capped at 20 entries
(`LASTHEARD_USERS_MAX_SIZE`, Users.cpp); the stream keeps as many as configured
and supports `XREVRANGE` for a history page and `XREAD BLOCK` for a live feed
that can resume after a disconnect.

**Relay heartbeat** — `urfd:relay` HASH: `started`, `lastevent`, per-source
message counts, drop counts, Redis error counts. Without this, a dead relay and
a dead reflector are indistinguishable to a dashboard.

**Doorbell** — `PUBLISH urfd:URF123:updates <epoch-ms>` after each snapshot
commit, for consumers that would rather be told than poll.

#### 4.4 Ingest: trim, attribute, validate

The relay targets the **corrected** event shape and carries no compatibility
path for the stock publisher. Every event arrives with `type`, `timestamp` and
`reflector`, and the `hearing` field names hold what they say, so nothing is
remapped. An event without that envelope is refused with `event.ErrNoEnvelope`
and logged once per source: repairing it would mean supplying identity from
config and stamping receipt time, and both were dropped on purpose.

What remains is mechanical:

| Wire field | Holds | Relay behaviour |
|---|---|---|
| `callsign` | the transmitting user | trim padding |
| `repeater` | `rpt1` — for M17, the linked client (M17Protocol.cpp:379) | trim padding |
| `rpt2` | `rpt2` | trim, pass through |
| `via_peer` | `xlx` — a peer callsign, or this reflector itself | trim; omit when it equals `reflector` |
| `module` | the module letter | may be blank; see below |
| `reflector` | the publishing reflector's callsign | trim; this is the attribution key, so no per-source config is needed |
| `timestamp` | emission time, whole seconds | the event time; a stream id is the tiebreak |

**Callsigns are space-padded to eight characters** — `"N0CALL  "`, or nine with
a module, `"URF999  M"`. Untrimmed, nothing downstream matches, so the relay
trims every callsign it writes, including the strings inside the `Peers`,
`Clients`, `Users` and `ActiveTalkers` arrays of a `state` snapshot.

**A blank module is an unknown module.** `hearing`'s `module` comes from
`xlx.GetCSModule()`, and four call sites — G3:573, DMRPlus:211, IMRS:156,
USRP:227 — pass the bare reflector callsign, whose module stays `' '`. Those
events carry `"module": " "`, which trims to empty rather than becoming a module
named space. Everything else in the reflector gets this right: `state`'s
`OnModule` and the XML `<On module>` both read `m_Rpt2`, and `closing` reads
`GetStreamModule()` — so the fallback for a blank module is the next `state`
snapshot. Fixed upstream in W0CHP/urfd#2; the guard stays for reflectors that
predate it.

**No version.** `JsonReport()` publishes no reflector version — it appears only
in urfd's startup log — so the `:reflector` hash cannot carry one and does not
invent it.

#### 4.5 Configuration

```yaml
redis:
  addr: 127.0.0.1:6379
  db: 0
  username: ""          # Redis 6+ ACL
  password: ""
  key_prefix: urfd

defaults:
  stream_maxlen: 5000
  snapshot_ttl_factor: 3

sources:
  - callsign: URF123
    nng: tcp://127.0.0.1:5555
```

Multiple `sources` is how aggregation works: one relay can watch several
reflectors and write them all into one keyspace under distinct callsigns.

#### 4.6 Deployment and security

**Default and recommended:** relay on the same host as urfd, NNG on loopback,
Redis on loopback. Nothing listens on a routable address.

**For a central dashboard:** run one relay *per reflector, on the reflector's
own host*, each writing to a shared Redis over WireGuard or a private network.

**Do not** simply change `NNGAddr` to `tcp://0.0.0.0:5555` to let a remote relay
dial in. NNG PUB as urfd uses it has **no authentication and no encryption**,
and the event stream includes **client IP addresses** (`client_connect` and
`client_disconnect` both carry `ip`). Exposing that port publishes your users'
IPs to anyone who can reach it. The shipped default of `127.0.0.1` is correct;
the temptation to widen it is the thing to warn sysops about in the README.

Redis carries the same caveat — no transport encryption by default. Bind it to
loopback or a unix socket, and if it must cross a network, put it on a VPN.

#### 4.7 Go dependencies

| Package | Why |
|---|---|
| `go.nanomsg.org/mangos/v3` (+ `protocol/sub`) | **Pure Go** NNG implementation, wire-compatible with libnng. No cgo, no libnng on the host, genuinely static binary. |
| `github.com/redis/go-redis/v9` | Maintained client, pipelining and `MULTI`/`EXEC`, context-aware. |
| `gopkg.in/yaml.v3` | Config. |

Standard library for JSON, logging and signals. That is the whole dependency
list, and it is deliberately short.

### 5. Phasing

1. ~~**Tap.**~~ **Done.** A raw SP/TCP subscriber, 66 messages captured off a
   live reflector. It retired the remapping layer, the config-supplied callsign
   and the receipt-time stamp, and found the callsign padding.
2. ~~**Snapshot writer.**~~ **Done.** `state` → the six snapshot keys,
   `MULTI`/`EXEC`, TTL = 3 × the reflector's own `Interval`. Verified against a
   live reflector and a real Redis: keys land, keys expire when the reflector
   stops, and the relay reconnects on its own when it returns.
3. **Streams + trim.** `hearing` / `closing` / connect / disconnect into the
   `:lastheard` and `:events` streams, with the ingest rules from §4.4.
4. **Heartbeat, doorbell, drop counters.**
5. **Multi-source.**
6. **Packaging.** systemd unit, a `.deb` or a release binary, README with the
   security guidance from §4.6 stated plainly.

Step 1 is the important one: it validates every assumption in this document
against a running reflector before any Redis code exists.

---

## Part B — Changes worth sending to w0chp

W0CHP merges things; nostar has not since December 2025. These are ordered by
value. All line numbers are from `cee46d1`.

| # | Change | Why it matters |
|---|---|---|
| 1 | ~~**Stop publishing under reflector mutexes.**~~ **Sent: W0CHP/urfd#1 (`ed79fc9`).** `Publish()` is called from `CUsers::Hearing()` (Users.cpp:77), `CUsers::Closing()` (Users.cpp:90) and `CClients::AddClient`/`RemoveClient` (Clients.cpp:71, 100) — all with the users or clients mutex held, on protocol threads. `Publish()` does `event.dump()` (JSON serialization plus allocation) and takes its own mutex before the non-blocking send. `NONBLOCK` bounds it, so this is contention rather than deadlock, but it is per-transmission serialization on a hot path under a lock every protocol thread needs. Hand off to a bounded queue drained by the maintenance thread; `CSafePacketQueue` is the existing pattern. |
| 2 | ~~**Add `timestamp` and `callsign` to every event.**~~ **Sent: W0CHP/urfd#1 (`683d305`).** Landed as `timestamp` plus `reflector` — not `callsign`, which already means the station an event is about. Follow-up: whole-second precision cannot order two events in the same second. |
| 3 | ~~**Fix the `hearing` field names.**~~ **Sent: W0CHP/urfd#1 (`d0c9477`).** Users.cpp:70–76 wrote `event["ur"] = rpt1`, `event["rpt1"] = rpt2`, `event["rpt2"] = xlx` — each label holding the *next* field's value. Now `callsign`, `repeater`, `rpt2`, `via_peer`. Breaking change for any existing subscriber. |
| 4 | **The `m_Xlx` inconsistency.** PR #20 rewrote the `Hearing()` call sites unevenly. Six protocols — DCS:215, DExtra:358, DPlus:220, NXDN:242, P25:237, YSF:300 — now pass `rpt2` as the `xlx` argument, where they previously used the 3-arg overload that sets `xlx = g_Reflector.GetCallsign()`. Three — DMRPlus:211, G3:573, USRP:227 — were converted to the 4-arg form and keep the old behaviour. `m_Xlx` is what `CUser::WriteXml()` renders as `<Via peer>` and `JsonReport()` as `ViaPeer`, so the existing XML dashboard now shows different things per protocol. In DExtra at least, `rpt2` originates as `Header->GetRpt2Callsign()` — inbound client data with only the module letter overwritten. **Ask before patching**: this may be deliberate, and it needs dbehnke's or W0CHP's intent. |
| 5 | **`-lnng -lopus -logg` are unconditional** (Makefile:35). Unlike `DHT`, there is no `urfd.mk` toggle, so all three are hard build dependencies even for someone who never enables `[Dashboard]` or `[Audio]`. Mirror the `DHT` pattern. |
| 6 | **The transcoder-accept guard** (Reflector.cpp:348). `if (xmlpath.empty() && jsonpath.empty() && !dashboard.enable) return;` exits `MaintenanceThread()`, which is also the only caller of `g_TCServer.Accept()` for dropped transcoder connections. Unreachable today because `XmlPath` is fatal-if-missing, but it is a trap waiting for whoever makes XML optional. Always run the loop; skip only the exporters. |
| 7 | **`JsonReport()` omits the client IP** that `WriteXml()` includes and `dashboard/pgs/repeaters.php` displays with its `HideIP` masking options. Any JSON-or-NNG-based dashboard silently loses the column. |
| 8 | Minor: `test_audio.cpp` is filtered out of `SRCS` (Makefile:43) but has no build rule, unlike `test_dmr`. Orphaned. |
| 9 | ~~**`hearing.module` is blank for four protocols.**~~ **Sent: W0CHP/urfd#2 (`9541235`).** The module came from `xlx.GetCSModule()`, but G3:573, DMRPlus:211, IMRS:156 and USRP:227 pass the bare reflector callsign, whose `m_Module` stays `' '`. One line reads it from `rpt2` instead, as `CUser::JsonReport()` already does. Found while writing this relay's ingest guard. |

Items 1–3 and 5–8 are mechanical. Item 4 is a question first.

---

## Part C — Build environment

Target is the **kali-linux** WSL2 instance (rolling, gcc 14.2.0), not the
Ubuntu 20.04 one — that ships OpenDHT 1.8.1, which urfd's README explicitly says
not to use.

The fast-forward added three link-time dependencies beyond the original set:

```bash
sudo apt update && sudo apt install -y \
  build-essential nlohmann-json3-dev libcurl4-gnutls-dev libopendht-dev \
  libnng-dev libopus-dev libogg-dev \
  redis-server golang
```

All verified available on kali rolling: `libnng-dev` 1.12.3, `libopus-dev`
1.6.1, `libogg-dev` 1.3.6, `libopendht-dev` 3.0.1, `libhiredis` not needed any
more.

```bash
cd ~/urfd/reflector && cp ../config/urfd.mk . && make -j$(nproc)
```

Three things to know about this environment:

* **GCC 14 against `-W -Werror` is untested.** urfd targets Debian 12 / Ubuntu 24
  (GCC 12/13). GCC 14 added warnings those do not emit. If the build breaks,
  `make CFLAGS="-W -std=c++17 -MMD"` drops `-Werror` to get moving, and the
  warnings become a worthwhile PR to w0chp in their own right.
* **No systemd** in that WSL instance — PID 1 is `init`. `make install` will
  fail; run `./urfd ./urfd.ini` directly, and start Redis with
  `sudo service redis-server start`.
* **Build in `~/`, not `/mnt/c`.** The 9p filesystem is slow and OneDrive will
  fight the compiler over `.o` and `.d` files.

For the relay itself, only Go is needed — mangos is pure Go, so the relay builds
and runs without libnng even on a host that has no reflector on it.
