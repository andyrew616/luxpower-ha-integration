# Frame-aware read-session liveness

The standalone `LuxReadSession` uses two conservative transport-liveness
safeguards. They do not change packet routing, request matching, telemetry
freshness, or the existing Home Assistant polling transport.

## TCP keepalive

New read-session sockets request OS TCP keepalive by default. Where the
platform exposes a TCP idle option, probing begins after 60 seconds of TCP
idleness. Probe interval and probe count remain under operating-system policy;
the setting therefore does not promise failure detection within 60 seconds.

Keepalive configuration is best effort. An unsupported socket API or socket
option does not make an otherwise usable Lux connection fail. Sanitized session
metrics distinguish applied, unavailable, and failed configuration attempts.

Set `tcp_keepalive=False` to disable the request. The idle value is configurable
through `tcp_keepalive_idle_seconds`.

## Application-byte inactivity

The sole reader retires a connection generation after 900 seconds without any
application bytes by default. The deadline is measured from the latest
non-empty socket read, including across partial-frame cleanup. Set
`receive_inactivity_timeout=None` for an intentionally unlimited passive
capture.

Any bytes prove receive-path activity and reset this transport timer. They do
not automatically become telemetry. Packet integrity, target, function,
register range, and data validation remain mandatory before values or
`observed_at` can change. TCP keepalive acknowledgements are handled by the OS
and are not application bytes.

Inactivity uses the existing generation-scoped reader-failure path. It closes
the writer, fails pending work explicitly, resets the decoder, and prevents old
traffic from entering a later connection generation. No heartbeat is emitted.

Passive observation waits remain caller-bounded. A transport failure does not
manufacture an observation to wake an active observation subscription;
passive consumers that need liveness notification should use a finite wait and
inspect session connectivity/metrics.

Observation event delivery is opt-in through `subscribe_observations()`. Each
subscription is an independent, bounded, single-consumer queue. Slow consumers
drop their own oldest event without delaying the socket reader or changing the
authoritative register snapshot. Per-subscription counters and monotonically
increasing observation sequence numbers expose delivery gaps. Delivered value
mappings are immutable, and subscriptions persist across transport reconnects
until explicitly closed (preferably with the async context manager).

The legacy `async_next_observation()` and `drain_observations()` methods create
one compatibility subscription lazily. They deliberately do not replay events
accepted before their first call; those observations remain available from the
authoritative session snapshot. With no active subscriber, the session retains
no event copies and cannot report a consumer-delivery queue drop.

## Prior art

This behavior follows mature Lux implementations without copying their weaker
reply matching or cache semantics:

- `celsworth/lxp-bridge` enables TCP keepalive with a 60-second idle setting
  and defaults its receive timeout to 900 seconds.
- `jaredmauch/eg4-bridge` also enables TCP keepalive and monitors received-data
  inactivity, although its heartbeat behavior is deliberately not adopted.
- `joyfulhouse/pylxpweb` treats response timeout, EOF, and socket failure as
  reasons to tear down a suspect connection before later work.

The implementations above are MIT licensed at the revisions recorded during
the programme audit. This implementation was written independently; no
external source code was copied.

## Scope

These safeguards apply to the standalone persistent/frame-aware read path.
The existing Home Assistant integration continues to use its legacy
connection-per-poll transport and unchanged write path.
