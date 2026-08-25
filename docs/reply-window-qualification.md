# FC4 reply-window qualification

The frame-aware read session historically used one three-second deadline for
both `StreamWriter.drain()` and the exactly correlated FC4 response. This is a
safe production default, but it confounds two different failure phases:

- a blocked drain is an ambiguous send and must invalidate the connection
  generation quickly;
- a completed drain may be followed by a valid but unusually late reply.

The session therefore retains the historic combined deadline by default and
offers an opt-in experimental split:

```text
write
  -> bounded drain deadline
  -> independently bounded correlated-reply deadline
  -> exact match or connection-generation taint
```

Supplying neither phase deadline preserves the original combined timeout. The
low-level session permits one phase override and inherits the other phase from
`request_timeout`; supplying either phase therefore enables split timing. The
live qualification CLI deliberately requires both values so every artifact is
fully attributable. A timeout in either phase retains the existing synchronous
generation taint, pending-request failure, socket teardown, and truthful
freshness rules.

The read-only qualification CLI exposes the split as
`--drain-timeout-seconds` and `--reply-timeout-seconds`. Both options are
required together. Artifacts record the two budgets and whether split timing
was active. Live artifacts remain outside Git and contain no target identity,
packets, or register values.

## Evidence and provenance

This behavior is an independent adaptation informed by, not copied from,
mature MIT-licensed Lux implementations:

- `celsworth/lxp-bridge` at commit
  `6d6fceed04e8ba19da977094c1f5a1198b7658bb` uses a persistent reader and a
  nominal ten-second reply wait.
- `joyfulhouse/pylxpweb` at commit
  `b1162731e28bc58c25dd295287d42a221ca9cbef` defaults to a ten-second
  operation timeout and discards suspect connections after timeouts.

Our exact inverter/count matching, single-reader ownership, unsolicited-frame
routing, connection-generation isolation, and per-register observation
freshness remain unchanged because those guarantees are stronger than the
audited reference implementations.

## Controlled live comparison

The decision experiment keeps the writer-drain deadline at three seconds and
counterbalances only the reply deadline:

```text
3-second reply -> 10-second reply -> 10-second reply -> 3-second reply
```

All arms use the same critical profile, transport, recovery policy, hardware,
Home Assistant state, freshness sampling, and implementation revision. The
analysis distinguishes replies under three seconds, three-to-five seconds,
five-to-ten seconds, and true ten-second non-response. No default changes on
the strength of a single run.

Heartbeat echo, TCP keepalive, inactivity watchdogs, Home Assistant production
integration, and standalone writes are deliberately outside this change.
