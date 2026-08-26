# Hours-scale read-core qualification

## Scope and safety

This qualification exercised only the standalone FC4 read path. It did not
send writes, change inverter or Home Assistant configuration, scan the LAN, or
persist private target identifiers or register values. Live artifacts remained
outside Git with mode `0600`.

The initial runs used merged revision
`091e740caa6a1c1744e8e3a69fc0f7ead91bab87`, a three-second drain deadline, a
ten-second correlated-reply deadline, a 20-second critical-profile target, TCP
keepalive, a 900-second receive-inactivity limit, and the existing bounded
recovery policy. Each run used the aligned 0–39 and 80–119 critical-profile
blocks.

## Initial soak evidence

| Run | Planned | Actual | Explicit FC4 | Accepted | Timeouts | Recovery result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | 3600 s | 265.607 s | 19 | 18 | 1 | reconnect dial failed; acquisition terminated |
| B | 3600 s | 3600.001 s | 299 | 298 | 1 | clean reconnect and profile recovery |

The selected sustained phases totalled 3865.608 seconds (1.073780 hours).
Across them, 316 accepted requests had a median latency of 693.177 ms, p95 of
757.225 ms, p99 of 783.851 ms, and maximum of 3347.286 ms. No evidence of an
invalid-frame acceptance, queue drop, Modbus rejection, false-freshness event,
or generation-isolation failure was observed. Generation fencing is guaranteed
by the reviewed control flow; the diagnostics cannot prove whether undecoded
old-generation bytes arrived after retirement.

Run A safely tainted and closed the timed-out generation, then stopped because
the policy made only one TCP reconnect dial and that dial failed. The maximum
critical-profile age reached 39.405 seconds and the final stale episode remained
open when the phase terminated. The schema-v6 artifact did not explicitly mark
that right-censoring and incorrectly left `acquisitions_abandoned` at zero.
Run A spent 19.372 sampled seconds above 20 seconds, all attributed to the
timeout/recovery episode.

Run B had one timeout on 80–119. A new connection was established 1.115 seconds
after failure detection and the full critical profile was restored after 1.834
seconds. The maximum profile age was 30.201 seconds. The completed hour spent
11.951 sampled seconds above the 20-second target, including 10.244 seconds in
the timeout/recovery episode. This is useful 20-second-class behavior, but it is
not a strict 20-second maximum-age result.

Validated unsolicited observations avoided 159 of 477 explicit profile
opportunities across the two selected phases (33.333%). The selected phases
also observed 318 unmatched validated FC4 frames and 32 diagnostic FC193
frames. FC193 remained untrusted for telemetry.

## Resource observations

The completed-hour process retained one thread and nine file descriptors from
the 60-second sample through the 3601-second sample, including across the
reconnect. No file-descriptor or thread accumulation was observed. Asyncio
reader tasks and pending futures were not sampled directly, and there was no
post-close descriptor sample, so those resource states remain unqualified.
RSS rose from approximately 29.7 MiB at 60 seconds to 54.2 MiB at 3601 seconds.
That approximately linear growth is consistent with the qualification runner's
retention of 10 Hz sample dictionaries, but the sampler does not prove sole
causality. Multi-hour tooling should use bounded or streaming sample aggregation
before it becomes a routine support tool.

Schema v6 did not record total `HEALTHY`, `RECOVERING`, and `DEGRADED`
durations, so aggregate health-state totals cannot be reconstructed for these
runs. The 10 Hz freshness samples are autocorrelated descriptive measurements,
not independent reliability trials. Application-byte inactivity retirement is
synthetically tested but did not occur naturally. OS keepalive configuration is
tested, but peer-loss behavior remains neither synthetically nor live qualified.

## Recovery hardening

The early Run A termination established a production-maturity defect in the
reconnect-dial policy, not a request-correlation or freshness defect. Mature
prior art informed the smallest change: pylxpweb uses bounded connection
attempts with backoff, while lxp-bridge and eg4-bridge retain persistent sessions
and retry connection establishment on a conservative cadence.

The focused hardening commit configures three TCP dial attempts within one
already-budgeted recovery episode, with a shutdown-aware five-second cooldown
between failed dials. The policy has a defensive hard cap of five; the reviewed
qualification configuration uses three. The rolling limit remains two recovery
episodes per five minutes. No FC4 request is retried inside the dial loop;
failed dials create no connection generation; explicit Modbus rejection remains
non-retryable.

Schema v7 also records per-dial metrics, truthful acquisition abandonment,
health-state duration, and terminal stale-episode right-censoring. Aggregate
reports preserve liveness and recovery-policy provenance.

## Post-fix live gate

Post-fix live validation used clean revision
`1e04c606a506e3f1d66060d7e2964187583b9ac3`. Two bounded attempts stopped in
forced preflight before a soak phase began:

- the first 0–39 request received one frame classified as
  `inverter_target_mismatch`, then timed out;
- after a reviewer-approved fresh-generation retry and a bounded pause, the
  same request received three `inverter_target_mismatch` frames, then timed out.

All mismatched frames were rejected. They did not complete the explicit request,
update telemetry, or advance freshness. Each timed-out generation was tainted
and closed. No recovery loop or write occurred.

Repeated wrong-target classifications are an unresolved live target-validation
blocker. The artifacts do not distinguish private target configuration, dongle
routing, concurrent-client effects, or another hardware/environment cause.
Further live work stopped rather than switching targets or probing on inference.
The intended inverter target and routing context must be confirmed before
another qualification.
The new multi-dial recovery behavior therefore remains synthetically validated
but has not yet been naturally exercised against the live dongle.

## Decision

Milestone B remains open. The normal persistent-session path completed one hour
with stable socket ownership and useful 20-second-class telemetry, and one
natural reconnect restored the profile correctly. However, the pre-fix terminal
failed dial required hardening, and repeated post-fix wrong-target traffic
prevented the necessary live proof.

No 2–4 hour run was attempted. After the target/routing issue is confirmed, the
next evidence step is two independent one-hour runs on the reviewed hardening
revision. Only if both are safe and useful should a longer bounded soak follow.

## Live artifacts

Sanitized artifacts are stored outside the repository:

- `/tmp/luxpower-soak-a.json`
- `/tmp/luxpower-soak-a-resources.txt`
- `/tmp/luxpower-soak-b.json`
- `/tmp/luxpower-soak-b-resources.txt`
- `/tmp/luxpower-soak-recovery-a.json`
- `/tmp/luxpower-soak-recovery-a-resources.txt`
- `/tmp/luxpower-soak-recovery-a-retry.json`
- `/tmp/luxpower-soak-recovery-a-retry-resources.txt`

These artifacts contain sanitized timings, ranges, classifications, and safe
revision/configuration provenance only. They are not committed.
