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

## Confirmed target and current hours-scale evidence

The operator subsequently supplied the protocol-evidenced inverter target for
the tested dongle. The live runner verified that target with exact FC4 replies
for both profile blocks; it did not probe or switch to another target.

Schema-v8 qualification used clean revision
`67563526a59c30bd72c28d55852c302d303dcb46` with the same three-second drain,
ten-second reply, keepalive, inactivity, profile, and multi-dial recovery
configuration throughout its first four hours:

| Run | Actual | Explicit / matched | Timeouts / reconnects | Median / p95 / p99 / max reply | Worst-age median / p95 / p99 / max | Time above 20 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 3600.001 s | 269 / 269 | 0 / 0 | 685 / 751 / 797 / 828 ms | 12.125 / 18.293 / 18.868 / 19.164 s | 0 s |
| B | 3600.001 s | 280 / 278 | 2 / 2 | 687 / 756 / 969 / 1268 ms | 11.507 / 18.518 / 19.056 / 30.198 s | 20.492 s |
| Long | 7200.002 s | 600 / 599 | 1 / 1 | 702 / 765 / 922 / 7332 ms | 11.761 / 18.557 / 19.020 / 30.169 s | 27.839 s |

All three timeouts conservatively retired their connection generations and
restored the complete profile on the first reconnect dial, in approximately
1.8 seconds after failure detection. There was no invalid-frame acceptance,
connection loss, Modbus rejection, retry-budget exhaustion, or false freshness.
The long run also accepted four genuine replies between five and ten seconds,
confirming that the independent ten-second correlated-reply window prevents
unnecessary retirement without treating a genuine miss as success.

The long run exposed 228 drops in an unconditional 1,024-event observation
queue. Authoritative cache/freshness updates and pending-request completion
happen before event publication, so those drops did not corrupt the measured
telemetry; they did show that an unconsumed support stream was not a valid
production boundary. Observation delivery was changed to opt-in, independent,
bounded subscriptions with immutable payloads, sequence/gap counters, and
explicit teardown. Subscriber delivery completeness is now reported separately
from transport/recovery safety.

The corrected revision
`865c34e2dd6f93807fd35711d480d757d5694a5f` passed an exact two-block preflight,
a 120.628-second smoke, and a further 7200.001-second soak:

- 600/600 sustained explicit FC4 replies and 632 validated unmatched FC4;
- one connection generation, no timeout, reconnect, connection loss, invalid
  frame, Modbus rejection, queue drop, or inactivity retirement;
- median/p95/p99/max accepted reply latency of
  699.075/758.001/791.377/987.103 ms;
- 71,363 complete freshness samples, with estimated median/p95/p99 ages of
  11.932/18.510/18.968 seconds and an exact maximum of 19.861 seconds;
- zero samples and zero sampled time above 20 seconds;
- 7199.196 sampled seconds healthy, 0.804 seconds degraded during startup, and
  no recovery interval;
- 300 of 900 profile read opportunities (33.333%) satisfied by recent
  validated unsolicited observations.

The corrected soak processed 1,234 validated FC4 observations, exceeding the
former 1,024-event boundary, while retaining no events and reporting no delivery
drops because qualification has no event subscriber.

## Current resource observations

External sampling during the corrected two-hour soak consistently observed one
thread and nine file descriptors. Post-interpreter RSS rose from about 30.1 MiB
while the fixed 16,384-sample reservoir and bounded diagnostic histories filled,
then changed gradually to about 32.7 MiB at the end. There was no queue-sized
step, descriptor/thread accumulation, or sustained unbounded pattern. Decoder
buffered bytes were zero at shutdown. Asyncio task/future counts were not
sampled from the live process; deterministic shutdown, request, reconnect, and
cancellation tests cover those ownership invariants.

## Decision

Milestone B is complete. Across six selected hours, the core issued 1,749
sustained explicit requests and accepted 1,746. The three genuine misses were
truthfully stale and safely recovered; the corrected final two-hour run was
clean. Historical stale time above the 20-second target was 48.331 sampled
seconds, approximately 0.224% of the six-hour observation window, and was never
hidden by refreshed timestamps. This is evidence for useful, truthful
20-second-class telemetry, not a promise that every future value will remain
below exactly 20 seconds. The six hours span two reviewed revisions around the
observation-delivery fix; they are supporting lifecycle evidence, not one
homogeneous six-hour statistical trial.

No further transport experiment is required before production-core
consolidation. Application inactivity retirement and peer-loss keepalive remain
synthetically rather than naturally exercised, but neither is a blocker to the
observed safe lifecycle. The next work should classify and consolidate
experimental tooling, freeze the supported standalone read/health API, and
audit read-only BMS telemetry before assessing Smart Energy readiness.

## Bounded hours-scale freshness evidence

Schema v8 replaces the qualification runner's phase-wide list of nominal 10 Hz
sample dictionaries with an append-only streaming accumulator. Acquisition and
sampling cadence are unchanged. Exact counters retain sample completeness,
strict threshold violations, maximum age, real sampled stale duration, health
duration, and bounded causal violation episodes. No raw freshness samples are
written to the report.

Median, p95, and p99 are exact while the phase has at most 16,384 complete
samples. Longer phases use a deterministic 16,384-value Algorithm-R reservoir
and label the resulting nearest-rank quantiles as estimates. The report records
the method, capacity, samples seen and retained, seed, and whether a phase's
quantiles remained exact. This makes memory independent of soak duration without
presenting approximate percentiles as exact measurements.

Algorithm R follows Jeffrey Vitter's published reservoir-sampling method. The
implementation was written independently; no external source code was copied.
The standard-library `asyncio.Queue` nonblocking bounded-queue behavior is used
for opt-in observation delivery.

Sampled durations use run-local monotonic time. UTC timestamps remain for
human-readable episode timestamps and continuous overlap with the recovery
event timeline; they do not determine total stale or health duration. This
prevents wall-clock/NTP adjustments from creating false stale or health totals,
while leaving causal overlap subject to the recorded UTC event boundaries.

Violation-episode evidence is capped at 4,096 episodes. The exact count, total
sampled stale duration, and longest duration continue to advance after the cap,
but a report explicitly becomes evidence-incomplete if episode details are
truncated; such a phase cannot pass qualification. Causal recovery-versus-normal
duration fields become unavailable rather than silently assigning dropped
episodes to normal operation. An explicit `ended_stale` scalar preserves
terminal right-censoring even when the detailed terminal episode cannot be
retained. Recovery attribution in v8 uses continuous overlap between retained
stale episodes and recorded recovery episodes, avoiding the previous one-sample
boundary classification bias.

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
- `/tmp/luxpower-milestone-b-run-a.json`
- `/tmp/luxpower-milestone-b-run-b.json`
- `/tmp/luxpower-milestone-b-long.json`
- `/tmp/luxpower-milestone-b-ab-aggregate.json`
- `/tmp/luxpower-milestone-b-preflight-reviewed.json`
- `/tmp/luxpower-milestone-b-subscription-smoke.json`
- `/tmp/luxpower-milestone-b-subscription-rerun.json`
- `/tmp/luxpower-milestone-b-subscription-rerun.resources`

These artifacts contain sanitized timings, ranges, classifications, and safe
revision/configuration provenance only. They are not committed.
