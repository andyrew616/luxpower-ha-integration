# Ten-second critical-profile qualification

## Decision

Strict ten-second freshness did **not** qualify on the measured inverter. The
frame-aware session and bounded recovery remained safe, but both independent
30-minute sustained samples contained truthful freshness violations. Production
Home Assistant integration remains deferred.

This report contains no host, dongle serial, inverter serial, raw packet, or
register value. Live traffic was limited to function-code-4 input reads. The
private runtime environment was deleted after the final run, and detailed live
artifacts remain untracked under `/tmp` with mode `0600`.

## Method

The existing 12K single-phase energy-flow profile was exercised through its
hardware-proven aligned blocks, 0-39 and 80-119. The reviewed Stage 6 policy was
unchanged: a three-second request timeout, one reconnect per acquisition, two
reconnects in a rolling five-minute interval, a one-second initial cooldown, and
a five-second repeated-failure cooldown.

Five forced refreshes preceded each phase to calculate a freshness-driven request
trigger. No full input scan, holding-register read, BMS read, production Home
Assistant poll, or write was part of the qualification.

Evidence collected on 2026-08-25 comprised one 60-second sanity run and two
independent 30-minute sustained runs. A planned 60-minute run was not performed:
both sustained samples had already failed the strict target, so another hour of
live traffic was not justified.

## Results

| Run | Duration | Explicit requests | Timeouts / reconnects | Median age | P95 age | P99 age | Maximum age | Time over 10s | Strict result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Sanity | 60.000s | 10 | 0 / 0 | 5.444s | 8.784s | 9.037s | 9.162s | 0.000s | Pass |
| Sustained 1 | 1800.001s | 373 | 4 / 4 | 6.298s | 8.909s | 10.671s | 17.199s | 20.595s | Fail |
| Sustained 2 | 1800.340s | 374 | 3 / 3 | 6.190s | 8.886s | 9.357s | 13.663s | 12.254s | Fail |

The sustained aggregate was 3600.341 seconds, 747 explicit requests, 740 expected
responses, seven timeouts, and seven successful reconnects. The observed rates
were 0.9371% timeouts per explicit request and approximately 6.999 timeouts and
reconnects per hour. These are qualification-sample rates, not guaranteed
long-term rates.

Across 35,739 sustained freshness samples, 327 samples (0.9150%) exceeded ten
seconds for 32.849 sampled seconds (0.9124% of runtime). The longest sampled
continuous violation episode was 7.231 seconds. Run 2 also had two short non-recovery
violations, with maxima of 11.047 and 10.978 seconds. No strict result is softened
because the proportion of stale time was small.

Each run computed its freshness distribution from its complete in-memory sample
series and persisted the resulting statistics. The table reports those per-run
statistics directly; no statistically invalid combined median or percentile was
manufactured from the persisted summaries. The conservative cross-run p99
envelope was 10.671 seconds and the observed absolute maximum was 17.199 seconds.

## Recovery evidence

All seven natural request timeouts tainted and closed their old connection
generation before reconnect. All seven created a clean generation and restored
the complete required profile. There were no failed reconnects, exhausted retry
budgets, acquisitions abandoned, connection losses, invalid frames, Modbus
rejections, queue drops, or accepted cross-generation responses.

Post-teardown recovery-handler entry to new connection ranged from 1.007 to 5.092
seconds (median 1.101 seconds). Handler entry to complete profile recovery ranged
from 1.713 to 6.271 seconds (median 1.816 seconds). The live instrumentation did
not separately timestamp initial failure detection and old-generation closure;
close-before-reconnect is verified by control flow, not by two causal timestamps.
Repeated failures in the rolling window correctly received the five-second
cooldown; those safe cooldowns produced the largest profile ages. Reconnect itself
did not refresh telemetry.

The client was healthy before each intentional terminal close. The resulting
`degraded` state after close is explicitly classified as shutdown, not an
operational recovery failure.

## Accepted-response latency and timeout assessment

Across 740 accepted sustained responses, latency was:

| Median | P95 | P99 | Maximum |
|---:|---:|---:|---:|
| 697.363ms | 807.169ms | 966.983ms | 2705.637ms |

Six accepted responses (0.8108%) exceeded 1.0 second. Two (0.2703%) exceeded each
of 1.5, 2.0, and 2.5 seconds. The slowest accepted response left only 294.363ms
before the existing three-second timeout.

This sample provides no evidence supporting a timeout reduction and directly
argues against 2.5 seconds or less: that threshold would have rejected two
responses that were successfully accepted. It does not prove that exactly three
seconds is optimal. A shorter timeout also cannot make an absent response arrive;
it would detect absence earlier at the cost of more ambiguous generations.

## Unsolicited contribution

Validated unsolicited FC4 observations avoided 132 explicit request opportunities
across the two sustained phases. This was 15.017% of the 879 combined explicit and
avoided opportunities. Unsolicited telemetry is useful but does not replace active
acquisition.

## Safety and production decision

The bounded recovery architecture is safe under the seven natural failures
observed here. That does not make strict ten-second freshness reliable. The
standalone transport is suitable for continued experimental use with truthful
stale-data handling, but the high-frequency profile is not ready for production
Home Assistant integration under a ten-second SLA.

No Smart Energy cadence should be promised from this failed qualification. If a
consumer experiments with the present path, it must inspect `observed_at` and
health state and tolerate ages beyond ten seconds. A more conservative service
target (at least twenty seconds based on the 17.199-second observed maximum) would
require its own sustained qualification before being described as an SLA.

## Evidence limitations

- The aggregate sustained observation window was one hour, split into two
  independent 30-minute runs; no claim of long-term failure rate is made.
- The planned 60-minute run was skipped after both sustained samples failed.
- Natural failures were observed but not induced, so their root cause remains
  unresolved.
- The two non-recovery violations in run 2 show that scheduler/request tail
  latency can also cross ten seconds; deeper causal instrumentation is deferred.
- Concurrent traffic external to this validation was not controlled or attributed.
- Run 1 used the pre-normalization enum label in episode-cause presentation; run 2
  used the normalized label. The underlying event kind, counts, durations, and
  freshness measurements are unaffected, but the instrumentation revision was not
  separately fingerprinted.

## Validation

- Merged-main baseline: 419 tests passed with 12 inherited warnings.
- Final complete suite: 427 tests passed with the same 12 warnings.
- Final focused qualification/session/recovery suite: 69 tests passed.
- Explicit protocol, profile, freshness, standalone-import, write, and Home
  Assistant regression subset: 202 tests passed.
- `git diff --check` passed.
- Private-value scans found no live target identifier in changed files or artifact
  keys.
- Independent GPT-5.6 Sol review approved the corrected diff with no blocking or
  high findings.

## Live artifacts

The following sanitized, untracked, mode-`0600` files contain the detailed
evidence:

- `/tmp/luxpower-ten-second-sanity.json`;
- `/tmp/luxpower-ten-second-run1.json`;
- `/tmp/luxpower-ten-second-run2.json`;
- `/tmp/luxpower-ten-second-aggregate.json`.
