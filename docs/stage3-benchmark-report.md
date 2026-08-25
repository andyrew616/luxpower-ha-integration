# Stage 3 hardware benchmark report

## Status and safety stop

The strictly read-only benchmark was executed against the intended local dongle.
No host, dongle serial, inverter serial, raw packet, or register value is included
in this report or committed artifacts.

The first supplied inverter target was tried first. An integrity-validated,
one-register FC4 response identified the one authorized alternate target, so the
run was retried once with that alternate. This categorical relationship is retained
in a sanitized evidence artifact without either identifier. No other serial was
tried.

Live progression stopped during the operational preflight, before cadence testing.
After four correct explicit responses, the fifth request received an integrity-
validated FC4 frame for a different register block. It could be pushed telemetry or
a delayed duplicate; either way, it did not match the outstanding request and the
cycle was therefore partial. No 10, 5, 3, or 2 second cadence run was attempted
after this safety stop.

Only TCP connect/disconnect, passive reads, and FC4 input-register requests were
used. No write or configuration-changing packet was sent.

## Connection measurements

Across 18 measured successful connections from passive and preflight stages:

| Metric | Mean | Median | P95 | Min | Max |
|---|---:|---:|---:|---:|---:|
| TCP connect | 34.166 ms | 44.227 ms | 88.032 ms | 2.643 ms | 88.032 ms |

Clean close durations were below 0.15 ms in the complete full-scan samples.
Connection establishment is a small part of cycle cost.

## Initial-data handling and unsolicited traffic

The production-equivalent initial `read(300)` was observed six times:

| Metric | Mean | Median | P95 | Min | Max |
|---|---:|---:|---:|---:|---:|
| Initial handling | 919.355 ms | 1000.460 ms | 1001.128 ms | 512.553 ms | 1001.128 ms |

Five initial reads received no bytes and consumed the full one-second timeout. One
returned a validated 40-register FC4 frame after 512.553 ms.

Twelve independent two-second passive probes were performed. One probe received
136 bytes in two socket chunks:

- a 19-byte TCP-function-193 frame after 448.913 ms; its structure was accepted but
  integrity remains unknown because no CRC rule is established;
- a validated 117-byte FC4 frame for input registers 0-39 after 1575.111 ms.

The validated initial FC4 frame covered registers 40-79. Compared with the
immediately subsequent explicit read, 39 values matched and one differed, which is
consistent with useful but time-varying telemetry.

During the operational preflight, a validated 0-39 FC4 frame was already available
0.059 ms after sending a request for 160-199. The frame was correctly rejected as
the explicit response, making the cycle partial. The benchmark cannot distinguish
this frame from a push versus a delayed duplicate of the earlier 0-39 response.
Either interpretation demonstrates that the next socket read cannot safely be
assumed to belong to the latest request.

Unsolicited data is therefore potentially useful, but one passive hit in twelve
two-second windows does not demonstrate complete or reliable push coverage.

## Register-block capability

Read-only diagnostics established these hardware constraints:

| FC4 request | Result |
|---|---|
| One register | Success |
| 40 aligned registers | Success |
| 108 registers | Valid Modbus exception 3 |
| 125 registers | Valid Modbus exception 3 |
| Unaligned 77-114 | Valid Modbus exception 3 |

The integration's existing aligned 40-register layout is the compatible layout
proven by this run. The tested larger ranges and the tested unaligned 77-114 range
failed; this does not prove that every other possible smaller or unaligned range
would fail. The original two-request operational proposal, 0-107 and 114-232, is
not supported by this hardware.

The smallest operational cover proven compatible in this run is six aligned blocks:

- 0-39;
- 40-79;
- 80-119;
- 120-159;
- 160-199;
- 200-239.

These six blocks cover all 89 Stage 2 operational addresses, request 240 registers,
and have 702 expected response bytes. No production polling range was changed.

## Full-scan measurements

Two complete 0-749 scans succeeded using the protected legacy 40-register layout:
19 requests, eighteen 40-register blocks and a final 30-register block.

| Sample | Cycle | Connect | Initial handling | Mean request |
|---|---:|---:|---:|---:|
| 1 | 14182.894 ms | 47.017 ms | 1000.424 ms | 691.214 ms |
| 2 | 13808.130 ms | 88.032 ms | 512.553 ms | 694.975 ms |

Both scans read all 750 registers. Across them there were no timeouts, malformed
responses, recovery attempts, connection failures, or freshness corruption.
Expected response traffic was 2203 bytes per full scan.

Across 43 accepted requests from the live preflight evidence, request latency was:

| Mean | Median | P95 | Min | Max |
|---:|---:|---:|---:|---:|
| 701.293 ms | 713.083 ms | 789.230 ms | 619.035 ms | 921.409 ms |

## Operational/selective measurement

No complete standalone selective cycle was obtained. The aligned preflight accepted
four blocks, refreshing 160 registers, before the unmatched 0-39 frame arrived in
place of the expected 160-199 response. It stopped partial after 4093.218 ms, with
no timeout, malformed packet, recovery, or unread-register freshness change.

As supporting timing evidence, the first six aligned requests within each successful
full scan took 4054.085 ms and 4093.575 ms respectively. Including measured connect
and initial handling, a reconnect-based 0-239 cycle would have occupied about
4.69-5.10 seconds before allowing for packet-routing overhead. This is an inference
from successful requests inside full scans, not a successful selective cadence run.

## Freshness verification

Each complete full scan advanced all 750 requested register timestamps. The partial
operational preflight advanced only the 160 registers from its four accepted blocks.
The unexpected unmatched frame was not accepted as the outstanding explicit response,
and the failed expected block did not receive a new observation timestamp. No
unread-register timestamp changed.

This confirms conservative observation-timestamp semantics under the measured
partial-read case. Existing automated tests, rather than this benchmark-local
tracker, verify that production known-good cached values remain available with
their earlier observation ages.

## Cadence results

No cadence stability claim is made:

| Requested cadence | Result |
|---|---|
| 10 seconds | Not run: operational preflight safety stop |
| 5 seconds | Not run: 10-second stage unavailable |
| 3 seconds | Not run: slower stages unavailable |
| 2 seconds | Not run: slower stages unavailable |

The request timing implies that six sequential reads consume approximately 4.1
seconds before reconnect and initial-data handling. A reconnect-per-cycle 5-second
target is therefore marginal, while 3- and 2-second active six-block polling are not
supported by the measured sequential request latency.

## Stage 4 recommendation

Use a hybrid, frame-aware transport design (Option D), with persistent connection
investigation as an enabling mechanism rather than a connect-time optimization.

Evidence for this recommendation:

- TCP connect is cheap: median 44.227 ms.
- The current initial discard costs 0.5-1.0 seconds and sometimes discards validated
  register telemetry.
- Valid FC4 frames that do not match the outstanding request can arrive during an
  explicit request sequence.
- The next socket read cannot safely be assumed to be the requested response.
- Push data was too intermittent in passive probes to replace polling.
- Six hardware-aligned requests require about 4.1 seconds of response time.

The next stage should introduce a single frame decoder/router that continuously
parses complete frames, routes explicit responses by target/function/register range,
and treats other validated FC4 frames as unsolicited observations. It should then
combine pushed observations with explicit reads for blocks that remain missing or
stale.

Persistent TCP is worth evaluating because it avoids repeated initial discard and
supports continuous frame routing, not because connection establishment itself is
expensive. Reconnect must remain the recovery fallback.

## Proposed Stage 4 fast-read plan

- Fast operational blocks: aligned 0-39, 40-79, 80-119, 120-159, 160-199, and
  200-239.
- Initial target cadence: 5 seconds, explicitly treated as provisional and requiring
  validation after frame routing exists.
- Do not claim or enable 3- or 2-second six-block active polling from current
  evidence.
- Accept validated unsolicited FC4 blocks as fresh observations, but actively read
  any block that is absent or too old.
- Preserve the full aligned 0-749 scan at 60 seconds.
- Preserve holding-register cadence and BMS polling/backoff.
- Advance freshness only for validated, accepted register blocks.
- On timeout, malformed data, unmatched response, or connection loss, reconnect and
  retain known-good values with their existing observation timestamps.

Confidence is high that frame-aware correlation is required, high that the existing
aligned 40-register layout is compatible, low that a hybrid persistent model will
make 5-second freshness practical until it is measured, and insufficient for 3- or
2-second operation.

## Validation and independent review

- Merged-main baseline: `321 passed, 12 warnings`.
- Final implementation suite before live execution: `350 passed, 12 warnings`.
- Plan mode completed without loading secrets or opening a socket.
- Independent GPT-5.6 Sol review reported no remaining blocking or high-severity
  implementation findings after corrections.
- Independent post-live GPT-5.6 Sol review found the sanitized evidence and Stage 4
  recommendation proportionate after wording corrections.
- Live execution created no repository changes and issued no writes.

## Live artifacts

Sanitized, untracked artifacts are stored outside the repository:

- `/tmp/luxpower-benchmark.json` — final aligned preflight and safety stop;
- `/tmp/luxpower-benchmark-initial-target.json` — initial-target evidence;
- `/tmp/luxpower-benchmark-unsupported-default-block.json` — default block-size
  rejection;
- `/tmp/luxpower-benchmark-unaligned-selective.json` — unaligned selective rejection.
- `/tmp/luxpower-serial-selection-evidence.json` — categorical authorized-target
  selection evidence without identifiers.

The temporary environment and runner files containing runtime configuration were
deleted after execution.
