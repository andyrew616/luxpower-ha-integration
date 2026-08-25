# Stage 3 benchmark report

## Status

The read-only benchmark and its synthetic validation are complete. Live hardware
measurements were not performed because no LuxPower host, dongle serial, or inverter
serial could be resolved from the available local project configuration or process
environment. No values were invented, no network scan was attempted, and no socket
was opened by the live command.

This document therefore separates code-inspection facts and theoretical wire sizes
from measurements that remain unresolved.

## Verified code-inspection facts

- Production opens a new TCP connection for each poll attempt.
- Production performs one `reader.read(300)` after connection when initial-data
  skipping is enabled. That read has a one-second timeout, which is suppressed.
- If bytes arrive, production currently parses the entire returned chunk as one Lux
  response and debug-logs its raw hexadecimal form.
- The normal default input scan sends six function-code-4 requests: 0-124, 125-249,
  250-374, 375-499, 500-624, and 625-749.
- Stage 2 currently classifies 89 input registers as operational.
- Those operational addresses require at least two Modbus reads because their total
  address span exceeds the 125-register maximum.
- The two-range experimental cover is 0-107 and 114-232. It includes all 89
  operational addresses plus 138 incidental addresses.

## Theoretical comparison, not hardware evidence

| Shape | Requests | Registers requested | Expected response bytes |
|---|---:|---:|---:|
| Current full scan | 6 | 750 | 1,722 |
| Operational cover | 2 | 227 | 528 |

The operational shape therefore has 66.7% fewer requests and approximately 69.3%
fewer expected response bytes. These figures come from protocol framing and register
counts. They do not establish real latency, stability, or an acceptable cadence.

## Unresolved live questions

No measured answer is yet available for:

- TCP connect or close duration;
- whether unsolicited data arrives, its delay/cadence, or its contents;
- actual duration of the production initial-data read;
- full or selective request/cycle latency;
- 10, 5, 3, or 2 second stability;
- timeout, malformed-packet, or recovery rates;
- whether reconnect-per-cycle is cheap enough;
- whether persistent TCP materially helps;
- whether pushed data is reliable enough to consume;
- effective hardware freshness at any tested cadence.

## Stage 4 recommendation

There is insufficient measured evidence to select reconnect/selective, persistent,
push, or hybrid production architecture. The safe recommendation is to make no
production polling change until the benchmark is run against the intended dongle.

Once connection details are safely available, run the bounded benchmark in this
order:

1. passive unsolicited-data probes;
2. full and operational shapes at 10 seconds;
3. proceed to 5, 3, and 2 seconds only while the preceding cadence has no partial or
   failed cycles;
4. compare connection, initial handling, request, and close proportions;
5. require repeated valid unsolicited frames with matching explicit-read values
   before considering push data.

Here, “valid” must mean integrity-validated translated-data input frames. Function
193 can currently be classified structurally, but its integrity remains unknown and
cannot support a push-telemetry decision by itself.

The provisional next implementation plan, subject to those measurements, is:

- keep the existing full input scan at 60 seconds;
- keep holding registers on their existing first/every-fifth-poll schedule;
- keep BMS polling and backoff unchanged;
- use 0-107 and 114-232 as the candidate fast-read plan;
- begin production rollout at the slowest measured stable cadence that meets the
  operational need;
- preserve Stage 2 timestamps so failed or skipped blocks remain visibly stale;
- retain existing failure/cache semantics until measured evidence justifies a
  separately reviewed change.

Connection strategy remains conditional:

- choose reconnect plus selective polling if connect and initial handling are a
  small fraction of the measured fast cycle;
- investigate persistent TCP only if those costs are material and stability data
  supports it;
- consider push or hybrid handling only if unsolicited frames are consistently
  valid, useful, timely, and correlated with explicit reads.

Confidence is high that two reads cover the current Stage 2 operational set, but
confidence is intentionally **insufficient** for a production connection model or
cadence without live results.

## Validation and review

- Merged-main baseline: `321 passed, 12 warnings` in 17.03 seconds.
- Final complete suite: `350 passed, 12 warnings` in 17.32 seconds.
- The warnings are the pre-existing mocked `StreamWriter.write()` coroutine
  warnings in production read/write tests; this stage did not change those files.
- Plan mode completed successfully without loading secrets or opening a socket.
- Live mode failed closed before network access because the three required
  connection variables were absent.
- Independent GPT-5.6 Sol review identified evidence-integrity issues in the first
  implementation; after corrections and regression coverage, its final review
  reported no remaining blocking or high-severity finding.

The requested GPT-5.3 Codex Spark discovery command was attempted first in a
read-only sandbox and then, under explicit no-edit constraints, workspace-write.
Both attempts failed during CLI app-server initialization before repository
inspection. The specified GPT-5.6 Luna fallback failed at the same initialization
stage. Discovery therefore continued by direct read-only inspection.

## Live artifacts

None. Live benchmark output should be written outside the repository, preferably
under `/tmp`, and must remain untracked.
