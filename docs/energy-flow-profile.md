# Critical energy-flow read profile

This profile is a LuxPower read contract for immediate household/inverter power
flow. It is independent of Home Assistant and deliberately separate from the
semantic register groups. A semantic `OPERATIONAL` register is not automatically
required at high cadence.

## Authority map

| Field | Input register authority | Raw scale | Rule | Existing HA equivalent |
|---|---:|---:|---|---|
| Inverter state | 0 | raw enum | direct | Inverter State |
| Battery SOC | 5 | low byte, 1% | direct packed value | Battery SOC |
| PV power | 7–9 and optionally 220–222 | 1 W | sum only explicitly configured active strings | PV Power |
| Battery charge | 10 | 1 W | direct | Battery Charge Power |
| Battery discharge | 11 | 1 W | direct | Battery Discharge Power |
| Signed battery power | 10, 11 | 1 W | discharge minus charge; positive is discharge | Battery Flow |
| Grid import/export | 27/26, with topology-specific phase registers | 1 W | direct directional values | Power from/to Grid |
| Signed grid power | topology-specific import/export registers | 1 W | import minus export; positive is import | Grid Flow |
| On-grid load | 170 (standard) or 114 (12K single-phase) | 1 W | explicit load-layout authority | Load Power / On-Grid Load Power |
| EPS load | 24 | 1 W | direct standard-layout value | EPS Power |
| Selected load | 0 plus 170/114 or 24 | 1 W | selects layout-specific direct on-grid/EPS value from operating state | Load Power / EPS Power |

Voltage/current-derived power is excluded because direct power registers exist.
Grid voltage and frequency remain useful diagnostics but are not required to
describe the immediate power balance. Missing raw inputs produce an unknown
profile value; they are never substituted with zero.

Every derived `observed_at` is the oldest local observation time among the raw
inputs actually used. The overall profile `observed_at` is the oldest time among
all required profile registers. Incidental values received in an aligned block
are cached truthfully but cannot improve profile completeness or freshness.

## Hardware-aligned plan

For a standard inverter with PV1–PV3, the minimum proven aligned 40-register
plan is:

| Start–end | Required registers | Incidental values | Expected response |
|---|---|---:|---:|
| 0–39 | 0, 5, 7, 8, 9, 10, 11, 24, 26, 27 | 30 | 117 bytes |
| 160–199 | 170 | 39 | 117 bytes |

For the measured 12K single-phase layout, the same critical fields instead use
blocks 0–39 and 80–119; register 114 replaces register 170 as the direct on-grid
load authority. The load layout is mandatory runtime capability metadata.

Three-phase grid totals add registers 184–187 but no block. Split-phase grid or
PV4–PV6 adds block 200–239. The profile requires active PV strings and grid
topology to be resolved explicitly; it never infers capabilities from zero-valued
registers.

Supported load authorities are deliberately narrow: standard single-phase uses
register 170 on-grid, 12K single-phase uses register 114, and both use register
24 for EPS. Complete three-/split-phase load and EPS authorities remain
unproven, so the profile rejects those topologies instead of publishing a
plausible but incomplete household load. Registers 129–130 and 208–209 remain
deferred pending model-specific evidence.

## Acquisition accounting

The experimental hybrid client checks freshness only for required registers in
each profile block. A validated unmatched FC4 frame may satisfy a block. Request
avoidance is credited once only when that frame keeps all required registers
fresh at a point where their prior explicit observations would otherwise be due.
Unrelated frames, repeated scheduler checks, forced reads, and full scans do not
receive avoidance credit.

The existing complete 0–749 scan, semantic groups, Home Assistant polling, and
write path are unchanged. The profile is additive experimental functionality.

## Experimental bounded recovery

`LuxPowerHybridReadClient` can be given a `RecoveryPolicy`. Recovery is opt-in
and applies only to the standalone experimental profile acquisition path. A
request timeout, connection loss, or ambiguous request taints and closes the
current frame-aware connection before a clean generation is opened. Cached
values and their original `observed_at` timestamps are retained throughout.

The conservative default policy permits one reconnect within one acquisition
and two reconnects in a rolling five-minute window. The first reconnect waits
one second; another recovery in the same window waits five seconds. Exhaustion
is explicit through `LuxPowerRecoveryExhaustedError` and `degraded` acquisition
health. Explicit Modbus rejection, cancellation, and shutdown are never blindly
retried. After reconnect, freshness is evaluated again and only stale required
profile blocks are requested; recovery never triggers a full input scan.

## Read-only hardware validation status

The corrected 12K single-phase two-block plan completed five forced refreshes
with a median/p95 of approximately 1.434 seconds. Two 30-second runs met a
five-second freshness target with no violations. A subsequent sustained burn-in
stopped conservatively after one request timed out at 157.3 seconds: 73 of 74
requests completed, 15 final-tail samples exceeded the target for 1.359 seconds,
and the maximum required-register age reached 6.367 seconds. No malformed frame,
queue loss, or false freshness was observed.

Therefore sustained five-second operation is **not yet demonstrated**. Three-
and two-second validation was not attempted after that failure. Production Home
Assistant integration remains deferred; the current production cadence and
transport are unchanged.
