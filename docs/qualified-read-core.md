# Qualified Lux FC4 read core

## Supported product boundary

Production consumers import the read-only core only from `luxpower.qualified`.
That module exposes an owner-level lifecycle around one inverter and one
connection:

```python
from luxpower.qualified import (
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    QualifiedLuxReadClient,
    RecoveryPolicy,
)

profile = EnergyFlowReadProfile(
    active_pv_strings=frozenset({1, 2, 3}),
    grid_topology=GridTopology.SINGLE_PHASE,
    load_layout=LoadLayout.TWELVE_K_SINGLE_PHASE,
)
client = QualifiedLuxReadClient(
    host="192.0.2.1",
    port=8000,
    dongle_serial="DG00000001",
    inverter_serial="0000000001",
    profile=profile,
    recovery_policy=RecoveryPolicy(),
)

await client.async_start()
try:
    snapshot = await client.async_acquire()
finally:
    await client.async_close()
```

`async_start()` opens the existing persistent session and sole reader task.
`async_acquire()` performs one freshness-driven acquisition and returns a
detached typed snapshot. `snapshot()` performs no socket I/O. `async_close()`
signals recovery shutdown, invalidates the connection generation, and performs
the existing bounded reader/socket close. The owner supplies any permanent
scheduling loop; this package creates none.

Facade start and close operations are serialized. Close interrupts and then
drains an in-flight acquisition before a later start may create a new
connection generation; overlapping close callers return only after the one
underlying close has completed. Callers should still use one explicit lifecycle
owner rather than treating this as a service supervisor.

The supported facade deliberately does not expose arbitrary register reads,
forced/full scans, mutable freshness phases, observation subscriptions, soak
runners, writes, or generic function-code selection. The older `luxpower` root
exports and `luxpower.hybrid` imports remain for compatibility and qualification
tooling, but they are not the supported production boundary.

Completed recovery diagnostics retain the most recent 512 events. Lifetime
`recovery_events_recorded` and `recovery_events_dropped` counters make rollover
explicit; all recovery-decision counters remain cumulative. Retention is
diagnostic only and does not participate in retry, reconnect, or health-state
decisions.

Each `QualifiedLuxSnapshot` contains the existing `EnergyFlowSnapshot`, profile
field definitions with units and register authorities, per-field value/register/
source provenance, oldest-input acceptance times, acquisition health, the
configured freshness target, an inspection time, and a derived freshness flag.
It does not invent an inverter timestamp or expose a transport generation as a
cross-device coherence claim.

API version 2 adds `profile.direct_energy`, a versioned, per-device semantic
contract for `pinv_w`, `prec_w`, export-positive `grid_signed_power_w`, and
validated `soc_percent`. These values come from the existing accepted 0–39
response and fail closed for missing provenance, `0xFFFF`, invalid SOC,
incoherent derived inputs, or staleness at inspection time. Accepted-response
sequence and range are retained only to prove 0–39 response ownership within one client;
it is not an inverter timestamp or a cross-device synchronisation claim.

The contract intentionally has no AC solar, AC battery, site SOC, or two-device
aggregate. PV registers are DC/MPPT-side. `Pinv` and `Prec` describe the whole
hybrid inverter AC boundary; their difference is not qualified as battery-only.
The full audit and gate are recorded in
[`direct-energy-telemetry.md`](direct-energy-telemetry.md).

## Packaging and dependencies

The distribution requires Python 3.13 or newer, the version exercised by the
repository CI. The qualified import closure uses only the Python standard
library and therefore declares no runtime dependency on Home Assistant.
Home Assistant and repository test dependencies are separate optional extras.

Install a pinned checkout or built wheel instead of manipulating `PYTHONPATH`:

```console
python -m pip install --no-deps .
```

This is deliberately a whole-repository compatibility distribution: it also
contains the existing legacy/Home Assistant modules because safely relocating
the qualified implementation would create transport drift in this slice. Those
modules are outside the supported core and some include a separate legacy
transport and write behavior. Lazy `luxpower` package initialization ensures
that importing `luxpower.qualified` loads neither that legacy transport nor its
write-capable module, and it neither imports nor requires Home Assistant. The
distribution defines no console script and has no publishing configuration.

## Product core versus qualification tooling

The product facade delegates to the same `LuxPowerHybridReadClient` exercised by
the qualification harness. The harness retains private access to forced reads,
phase-specific freshness changes, detailed diagnostics, and bounded-duration
runners because those controls are deliberately absent from the production API.
The supported qualified path and its harness therefore share one transport
implementation; this claim does not apply to the repository's retained legacy
Home Assistant path.

Production core modules:

- `luxpower/qualified.py`: supported product facade and lifecycle contract;
- `luxpower/hybrid.py` through `LuxPowerHybridReadClient`: freshness acquisition
  and recovery ownership;
- `custom_components/lxp_modbus/classes/read_session.py`: socket, sole reader,
  FC4 request correlation, generation fencing, cache, and observation times;
- `frame_decoder.py`, `lxp_response.py`, `lxp_request_builder.py`, and
  `lxp_packet_utils.py`: wire framing and validation;
- `read_profiles.py`: qualified profile planning and typed snapshot decoding;
- `recovery.py`: recovery policy and sanitized metrics;
- their constants, exception, observation, validation, telemetry-group, and
  diagnostic support modules.

Qualification and development tooling:

- `luxpower/profile_validation.py` and its report schemas;
- `luxpower/hybrid.py` helpers below the acquisition class and its legacy CLI;
- `luxpower/benchmark.py`;
- `luxpower/fc4_matrix.py`;
- `tests/` and the qualification documents.

## Qualification identity and limits

The corrected live two-hour soak qualified Git revision
`865c34e2dd6f93807fd35711d480d757d5694a5f`. Its reviewed evidence is
`docs/hours-scale-soak-qualification.md`. The requested starting revision for
this product-boundary work, `8e392fd3795fceeca057f69215fee64dab2f07a8`, is a
descendant whose only subsequent change documented that completed evidence.

The live evidence covers the existing FC4 request/session/parser path, exact
correlation, connection-generation fencing, cache acceptance, profile field and
block plan, freshness decisions, and bounded recovery decisions under the
recorded live configuration. This slice adds packaging, a lifecycle facade, and
bounded retention of already-completed recovery events. Repository tests can
show that those changes preserve offline behavior; they do not constitute new
live-device qualification of the facade or a future service loop.

Repeat live qualification before production promotion when a revision changes:

- FC4 request bytes, CRC, framing, parsing, target/function/range correlation,
  or the one-outstanding-request/sole-reader discipline;
- generation fencing, cache acceptance, observation timestamps, source
  classification, or false-freshness protection;
- request, drain, reply, reconnect, retry-budget, keepalive, inactivity, or
  shutdown decisions;
- required profile registers, aligned block planning, field transforms, or
  freshness rules;
- runtime scheduling, service supervision, concurrency, or device topology;
- any newly exposed field or semantic claim.

The direct energy fields added after the live soak are offline-qualified only.
Their register mapping, transforms, quality handling, and response provenance
must be compared with live Home Assistant/device evidence before any production
source-authority promotion.

Packaging or documentation-only changes still require isolated import/install
checks and the complete offline transport/profile regression suite. Diagnostic
retention changes require rollover/counter tests proving that recovery decisions
are unchanged. They do not qualify new telemetry fields.
