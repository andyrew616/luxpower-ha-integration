# Direct energy telemetry semantic qualification

This document records the offline semantic gate for the per-device telemetry
added to the supported qualified FC4 snapshot. It does not promote a source in
the energy controller and is not live-device qualification.

## Source audit

The Home Assistant integration in this repository defines these relevant
entities:

| Entity description | Authority | Transform | Electrical meaning |
|---|---:|---|---|
| PV Power | configured registers 7–9, optionally 220–222 | sum active strings | DC/MPPT-side PV input |
| Grid Flow | 27, 26 | `PTOUSER - PTOGRID` | positive import |
| Inverter Power | 16 | direct W | whole-inverter on-grid AC output (`Pinv`) |
| AC Charging Rectification Power | 17 | direct W | whole-inverter AC rectification input (`Prec`) |
| Battery SOC | register 5 low byte | `raw & 0xff` | per-device SOC |

The installed-style names `solar_output_live`, `grid_flow_live`,
`power_to_inverter_live`, `power_from_inverter_live`, and `data_received_time`
are not defined in this repository. They are external Home Assistant/template
assets, so their formulas cannot be inferred from their names. This repository's
coordinator `last_success` and qualified observation times are local acceptance
times, not inverter-origin timestamps.

The read-only energy-controller audit found that its Lux grid convention is the
opposite of this integration's `Grid Flow`: it expects positive export. It also
calculates its configured AC battery pair as power-to-inverter minus
power-from-inverter, positive charging. The controller's configured
`sensor.solar_output` has no register-level provenance in either repository.

## Semantic gate

| Candidate | Gate | Decision |
|---|---|---|
| Register 16 `Pinv` | PROVEN | expose raw W as whole-inverter AC output |
| Register 17 `Prec` | PROVEN | expose raw W as whole-inverter AC rectification input |
| Export-positive grid | PROVEN WITH BOUNDED TRANSFORM | expose `PTOGRID - PTOUSER` from one accepted response |
| Per-device SOC | PROVEN WITH BOUNDED TRANSFORM | expose low byte only when 0–100 |
| `Prec - Pinv` sign | PROVEN WITH BOUNDED TRANSFORM | net whole-inverter AC-boundary direction |
| `Prec - Pinv` as battery AC | UNSUITABLE | `Pinv` can contain PV-derived output; do not attribute it to battery |
| PV register sum | PROVEN as DC/PV only | keep explicitly DC/MPPT-labelled; not solar AC |
| Direct solar AC | UNRESOLVED | no solar-only AC field is proven in 0–39 |
| Site SOC | UNRESOLVED | no installed master/selection rule is proven |

The two known installed device identifiers occur in energy-controller
configuration, but neither repository proves their master/slave roles, PV-string
ownership, CT sharing, or site SOC authority. This contract therefore performs
no cross-device sum, selection, or averaging.

## Quality and temporal contract

The supported per-device fields are `pinv_w`, `prec_w`,
`grid_signed_power_w`, and `soc_percent`. Each retains register authority,
explicit/unsolicited source, oldest/newest local acceptance time, and accepted
observation sequence/range. A semantic value is one of:

- `available`;
- `missing`;
- `invalid`;
- `incoherent`;
- `stale`.

Measured zero is available. Raw power value `0xFFFF` and SOC outside 0–100 are
invalid, while values through `0xFFFE` are not arbitrarily clipped. Grid flow is
unavailable unless registers 26 and 27 share one accepted-response sequence.
The qualified facade removes stale values at inspection time rather than
returning their last number.

All four fields fit in the already-qualified aligned 0–39 FC4 response. The
response sequence proves only that values were decoded from the same accepted
client response; it does not prove simultaneous internal inverter sampling.
Across responses or devices no coherence claim is made.

## Qualification boundary

This change leaves FC4 bytes, CRC, framing, parsing, target/function/range
correlation, one-outstanding-request discipline, sole-reader ownership,
generation fencing, recovery policy, required profile registers, aligned block
planning, freshness selection, and request cadence unchanged. Registers 16 and
17 remain incidental values in the existing 0–39 response.

The transport retains its previous live qualification lineage. The new field
semantics, validation, and accepted-response provenance have offline tests only.
They require a later bounded live comparison before a sidecar schema or energy
source authority can be approved.
