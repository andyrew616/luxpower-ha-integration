"""Home-Assistant-independent LuxPower read profiles.

Semantic telemetry groups describe what a register means.  A read profile is a
separate, consumer-oriented contract describing the raw values needed for one
purpose.  Profiles do not prescribe a polling interval.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Callable, Mapping

from .classes.read_session import (
    LuxObservationSource,
    LuxReadSessionSnapshot,
)
from .constants import input_registers as input_register

HARDWARE_READ_BLOCK_SIZE = 40
ENERGY_FLOW_PROFILE_DEFINITION_VERSION = 1
DIRECT_ENERGY_TELEMETRY_DEFINITION_VERSION = 1
OFF_GRID_STATES = frozenset({64, 96, 128, 136, 192})


class ReadProfileName(str, Enum):
    """Stable names for supported input-register read profiles."""

    ENERGY_FLOW = "energy_flow"
    DIAGNOSTIC = "diagnostic"
    FULL_INPUT = "full_input"


class GridTopology(str, Enum):
    """Explicit grid-power register layout; never inferred from zero values."""

    SINGLE_PHASE = "single_phase"
    THREE_PHASE = "three_phase"
    SPLIT_PHASE = "split_phase"


class LoadLayout(str, Enum):
    """Evidence-backed household-load register layout."""

    STANDARD = "standard"
    TWELVE_K_SINGLE_PHASE = "twelve_k_single_phase"


class ProfileValueQuality(str, Enum):
    """Semantic usability of a decoded profile value."""

    AVAILABLE = "available"
    MISSING = "missing"
    INVALID = "invalid"
    INCOHERENT = "incoherent"
    STALE = "stale"


@dataclass(frozen=True, order=True)
class InputReadBlock:
    """One contiguous input-register request."""

    start: int
    count: int

    @property
    def end(self) -> int:
        return self.start + self.count - 1

    def addresses(self) -> range:
        return range(self.start, self.end + 1)


@dataclass(frozen=True)
class ProfileField:
    """Documented raw authority for one public energy-flow field."""

    name: str
    registers: tuple[int, ...]
    unit: str | None
    scale: float
    derived: bool
    reason: str
    existing_ha_equivalent: str


@dataclass(frozen=True)
class ObservedProfileValue:
    """A value paired with its truthful oldest-input observation time."""

    value: int | None
    observed_at: datetime | None
    registers: tuple[int, ...]
    sources: tuple[LuxObservationSource | None, ...]
    newest_observed_at: datetime | None = None
    observation_sequences: tuple[int | None, ...] = ()
    observation_ranges: tuple[tuple[int, int] | None, ...] = ()
    quality: ProfileValueQuality = ProfileValueQuality.AVAILABLE

    @property
    def available(self) -> bool:
        """Whether the semantic value is currently safe to consume."""
        return self.quality is ProfileValueQuality.AVAILABLE and self.value is not None

    def unavailable_if_stale(
        self,
        *,
        inspected_at: datetime,
        freshness_target: timedelta,
    ) -> "ObservedProfileValue":
        """Return a detached value that fails closed after the freshness target."""
        if freshness_target.total_seconds() <= 0:
            raise ValueError("freshness_target must be positive")
        if inspected_at.tzinfo is None or inspected_at.utcoffset() is None:
            raise ValueError("inspected_at must be timezone-aware")
        if self.quality is not ProfileValueQuality.AVAILABLE:
            return self
        if self.observed_at is None or inspected_at - self.observed_at > freshness_target:
            return replace(self, value=None, quality=ProfileValueQuality.STALE)
        return self


@dataclass(frozen=True)
class DirectEnergyTelemetrySnapshot:
    """Per-device AC-boundary diagnostics from one qualified 0-39 block.

    No site aggregation or battery/solar attribution is performed here.
    ``grid_signed_power_w`` is positive for export and negative for import.
    """

    pinv_w: ObservedProfileValue
    prec_w: ObservedProfileValue
    grid_signed_power_w: ObservedProfileValue
    soc_percent: ObservedProfileValue
    coherent_response_sequence: int | None
    observed_at: datetime | None

    def unavailable_if_stale(
        self,
        *,
        inspected_at: datetime,
        freshness_target: timedelta,
    ) -> "DirectEnergyTelemetrySnapshot":
        """Apply the supported freshness target to every semantic field."""
        fields = {
            name: getattr(self, name).unavailable_if_stale(
                inspected_at=inspected_at,
                freshness_target=freshness_target,
            )
            for name in ("pinv_w", "prec_w", "grid_signed_power_w", "soc_percent")
        }
        return replace(self, **fields)


@dataclass(frozen=True)
class EnergyFlowSnapshot:
    """Lux-specific immediate power-flow values; no Smart Energy model."""

    inverter_state: ObservedProfileValue
    battery_soc_percent: ObservedProfileValue
    pv_power_w: ObservedProfileValue
    battery_charge_power_w: ObservedProfileValue
    battery_discharge_power_w: ObservedProfileValue
    battery_power_w: ObservedProfileValue
    grid_import_power_w: ObservedProfileValue
    grid_export_power_w: ObservedProfileValue
    grid_power_w: ObservedProfileValue
    on_grid_load_power_w: ObservedProfileValue
    eps_load_power_w: ObservedProfileValue
    load_power_w: ObservedProfileValue
    direct_energy: DirectEnergyTelemetrySnapshot
    required_registers: frozenset[int]
    observed_at: datetime | None


@dataclass(frozen=True)
class DiagnosticSnapshot:
    """Raw per-device observations, without installed-topology attribution.

    Raw words (including 0xffff) are diagnostic data, not validated electrical
    measurements. The direct-energy fields retain their existing validation;
    their grid pair is not a claim of complete multi-phase or site grid power.
    """

    registers: Mapping[int, ObservedProfileValue]
    direct_energy: DirectEnergyTelemetrySnapshot
    required_registers: frozenset[int]
    observed_at: datetime | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "registers", MappingProxyType(dict(self.registers)))

    def unavailable_if_stale(
        self, *, inspected_at: datetime, freshness_target: timedelta
    ) -> "DiagnosticSnapshot":
        return replace(
            self,
            registers={
                register: value.unavailable_if_stale(
                    inspected_at=inspected_at, freshness_target=freshness_target
                )
                for register, value in self.registers.items()
            },
            direct_energy=self.direct_energy.unavailable_if_stale(
                inspected_at=inspected_at, freshness_target=freshness_target
            ),
        )


_PV_POWER_REGISTERS = {
    1: input_register.I_PPV1,
    2: input_register.I_PPV2,
    3: input_register.I_PPV3,
    4: input_register.I_PPV4,
    5: input_register.I_PPV5,
    6: input_register.I_PPV6,
}

_GRID_PAIRS = {
    GridTopology.SINGLE_PHASE: (
        (input_register.I_PTOUSER, input_register.I_PTOGRID),
    ),
    GridTopology.THREE_PHASE: (
        (input_register.I_PTOUSER, input_register.I_PTOGRID),
        (input_register.I_PTOUSER_S, input_register.I_PTOGRID_S),
        (input_register.I_PTOUSER_T, input_register.I_PTOGRID_T),
    ),
    GridTopology.SPLIT_PHASE: (
        (input_register.I_PTOUSER_L1N, input_register.I_PTOGRID_L1N),
        (input_register.I_PTOUSER_L2N, input_register.I_PTOGRID_L2N),
    ),
}


def plan_aligned_input_blocks(
    registers: frozenset[int], *, block_size: int = HARDWARE_READ_BLOCK_SIZE
) -> tuple[InputReadBlock, ...]:
    """Return the deterministic minimum aligned block cover for registers."""
    if block_size <= 0:
        raise ValueError("block_size must be positive")
    if any(register < 0 or register >= 750 for register in registers):
        raise ValueError("input registers must be in the 0-749 range")
    starts = sorted({(register // block_size) * block_size for register in registers})
    return tuple(
        InputReadBlock(start, min(block_size, 750 - start)) for start in starts
    )


@dataclass(frozen=True)
class EnergyFlowReadProfile:
    """Capability-resolved immediate household/inverter energy-flow profile.

    ``active_pv_strings`` and ``grid_topology`` must come from known inverter
    configuration. They are deliberately never guessed by the core from register
    values. ``load_layout`` is also mandatory and selects the evidence-backed
    on-grid authority; unsupported layouts are rejected conservatively.
    """

    active_pv_strings: frozenset[int]
    grid_topology: GridTopology
    load_layout: LoadLayout
    name: ReadProfileName = ReadProfileName.ENERGY_FLOW

    def __post_init__(self) -> None:
        strings = frozenset(self.active_pv_strings)
        if not strings or not strings <= frozenset(_PV_POWER_REGISTERS):
            raise ValueError("active_pv_strings must contain only values 1-6")
        object.__setattr__(self, "active_pv_strings", strings)
        object.__setattr__(self, "grid_topology", GridTopology(self.grid_topology))
        object.__setattr__(self, "load_layout", LoadLayout(self.load_layout))
        if self.grid_topology is not GridTopology.SINGLE_PHASE:
            raise ValueError(
                "complete three/split-phase load authority is not yet proven"
            )

    @property
    def on_grid_load_register(self) -> int:
        if self.load_layout is LoadLayout.TWELVE_K_SINGLE_PHASE:
            return input_register.I_ONGRID_LOAD_POWER
        return input_register.I_PLOAD

    @property
    def pv_registers(self) -> tuple[int, ...]:
        return tuple(_PV_POWER_REGISTERS[number] for number in sorted(self.active_pv_strings))

    @property
    def grid_import_registers(self) -> tuple[int, ...]:
        return tuple(pair[0] for pair in _GRID_PAIRS[self.grid_topology])

    @property
    def grid_export_registers(self) -> tuple[int, ...]:
        return tuple(pair[1] for pair in _GRID_PAIRS[self.grid_topology])

    @property
    def fields(self) -> tuple[ProfileField, ...]:
        """Return deterministic authority metadata for public fields."""
        return (
            ProfileField("inverter_state", (input_register.I_STATE,), None, 1, False,
                         "interprets inverter operating/islanding state", "Inverter State"),
            ProfileField("battery_soc_percent", (input_register.I_SOC_SOH,), "%", 1, False,
                         "control-relevant battery state", "Battery SOC"),
            ProfileField("pv_power_w", self.pv_registers, "W", 1, len(self.pv_registers) > 1,
                         "DC/MPPT-side sum of configured active PV string powers", "PV Power"),
            ProfileField("battery_charge_power_w", (input_register.I_PCHARGE,), "W", 1, False,
                         "direct power flowing into the battery", "Battery Charge Power"),
            ProfileField("battery_discharge_power_w", (input_register.I_PDISCHARGE,), "W", 1, False,
                         "direct power flowing out of the battery", "Battery Discharge Power"),
            ProfileField("battery_power_w", (input_register.I_PCHARGE, input_register.I_PDISCHARGE),
                         "W", 1, True, "signed discharge minus charge power", "Battery Flow"),
            ProfileField("grid_import_power_w", self.grid_import_registers, "W", 1,
                         len(self.grid_import_registers) > 1,
                         "direct import summed over the configured phases", "Power from Grid"),
            ProfileField("grid_export_power_w", self.grid_export_registers, "W", 1,
                         len(self.grid_export_registers) > 1,
                         "direct export summed over the configured phases", "Power to Grid"),
            ProfileField("grid_power_w", self.grid_import_registers + self.grid_export_registers,
                         "W", 1, True, "signed configured-phase import minus export", "Grid Flow"),
            ProfileField(
                "on_grid_load_power_w",
                (self.on_grid_load_register,),
                "W",
                1,
                False,
                f"direct {self.load_layout.value} on-grid household load",
                (
                    "On-Grid Load Power"
                    if self.load_layout is LoadLayout.TWELVE_K_SINGLE_PHASE
                    else "Load Power"
                ),
            ),
            ProfileField("eps_load_power_w", (input_register.I_PEPS,), "W", 1, False,
                         "direct standard-layout EPS/off-grid load", "EPS Power"),
            ProfileField("load_power_w", (input_register.I_STATE, self.on_grid_load_register,
                                           input_register.I_PEPS), "W", 1, True,
                         "selects direct on-grid or EPS load using operating state", "Load Power / EPS Power"),
        )

    @property
    def direct_energy_fields(self) -> tuple[ProfileField, ...]:
        """Semantic metadata for 0-39 values incidental to the proven plan.

        These fields deliberately do not participate in ``required_registers``
        or block planning. The qualified 0-39 response already contains them.
        """
        return (
            ProfileField(
                "pinv_w",
                (input_register.I_PINV,),
                "W",
                1,
                False,
                "whole-inverter on-grid AC output; not solar/battery attributed",
                "Inverter Power",
            ),
            ProfileField(
                "prec_w",
                (input_register.I_PREC,),
                "W",
                1,
                False,
                "whole-inverter AC charging/rectification input",
                "AC Charging Rectification Power",
            ),
            ProfileField(
                "grid_signed_power_w",
                (input_register.I_PTOGRID, input_register.I_PTOUSER),
                "W",
                1,
                True,
                "export minus import; positive export and negative import",
                "bounded sign transform of Grid Flow",
            ),
            ProfileField(
                "soc_percent",
                (input_register.I_SOC_SOH,),
                "%",
                1,
                False,
                "per-device SOC low byte; no site authority claim",
                "Battery SOC",
            ),
        )

    @property
    def required_registers(self) -> frozenset[int]:
        return frozenset(
            register for field in self.fields for register in field.registers
        )

    @property
    def read_blocks(self) -> tuple[InputReadBlock, ...]:
        return plan_aligned_input_blocks(self.required_registers)

    def required_registers_in(self, block: InputReadBlock) -> frozenset[int]:
        return self.required_registers.intersection(block.addresses())

    def snapshot(self, raw: LuxReadSessionSnapshot) -> EnergyFlowSnapshot:
        """Build a detached typed profile snapshot without defaulting missing data."""
        state = _observed(raw, (input_register.I_STATE,), lambda values: values[0])
        soc = _observed(raw, (input_register.I_SOC_SOH,), lambda values: values[0] & 0xFF)
        pv = _observed(raw, self.pv_registers, sum)
        charge = _observed(raw, (input_register.I_PCHARGE,), lambda values: values[0])
        discharge = _observed(raw, (input_register.I_PDISCHARGE,), lambda values: values[0])
        battery = _observed(
            raw,
            (input_register.I_PCHARGE, input_register.I_PDISCHARGE),
            lambda values: values[1] - values[0],
        )
        grid_import = _observed(raw, self.grid_import_registers, sum)
        grid_export = _observed(raw, self.grid_export_registers, sum)
        grid = _observed(
            raw,
            self.grid_import_registers + self.grid_export_registers,
            lambda values: sum(values[: len(self.grid_import_registers)])
            - sum(values[len(self.grid_import_registers) :]),
        )
        direct_energy = _direct_energy_snapshot(raw)
        on_grid = _observed(raw, (self.on_grid_load_register,), lambda values: values[0])
        eps = _observed(raw, (input_register.I_PEPS,), lambda values: values[0])
        state_code = raw.input_registers.get(input_register.I_STATE)
        selected_load = (
            _observed(
                raw,
                (input_register.I_STATE, input_register.I_PEPS),
                lambda values: values[1],
            )
            if state_code in OFF_GRID_STATES
            else _observed(
                raw,
                (input_register.I_STATE, self.on_grid_load_register),
                lambda values: values[1],
            )
        )
        required_times = [
            raw.observed_at.input_registers.get(register)
            for register in self.required_registers
        ]
        overall = (
            min(required_times)
            if required_times and all(item is not None for item in required_times)
            else None
        )
        return EnergyFlowSnapshot(
            inverter_state=state,
            battery_soc_percent=soc,
            pv_power_w=pv,
            battery_charge_power_w=charge,
            battery_discharge_power_w=discharge,
            battery_power_w=battery,
            grid_import_power_w=grid_import,
            grid_export_power_w=grid_export,
            grid_power_w=grid,
            on_grid_load_power_w=on_grid,
            eps_load_power_w=eps,
            load_power_w=selected_load,
            direct_energy=direct_energy,
            required_registers=self.required_registers,
            observed_at=overall,
        )


@dataclass(frozen=True)
class DiagnosticReadProfile:
    """Topology-neutral observations using the previously qualified block plan.

    Required registers deliberately match the qualified PV1-3/12K acquisition
    demand, solely to preserve freshness-driven I/O. Register 114 is a raw
    diagnostic word here, not selected as household-load authority. No PV
    strings, grid wiring, load layout or site aggregates are inferred.
    """

    @property
    def name(self) -> ReadProfileName:
        return ReadProfileName.DIAGNOSTIC

    @property
    def required_registers(self) -> frozenset[int]:
        return frozenset({0, 5, 7, 8, 9, 10, 11, 24, 26, 27, 114})

    @property
    def read_blocks(self) -> tuple[InputReadBlock, ...]:
        return plan_aligned_input_blocks(self.required_registers)

    def required_registers_in(self, block: InputReadBlock) -> frozenset[int]:
        return self.required_registers.intersection(block.addresses())

    @property
    def fields(self) -> tuple[ProfileField, ...]:
        return tuple(
            ProfileField(
                f"input_register_{register}", (register,), None, 1, False,
                "raw diagnostic word; no installed electrical authority", "",
            )
            for block in self.read_blocks for register in block.addresses()
        )

    @property
    def direct_energy_fields(self) -> tuple[ProfileField, ...]:
        return (
            ProfileField("pinv_w", (16,), "W", 1, False,
                         "whole-inverter AC output register; not solar/battery attributed", ""),
            ProfileField("prec_w", (17,), "W", 1, False,
                         "whole-inverter AC rectification register", ""),
            ProfileField("grid_signed_power_w", (26, 27), "W", 1, True,
                         "export minus import register pair; not proven site/multiphase total", ""),
            ProfileField("soc_percent", (5,), "%", 1, False,
                         "validated per-device SOC low byte; no site authority", ""),
        )

    def snapshot(self, raw: LuxReadSessionSnapshot) -> DiagnosticSnapshot:
        registers = {
            register: _observed(
                raw, (register,), lambda values: values[0],
                require_provenance=True, require_same_observation=True,
                required_observation_range=(block.start, block.count),
            )
            for block in self.read_blocks for register in block.addresses()
        }
        required_times = [
            raw.observed_at.input_registers.get(register)
            for register in self.required_registers
        ]
        return DiagnosticSnapshot(
            registers=registers,
            direct_energy=_direct_energy_snapshot(raw),
            required_registers=self.required_registers,
            observed_at=(
                min(required_times)
                if all(item is not None for item in required_times)
                else None
            ),
        )


def _direct_energy_snapshot(raw: LuxReadSessionSnapshot) -> DirectEnergyTelemetrySnapshot:
    """Shared, unchanged direct-energy validation for both profile boundaries."""
    pinv = _observed(
        raw, (input_register.I_PINV,), lambda values: values[0],
        validator=_valid_power_registers, require_provenance=True,
        require_same_observation=True,
        required_observation_range=(0, HARDWARE_READ_BLOCK_SIZE),
    )
    prec = _observed(
        raw, (input_register.I_PREC,), lambda values: values[0],
        validator=_valid_power_registers, require_provenance=True,
        require_same_observation=True,
        required_observation_range=(0, HARDWARE_READ_BLOCK_SIZE),
    )
    grid = _observed(
        raw, (input_register.I_PTOGRID, input_register.I_PTOUSER),
        lambda values: values[0] - values[1],
        validator=_valid_power_registers, require_provenance=True,
        require_same_observation=True,
        required_observation_range=(0, HARDWARE_READ_BLOCK_SIZE),
    )
    soc = _observed(
        raw, (input_register.I_SOC_SOH,), lambda values: values[0] & 0xFF,
        validator=_valid_soc_register, require_provenance=True,
        require_same_observation=True,
        required_observation_range=(0, HARDWARE_READ_BLOCK_SIZE),
    )
    times = (pinv.observed_at, prec.observed_at, grid.observed_at, soc.observed_at)
    return DirectEnergyTelemetrySnapshot(
        pinv_w=pinv, prec_w=prec, grid_signed_power_w=grid, soc_percent=soc,
        coherent_response_sequence=_coherent_sequence(pinv, prec, grid, soc),
        observed_at=min(times) if all(item is not None for item in times) else None,
    )


def _observed(
    snapshot: LuxReadSessionSnapshot,
    registers: tuple[int, ...],
    transform: Callable[[tuple[int, ...]], int],
    *,
    validator: Callable[[tuple[int, ...]], bool] | None = None,
    require_provenance: bool = False,
    require_same_observation: bool = False,
    required_observation_range: tuple[int, int] | None = None,
) -> ObservedProfileValue:
    values = tuple(snapshot.input_registers.get(register) for register in registers)
    times = tuple(snapshot.observed_at.input_registers.get(register) for register in registers)
    complete = all(value is not None for value in values) and all(
        observed is not None for observed in times
    )
    sources = tuple(snapshot.input_sources.get(register) for register in registers)
    sequences = tuple(
        snapshot.input_observation_sequences.get(register) for register in registers
    )
    ranges = tuple(
        snapshot.input_observation_ranges.get(register) for register in registers
    )
    if not complete or (require_provenance and any(source is None for source in sources)):
        quality = ProfileValueQuality.MISSING
    elif validator is not None and not validator(values):  # type: ignore[arg-type]
        quality = ProfileValueQuality.INVALID
    elif require_same_observation and (
        any(sequence is None for sequence in sequences)
        or len(set(sequences)) != 1
        or len(set(times)) != 1
        or len(set(sources)) != 1
    ):
        quality = ProfileValueQuality.INCOHERENT
    elif required_observation_range is not None and any(
        observed_range != required_observation_range for observed_range in ranges
    ):
        quality = ProfileValueQuality.INCOHERENT
    else:
        quality = ProfileValueQuality.AVAILABLE
    available = quality is ProfileValueQuality.AVAILABLE
    return ObservedProfileValue(
        value=transform(values) if available else None,  # type: ignore[arg-type]
        observed_at=min(times) if complete else None,  # type: ignore[arg-type]
        registers=registers,
        sources=sources,
        newest_observed_at=max(times) if complete else None,  # type: ignore[arg-type]
        observation_sequences=sequences,
        observation_ranges=ranges,
        quality=quality,
    )


def _valid_power_registers(values: tuple[int, ...]) -> bool:
    """Reject the protocol's unsupported-value sentinel without clipping power."""
    return all(value != 0xFFFF for value in values)


def _valid_soc_register(values: tuple[int, ...]) -> bool:
    """Validate the packed SOC low byte while leaving its SOH byte independent."""
    raw = values[0]
    return raw != 0xFFFF and 0 <= (raw & 0xFF) <= 100


def _coherent_sequence(*values: ObservedProfileValue) -> int | None:
    """Return one shared accepted-response identity, or fail closed."""
    sequences = tuple(
        sequence
        for value in values
        for sequence in value.observation_sequences
    )
    times = tuple(value.observed_at for value in values)
    sources = tuple(source for value in values for source in value.sources)
    ranges = tuple(
        observed_range
        for value in values
        for observed_range in value.observation_ranges
    )
    if (
        not sequences
        or any(value.quality is not ProfileValueQuality.AVAILABLE for value in values)
        or any(sequence is None for sequence in sequences)
        or any(observed is None for observed in times)
        or any(source is None for source in sources)
        or any(observed_range != (0, HARDWARE_READ_BLOCK_SIZE) for observed_range in ranges)
        or len(set(times)) != 1
        or len(set(sources)) != 1
    ):
        return None
    unique = set(sequences)
    return unique.pop() if len(unique) == 1 else None


def profile_block_details(
    profile: EnergyFlowReadProfile | DiagnosticReadProfile,
) -> tuple[Mapping[str, object], ...]:
    """Return sanitized deterministic planner details for docs and tooling."""
    details = []
    for block in profile.read_blocks:
        required = sorted(profile.required_registers_in(block))
        details.append(
            {
                "start": block.start,
                "end": block.end,
                "count": block.count,
                "required_registers": tuple(required),
                "incidental_register_count": block.count - len(required),
                "expected_response_bytes": 37 + (2 * block.count),
            }
        )
    return tuple(details)
