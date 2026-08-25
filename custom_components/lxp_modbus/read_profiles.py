"""Home-Assistant-independent LuxPower read profiles.

Semantic telemetry groups describe what a register means.  A read profile is a
separate, consumer-oriented contract describing the raw values needed for one
purpose.  Profiles do not prescribe a polling interval.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Mapping

from .classes.read_session import (
    LuxObservationSource,
    LuxReadSessionSnapshot,
)
from .constants import input_registers as input_register

HARDWARE_READ_BLOCK_SIZE = 40
OFF_GRID_STATES = frozenset({64, 96, 128, 136, 192})


class ReadProfileName(str, Enum):
    """Stable names for supported input-register read profiles."""

    ENERGY_FLOW = "energy_flow"
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
    required_registers: frozenset[int]
    observed_at: datetime | None


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
                         "sum of explicitly configured active PV string powers", "PV Power"),
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
            required_registers=self.required_registers,
            observed_at=overall,
        )


def _observed(
    snapshot: LuxReadSessionSnapshot,
    registers: tuple[int, ...],
    transform: Callable[[tuple[int, ...]], int],
) -> ObservedProfileValue:
    values = tuple(snapshot.input_registers.get(register) for register in registers)
    times = tuple(snapshot.observed_at.input_registers.get(register) for register in registers)
    complete = all(value is not None for value in values) and all(
        observed is not None for observed in times
    )
    return ObservedProfileValue(
        value=transform(values) if complete else None,  # type: ignore[arg-type]
        observed_at=min(times) if complete else None,  # type: ignore[arg-type]
        registers=registers,
        sources=tuple(
            snapshot.input_sources.get(register) for register in registers
        ),
    )


def profile_block_details(profile: EnergyFlowReadProfile) -> tuple[Mapping[str, object], ...]:
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
