"""Tests for Lux-specific read profiles, separate from semantic groups."""

from datetime import datetime, timedelta, timezone

import pytest

from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.constants import input_registers as reg
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    GridTopology,
    InputReadBlock,
    LoadLayout,
    plan_aligned_input_blocks,
    profile_block_details,
)
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
)


def standard_profile(strings=frozenset({1, 2, 3})):
    return EnergyFlowReadProfile(
        strings,
        GridTopology.SINGLE_PHASE,
        LoadLayout.STANDARD,
    )


def test_common_energy_flow_profile_is_deterministic_and_two_blocks():
    first = standard_profile()
    second = standard_profile(frozenset({3, 1, 2}))

    assert first == second
    assert first.required_registers == frozenset(
        {0, 5, 7, 8, 9, 10, 11, 24, 26, 27, 170}
    )
    assert first.read_blocks == (InputReadBlock(0, 40), InputReadBlock(160, 40))
    assert {field.name for field in first.fields} == {
        "inverter_state",
        "battery_soc_percent",
        "pv_power_w",
        "battery_charge_power_w",
        "battery_discharge_power_w",
        "battery_power_w",
        "grid_import_power_w",
        "grid_export_power_w",
        "grid_power_w",
        "on_grid_load_power_w",
        "eps_load_power_w",
        "load_power_w",
    }
    assert profile_block_details(first) == (
        {
            "start": 0,
            "end": 39,
            "count": 40,
            "required_registers": (0, 5, 7, 8, 9, 10, 11, 24, 26, 27),
            "incidental_register_count": 30,
            "expected_response_bytes": 117,
        },
        {
            "start": 160,
            "end": 199,
            "count": 40,
            "required_registers": (170,),
            "incidental_register_count": 39,
            "expected_response_bytes": 117,
        },
    )


def test_profile_capabilities_add_only_physically_required_blocks():
    twelve_k = EnergyFlowReadProfile(
        frozenset({1, 3}),
        GridTopology.SINGLE_PHASE,
        LoadLayout.TWELVE_K_SINGLE_PHASE,
    )
    six_pv = standard_profile(frozenset(range(1, 7)))

    assert twelve_k.read_blocks == (
        InputReadBlock(0, 40),
        InputReadBlock(80, 40),
    )
    assert twelve_k.on_grid_load_register == reg.I_ONGRID_LOAD_POWER
    assert six_pv.read_blocks == (
        InputReadBlock(0, 40),
        InputReadBlock(160, 40),
        InputReadBlock(200, 40),
    )


def test_profile_requires_resolved_pv_capability_and_planner_validates_ranges():
    with pytest.raises(TypeError):
        EnergyFlowReadProfile(frozenset({1}))
    with pytest.raises(ValueError):
        standard_profile(frozenset())
    with pytest.raises(ValueError):
        standard_profile(frozenset({7}))
    with pytest.raises(ValueError):
        plan_aligned_input_blocks(frozenset({750}))

    assert EnergyFlowReadProfile(
        frozenset({1}), "single_phase", "standard"
    ).grid_topology is GridTopology.SINGLE_PHASE
    with pytest.raises(ValueError, match="load authority"):
        EnergyFlowReadProfile(
            frozenset({1}), GridTopology.THREE_PHASE, LoadLayout.STANDARD
        )
    with pytest.raises(ValueError, match="load authority"):
        EnergyFlowReadProfile(
            frozenset({1}), GridTopology.SPLIT_PHASE, LoadLayout.STANDARD
        )


def test_profile_snapshot_preserves_authority_and_oldest_input_freshness():
    profile = standard_profile()
    newer = datetime(2026, 8, 25, 12, 0, 5, tzinfo=timezone.utc)
    older = newer - timedelta(seconds=2)
    values = {
        reg.I_STATE: 4,
        reg.I_SOC_SOH: 0x632A,
        reg.I_PPV1: 100,
        reg.I_PPV2: 200,
        reg.I_PPV3: 300,
        reg.I_PCHARGE: 50,
        reg.I_PDISCHARGE: 500,
        reg.I_PEPS: 20,
        reg.I_PTOGRID: 70,
        reg.I_PTOUSER: 120,
        reg.I_PLOAD: 900,
    }
    observed = {register: newer for register in values}
    observed[reg.I_PPV2] = older
    sources = {register: LuxObservationSource.EXPLICIT for register in values}
    snapshot = profile.snapshot(
        LuxReadSessionSnapshot(
            input_registers=values,
            observed_at=LuxPowerObservationTimes(input_registers=observed),
            input_sources=sources,
        )
    )

    assert snapshot.battery_soc_percent.value == 42
    assert snapshot.pv_power_w.value == 600
    assert snapshot.pv_power_w.observed_at == older
    assert snapshot.battery_power_w.value == 450
    assert snapshot.grid_power_w.value == 50
    assert snapshot.load_power_w.value == 900
    assert snapshot.observed_at == older


def test_off_grid_load_uses_eps_and_oldest_of_state_and_power():
    profile = standard_profile(frozenset({1}))
    state_time = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    eps_time = state_time + timedelta(seconds=1)
    values = {
        reg.I_STATE: 136,
        reg.I_PEPS: 321,
    }
    snapshot = profile.snapshot(
        LuxReadSessionSnapshot(
            input_registers=values,
            observed_at=LuxPowerObservationTimes(
                input_registers={reg.I_STATE: state_time, reg.I_PEPS: eps_time}
            ),
        )
    )

    assert snapshot.load_power_w.value == 321
    assert snapshot.load_power_w.observed_at == state_time


def test_twelve_k_layout_uses_register_114_not_standard_register_170():
    profile = EnergyFlowReadProfile(
        frozenset({1, 3}),
        GridTopology.SINGLE_PHASE,
        LoadLayout.TWELVE_K_SINGLE_PHASE,
    )
    at = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    snapshot = profile.snapshot(
        LuxReadSessionSnapshot(
            input_registers={
                reg.I_STATE: 4,
                reg.I_ONGRID_LOAD_POWER: 444,
                reg.I_PLOAD: 999,
            },
            observed_at=LuxPowerObservationTimes(
                input_registers={
                    reg.I_STATE: at,
                    reg.I_ONGRID_LOAD_POWER: at,
                    reg.I_PLOAD: at,
                }
            ),
        )
    )

    assert snapshot.on_grid_load_power_w.value == 444
    assert snapshot.load_power_w.value == 444
    assert reg.I_PLOAD not in profile.required_registers


def test_missing_raw_input_never_defaults_a_derived_value_to_zero():
    profile = standard_profile(frozenset({1, 2}))
    at = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    snapshot = profile.snapshot(
        LuxReadSessionSnapshot(
            input_registers={reg.I_PPV1: 100},
            observed_at=LuxPowerObservationTimes(
                input_registers={reg.I_PPV1: at}
            ),
        )
    )

    assert snapshot.pv_power_w.value is None
    assert snapshot.pv_power_w.observed_at is None


def test_existing_semantic_groups_remain_independent_of_profile_membership():
    profile = standard_profile()

    assert input_register_group(reg.I_VBAT) is TelemetryGroup.OPERATIONAL
    assert reg.I_VBAT not in profile.required_registers
    assert input_register_group(reg.I_PLOAD) is TelemetryGroup.OPERATIONAL
    assert reg.I_PLOAD in profile.required_registers
