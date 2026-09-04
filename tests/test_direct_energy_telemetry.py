"""Offline semantic contract tests for per-device direct Lux energy telemetry."""

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
    ProfileValueQuality,
)


AT = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
DIRECT_REGISTERS = (reg.I_SOC_SOH, reg.I_PINV, reg.I_PREC, reg.I_PTOGRID, reg.I_PTOUSER)


def profile() -> EnergyFlowReadProfile:
    return EnergyFlowReadProfile(
        frozenset({1, 2, 3}),
        GridTopology.SINGLE_PHASE,
        LoadLayout.STANDARD,
    )


def raw_snapshot(
    values: dict[int, int],
    *,
    observed: dict[int, datetime] | None = None,
    sequences: dict[int, int] | None = None,
) -> LuxReadSessionSnapshot:
    times = observed or {register: AT for register in values}
    sources = {
        register: LuxObservationSource.EXPLICIT for register in values
    }
    response_sequences = sequences or {register: 41 for register in values}
    return LuxReadSessionSnapshot(
        input_registers=values,
        observed_at=LuxPowerObservationTimes(input_registers=times),
        input_sources=sources,
        input_observation_sequences=response_sequences,
        input_observation_ranges={register: (0, 40) for register in values},
        explicit_observed_at=times,
    )


def complete_values(**overrides: int) -> dict[int, int]:
    values = {
        reg.I_SOC_SOH: 0x632A,
        reg.I_PINV: 1200,
        reg.I_PREC: 300,
        reg.I_PTOGRID: 700,
        reg.I_PTOUSER: 100,
    }
    values.update({getattr(reg, name): value for name, value in overrides.items()})
    return values


def test_direct_contract_uses_exact_registers_without_changing_read_plan():
    energy_profile = profile()

    assert energy_profile.read_blocks == (
        InputReadBlock(0, 40),
        InputReadBlock(160, 40),
    )
    assert reg.I_PINV not in energy_profile.required_registers
    assert reg.I_PREC not in energy_profile.required_registers
    assert {
        field.name: (field.registers, field.unit)
        for field in energy_profile.direct_energy_fields
    } == {
        "pinv_w": ((16,), "W"),
        "prec_w": ((17,), "W"),
        "grid_signed_power_w": ((26, 27), "W"),
        "soc_percent": ((5,), "%"),
    }

    unsupported = {"battery_ac_signed_power_w", "solar_ac_power_w", "site_soc_percent"}
    assert unsupported.isdisjoint(
        field.name for field in energy_profile.direct_energy_fields
    )


@pytest.mark.parametrize(
    ("to_grid", "to_user", "expected"),
    [(0, 450, -450), (550, 0, 550), (0, 0, 0)],
)
def test_grid_is_export_positive_import_negative_and_preserves_zero(
    to_grid: int,
    to_user: int,
    expected: int,
):
    values = complete_values(I_PTOGRID=to_grid, I_PTOUSER=to_user)
    snapshot = profile().snapshot(raw_snapshot(values)).direct_energy

    assert snapshot.grid_signed_power_w.value == expected
    assert snapshot.grid_signed_power_w.available is True
    assert snapshot.grid_signed_power_w.registers == (26, 27)
    assert snapshot.grid_signed_power_w.observation_sequences == (41, 41)
    assert snapshot.grid_signed_power_w.observation_ranges == ((0, 40), (0, 40))


@pytest.mark.parametrize(
    ("pinv", "prec"),
    [(900, 0), (0, 900), (0, 0)],
)
def test_pinv_and_prec_remain_independent_whole_inverter_ac_diagnostics(
    pinv: int,
    prec: int,
):
    snapshot = profile().snapshot(
        raw_snapshot(complete_values(I_PINV=pinv, I_PREC=prec))
    ).direct_energy

    assert snapshot.pinv_w.value == pinv
    assert snapshot.prec_w.value == prec
    assert not hasattr(snapshot, "battery_ac_signed_power_w")


@pytest.mark.parametrize(
    ("raw_soc", "expected", "quality"),
    [
        (0x0000, 0, ProfileValueQuality.AVAILABLE),
        (0x0064, 100, ProfileValueQuality.AVAILABLE),
        (0x632A, 42, ProfileValueQuality.AVAILABLE),
        (0x0065, None, ProfileValueQuality.INVALID),
        (0xFFFF, None, ProfileValueQuality.INVALID),
    ],
)
def test_soc_low_byte_has_bounded_per_device_domain(
    raw_soc: int,
    expected: int | None,
    quality: ProfileValueQuality,
):
    snapshot = profile().snapshot(
        raw_snapshot(complete_values(I_SOC_SOH=raw_soc))
    )

    assert snapshot.direct_energy.soc_percent.value == expected
    assert snapshot.direct_energy.soc_percent.quality is quality
    assert snapshot.battery_soc_percent.value == (raw_soc & 0xFF)


@pytest.mark.parametrize("register", (reg.I_PINV, reg.I_PREC, reg.I_PTOGRID, reg.I_PTOUSER))
def test_unsupported_power_sentinel_is_invalid_not_a_measurement(register: int):
    values = complete_values()
    values[register] = 0xFFFF
    snapshot = profile().snapshot(raw_snapshot(values)).direct_energy

    affected = {
        reg.I_PINV: (snapshot.pinv_w,),
        reg.I_PREC: (snapshot.prec_w,),
        reg.I_PTOGRID: (snapshot.grid_signed_power_w,),
        reg.I_PTOUSER: (snapshot.grid_signed_power_w,),
    }[register]
    assert all(field.value is None for field in affected)
    assert all(field.quality is ProfileValueQuality.INVALID for field in affected)


def test_valid_high_power_is_not_arbitrarily_clipped():
    snapshot = profile().snapshot(
        raw_snapshot(complete_values(I_PINV=0xFFFE))
    ).direct_energy

    assert snapshot.pinv_w.value == 0xFFFE
    assert snapshot.pinv_w.quality is ProfileValueQuality.AVAILABLE


def test_missing_register_and_provenance_fail_closed_without_becoming_zero():
    values = complete_values()
    del values[reg.I_PREC]
    snapshot = profile().snapshot(raw_snapshot(values)).direct_energy

    assert snapshot.prec_w.value is None
    assert snapshot.prec_w.quality is ProfileValueQuality.MISSING
    assert snapshot.prec_w.available is False


def test_derived_grid_requires_one_accepted_response_identity():
    values = complete_values()
    sequences = {register: 41 for register in values}
    sequences[reg.I_PTOUSER] = 42
    snapshot = profile().snapshot(
        raw_snapshot(values, sequences=sequences)
    ).direct_energy

    assert snapshot.grid_signed_power_w.value is None
    assert snapshot.grid_signed_power_w.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.coherent_response_sequence is None


def test_direct_contract_rejects_values_from_an_unqualified_response_range():
    values = complete_values()
    raw = raw_snapshot(values)
    raw = LuxReadSessionSnapshot(
        input_registers=raw.input_registers,
        observed_at=raw.observed_at,
        input_sources=raw.input_sources,
        input_observation_sequences=raw.input_observation_sequences,
        input_observation_ranges={register: (5, 23) for register in values},
        explicit_observed_at=raw.explicit_observed_at,
    )
    snapshot = profile().snapshot(raw).direct_energy

    assert snapshot.pinv_w.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.prec_w.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.grid_signed_power_w.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.soc_percent.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.coherent_response_sequence is None


def test_equal_sequence_cannot_mask_inconsistent_acceptance_times():
    values = complete_values()
    observed = {register: AT for register in values}
    observed[reg.I_PTOUSER] = AT + timedelta(microseconds=1)
    snapshot = profile().snapshot(
        raw_snapshot(values, observed=observed)
    ).direct_energy

    assert snapshot.grid_signed_power_w.value is None
    assert snapshot.grid_signed_power_w.quality is ProfileValueQuality.INCOHERENT
    assert snapshot.coherent_response_sequence is None


def test_all_direct_fields_retain_one_accepted_0_39_response_identity():
    snapshot = profile().snapshot(raw_snapshot(complete_values())).direct_energy

    assert snapshot.coherent_response_sequence == 41
    assert snapshot.observed_at == AT
    assert all(
        field.observed_at == AT
        and field.newest_observed_at == AT
        and set(field.sources) == {LuxObservationSource.EXPLICIT}
        for field in (
            snapshot.pinv_w,
            snapshot.prec_w,
            snapshot.grid_signed_power_w,
            snapshot.soc_percent,
        )
    )


def test_stale_fields_become_unavailable_without_fabricating_zero():
    snapshot = profile().snapshot(raw_snapshot(complete_values())).direct_energy

    stale = snapshot.unavailable_if_stale(
        inspected_at=AT + timedelta(seconds=21),
        freshness_target=timedelta(seconds=20),
    )

    assert all(
        field.value is None
        and field.quality is ProfileValueQuality.STALE
        and field.available is False
        for field in (
            stale.pinv_w,
            stale.prec_w,
            stale.grid_signed_power_w,
            stale.soc_percent,
        )
    )
    assert stale.coherent_response_sequence == 41


def test_pv_contract_is_dc_input_and_has_no_solar_ac_alias():
    energy_profile = profile()
    pv_field = next(field for field in energy_profile.fields if field.name == "pv_power_w")

    assert pv_field.registers == (reg.I_PPV1, reg.I_PPV2, reg.I_PPV3)
    assert "DC/MPPT-side" in pv_field.reason
    assert not hasattr(energy_profile.snapshot(raw_snapshot({})), "solar_ac_power_w")
