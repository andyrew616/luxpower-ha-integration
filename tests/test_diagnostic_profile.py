"""Topology-neutral diagnostics preserve the qualified acquisition contract."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes
from custom_components.lxp_modbus.read_profiles import InputReadBlock
import luxpower.qualified as qualified
from luxpower.qualified import (
    AcquisitionHealth,
    DiagnosticReadProfile,
    DiagnosticSnapshot,
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    ProfileValueQuality,
    QualifiedLuxReadClient,
)


AT = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
ADDRESSES = frozenset(range(40)) | frozenset(range(80, 120))


def raw_snapshot(*, omit=(), sentinel=None):
    values = {register: 0 for register in ADDRESSES}
    values.update({5: 0x6342, 7: 350, 8: 240, 16: 900, 17: 100, 26: 80, 27: 20})
    if sentinel is not None:
        values[sentinel] = 0xFFFF
    for register in omit:
        values.pop(register)
    # An unrelated cached register must not escape the diagnostic profile.
    values[170] = 1234
    return LuxReadSessionSnapshot(
        input_registers=values,
        observed_at=LuxPowerObservationTimes(
            input_registers={register: AT for register in values}
        ),
        input_sources={register: LuxObservationSource.EXPLICIT for register in values},
        input_observation_sequences={
            register: 11 if register < 40 else 12 for register in values
        },
        input_observation_ranges={
            register: (0, 40) if register < 40 else (80, 40) for register in values
        },
    )


def test_diagnostic_plan_preserves_exact_qualified_12k_acquisition_requirements():
    diagnostic = DiagnosticReadProfile()
    original = EnergyFlowReadProfile(
        frozenset({1, 2, 3}), GridTopology.SINGLE_PHASE,
        LoadLayout.TWELVE_K_SINGLE_PHASE,
    )
    assert diagnostic.required_registers == original.required_registers == frozenset(
        {0, 5, 7, 8, 9, 10, 11, 24, 26, 27, 114}
    )
    assert diagnostic.read_blocks == original.read_blocks == (
        InputReadBlock(0, 40), InputReadBlock(80, 40),
    )
    for attribute in ("active_pv_strings", "grid_topology", "load_layout"):
        assert not hasattr(diagnostic, attribute)


def test_diagnostic_omits_unproven_aggregates_and_detaches_raw_registers():
    raw = raw_snapshot()
    snapshot = DiagnosticReadProfile().snapshot(raw)
    assert isinstance(snapshot, DiagnosticSnapshot)
    assert set(snapshot.registers) == ADDRESSES
    for attribute in ("pv_power_w", "load_power_w", "battery_power_w", "grid_power_w"):
        assert not hasattr(snapshot, attribute)
    with pytest.raises(TypeError):
        snapshot.registers[7] = snapshot.registers[8]
    raw.input_registers[7] = 9999
    assert snapshot.registers[7].value == 350


def test_diagnostic_retains_raw_sentinel_but_semantic_power_fails_closed():
    snapshot = DiagnosticReadProfile().snapshot(raw_snapshot(sentinel=16))
    assert snapshot.registers[16].value == 0xFFFF
    assert snapshot.registers[16].quality is ProfileValueQuality.AVAILABLE
    assert snapshot.direct_energy.pinv_w.value is None
    assert snapshot.direct_energy.pinv_w.quality is ProfileValueQuality.INVALID


def test_diagnostic_missing_values_are_not_zero_and_keep_response_provenance():
    snapshot = DiagnosticReadProfile().snapshot(raw_snapshot(omit=(7,)))
    assert snapshot.registers[7].value is None
    assert snapshot.registers[7].quality is ProfileValueQuality.MISSING
    value = snapshot.registers[114]
    assert value.value == 0
    assert value.observed_at == AT
    assert value.registers == (114,)
    assert value.sources == (LuxObservationSource.EXPLICIT,)
    assert value.observation_sequences == (12,)
    assert value.observation_ranges == ((80, 40),)


def test_diagnostic_direct_energy_matches_existing_per_device_contract():
    original = EnergyFlowReadProfile(
        frozenset({1, 2, 3}), GridTopology.SINGLE_PHASE,
        LoadLayout.TWELVE_K_SINGLE_PHASE,
    )
    snapshot = DiagnosticReadProfile().snapshot(raw_snapshot())
    assert snapshot.direct_energy == original.snapshot(raw_snapshot()).direct_energy
    assert snapshot.direct_energy.pinv_w.value == 900
    assert snapshot.direct_energy.prec_w.value == 100
    assert snapshot.direct_energy.grid_signed_power_w.value == 60
    assert snapshot.direct_energy.soc_percent.value == 66


@pytest.mark.parametrize("fault", ["source", "sequence", "range"])
def test_raw_diagnostics_require_accepted_response_provenance(fault):
    raw = raw_snapshot()
    if fault == "source":
        raw.input_sources.pop(7)
    elif fault == "sequence":
        raw.input_observation_sequences.pop(7)
    else:
        raw.input_observation_ranges[7] = (0, 20)
    value = DiagnosticReadProfile().snapshot(raw).registers[7]
    assert value.value is None
    assert not value.available


def test_incidental_register_expires_even_when_required_profile_is_fresh():
    raw = raw_snapshot()
    raw.observed_at.input_registers[18] = AT - timedelta(seconds=21)
    snapshot = DiagnosticReadProfile().snapshot(raw)
    assert snapshot.observed_at == AT
    inspected = snapshot.unavailable_if_stale(
        inspected_at=AT, freshness_target=timedelta(seconds=20),
    )
    assert inspected.registers[18].quality is ProfileValueQuality.STALE
    assert inspected.registers[18].value is None
    assert inspected.registers[7].value == 350


@pytest.fixture
def facade_boundary(monkeypatch):
    class Session:
        def __init__(self, *args, **kwargs):
            self.connection_args = args

    class Delegate:
        def __init__(self, *args, profile, session, **kwargs):
            self.profile = profile
            self.session = session
            self.acquisition_health = AcquisitionHealth.HEALTHY
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.release.set()

        async def async_connect(self):
            pass

        async def async_refresh_profile(self):
            self.entered.set()
            await self.release.wait()

        def profile_snapshot(self):
            return self.profile.snapshot(raw_snapshot())

        async def async_close(self):
            pass

    monkeypatch.setattr(qualified, "_LuxReadSession", Session)
    monkeypatch.setattr(qualified, "_LuxPowerHybridReadClient", Delegate)
    monkeypatch.setattr(qualified, "utc_now", lambda: AT)


def client(serial="TESTINV001"):
    return QualifiedLuxReadClient(
        "192.0.2.1", "TESTDONGLE", serial, profile=DiagnosticReadProfile()
    )


@pytest.mark.asyncio
async def test_facade_accepts_diagnostics_and_expires_raw_values_with_provenance(
    facade_boundary, monkeypatch,
):
    async with client() as reader:
        fresh = await reader.async_acquire()
        assert fresh.fresh
        assert fresh.profile.registers[7].value == 350
        monkeypatch.setattr(qualified, "utc_now", lambda: AT + timedelta(seconds=21))
        expired = reader.snapshot()
        assert not expired.fresh
        raw = expired.profile.registers[7]
        assert raw.value is None
        assert raw.quality is ProfileValueQuality.STALE
        assert raw.observed_at == AT
        assert raw.observation_sequences == (11,)
        assert raw.observation_ranges == ((0, 40),)
        assert expired.profile.direct_energy.pinv_w.quality is ProfileValueQuality.STALE
        # Inspection creates a detached result and does not mutate prior evidence.
        assert fresh.profile.registers[7].value == 350


@pytest.mark.asyncio
async def test_diagnostic_facades_have_independent_sessions_and_acquisition_locks(
    facade_boundary,
):
    async with client("TESTINV001") as first, client("TESTINV002") as second:
        assert first._delegate.session is not second._delegate.session
        first._delegate.release.clear()
        pending = asyncio.create_task(first.async_acquire())
        try:
            await asyncio.wait_for(first._delegate.entered.wait(), timeout=1)
            result = await asyncio.wait_for(second.async_acquire(), timeout=1)
            assert result.fresh
            assert not pending.done()
        finally:
            first._delegate.release.set()
            await pending
