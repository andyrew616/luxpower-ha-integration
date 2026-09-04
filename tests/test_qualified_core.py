"""Contract tests for the supported qualified Lux read-core boundary."""

import asyncio
import os
from pathlib import Path
import subprocess
import sys

import pytest

from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadSessionMetrics,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.exceptions import LuxPowerConnectionError
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes, utc_now
from custom_components.lxp_modbus.recovery import AcquisitionHealth, RecoveryMetrics
import luxpower.qualified as qualified
from luxpower.qualified import (
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    LuxPowerSessionClosedError,
    QualifiedLuxReadClient,
    RecoveryPolicy,
)


EXPECTED_PUBLIC_NAMES = {
    "AcquisitionHealth",
    "EnergyFlowReadProfile",
    "EnergyFlowSnapshot",
    "GridTopology",
    "HybridProfileMetrics",
    "LoadLayout",
    "LuxPowerCommunicationError",
    "LuxPowerConnectionError",
    "LuxPowerReadRejectedError",
    "LuxPowerRecoveryExhaustedError",
    "LuxPowerSessionClosedError",
    "LuxReadSessionMetrics",
    "ObservedProfileValue",
    "ProfileField",
    "QualifiedLuxReadClient",
    "QualifiedLuxSnapshot",
    "RecoveryMetrics",
    "RecoveryPolicy",
}


def standard_profile():
    return EnergyFlowReadProfile(
        frozenset({1, 2, 3}),
        GridTopology.SINGLE_PHASE,
        LoadLayout.STANDARD,
    )


def empty_transport_metrics():
    return LuxReadSessionMetrics(
        connections=0,
        reconnects=0,
        bytes_received=0,
        frames_received=0,
        validated_fc4_frames=0,
        expected_fc4_responses=0,
        unmatched_fc4_observations=0,
        duplicate_fc4_frames=0,
        invalid_frames=0,
        function_193_frames=0,
        explicit_requests=0,
        request_timeouts=0,
        connection_losses=0,
        operational_registers_expected=0,
        operational_registers_unmatched=0,
        observation_queue_drops=0,
        request_latencies_ms=(),
        request_latency_samples_total=0,
        decoder_discarded_bytes=0,
        decoder_buffered_bytes=0,
    )


def empty_recovery_metrics(health):
    return RecoveryMetrics(
        health=health,
        timeout_count=0,
        connection_loss_count=0,
        connection_establishment_failure_count=0,
        ambiguous_request_count=0,
        reconnect_attempts=0,
        successful_reconnects=0,
        failed_reconnects=0,
        completed_recoveries=0,
        retry_budget_exhausted=0,
        acquisitions_abandoned=0,
        connection_generations_created=0,
    )


class FakeSession:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeDelegate:
    instances = []
    fail_connect = False
    connect_gate = None
    refresh_gate = None
    close_gate = None

    def __init__(self, *args, profile, session, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.profile = profile
        self.session = session
        self.connect_calls = 0
        self.refresh_calls = 0
        self.close_calls = 0
        self.acquisition_health = AcquisitionHealth.DEGRADED
        self.__class__.instances.append(self)

    async def async_connect(self):
        self.connect_calls += 1
        if self.connect_gate is not None:
            await self.connect_gate.wait()
        if self.fail_connect:
            raise LuxPowerConnectionError("synthetic connection failure")

    async def async_refresh_profile(self):
        self.refresh_calls += 1
        if self.refresh_gate is not None:
            await self.refresh_gate.wait()
        self.acquisition_health = AcquisitionHealth.HEALTHY

    def profile_snapshot(self):
        observed_at = utc_now()
        values = {register: 0 for register in self.profile.required_registers}
        times = {
            register: observed_at for register in self.profile.required_registers
        }
        sources = {
            register: LuxObservationSource.EXPLICIT
            for register in self.profile.required_registers
        }
        return self.profile.snapshot(
            LuxReadSessionSnapshot(
                input_registers=values,
                observed_at=LuxPowerObservationTimes(input_registers=times),
                input_sources=sources,
                explicit_observed_at=times,
            )
        )

    def metrics(self):
        return empty_transport_metrics()

    def profile_metrics(self):
        return qualified.HybridProfileMetrics(0, 0, 0)

    def recovery_metrics(self):
        return empty_recovery_metrics(self.acquisition_health)

    async def async_close(self):
        self.close_calls += 1
        if self.close_gate is not None:
            await self.close_gate.wait()
        self.acquisition_health = AcquisitionHealth.DEGRADED


@pytest.fixture
def fake_boundary(monkeypatch):
    FakeDelegate.instances = []
    FakeDelegate.fail_connect = False
    FakeDelegate.connect_gate = None
    FakeDelegate.refresh_gate = None
    FakeDelegate.close_gate = None
    monkeypatch.setattr(qualified, "_LuxReadSession", FakeSession)
    monkeypatch.setattr(qualified, "_LuxPowerHybridReadClient", FakeDelegate)
    return FakeDelegate


def new_client():
    return QualifiedLuxReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=standard_profile(),
        recovery_policy=RecoveryPolicy(),
    )


def test_supported_public_surface_is_narrow_and_read_only():
    assert set(qualified.__all__) == EXPECTED_PUBLIC_NAMES
    assert "LuxReadSession" not in qualified.__all__
    assert "LuxPowerHybridReadClient" not in qualified.__all__

    forbidden = {
        "async_full_scan",
        "async_read_input",
        "async_read_operational",
        "async_read_profile",
        "async_run_hybrid",
        "async_run_profile",
        "set_freshness_target",
        "subscribe_observations",
    }
    assert not forbidden.intersection(dir(QualifiedLuxReadClient))
    assert not any(
        "write" in name
        for name in dir(QualifiedLuxReadClient)
        if not name.startswith("_")
    )


def test_supported_import_does_not_load_home_assistant_or_tooling():
    repository = Path(__file__).resolve().parents[1]
    script = """
import builtins
import sys
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'homeassistant' or name.startswith('homeassistant.'):
        raise AssertionError(name)
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from luxpower.qualified import QualifiedLuxReadClient
assert 'luxpower.profile_validation' not in sys.modules
assert 'luxpower.benchmark' not in sys.modules
assert 'luxpower.fc4_matrix' not in sys.modules
assert 'custom_components.lxp_modbus.classes.read_client' not in sys.modules
assert 'custom_components.lxp_modbus.classes.modbus_client' not in sys.modules
assert not any(name == 'homeassistant' or name.startswith('homeassistant.') for name in sys.modules)
assert not hasattr(QualifiedLuxReadClient, 'async_read_input')
"""
    result = subprocess.run(
        [sys.executable, "-S", "-c", script],
        cwd=repository,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_historic_root_export_resolves_lazily():
    script = """
import sys
import luxpower
assert 'custom_components.lxp_modbus.classes.read_client' not in sys.modules
from luxpower import LuxPowerReadClient
assert LuxPowerReadClient.__name__ == 'LuxPowerReadClient'
assert 'custom_components.lxp_modbus.classes.read_client' in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-S", "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_facade_uses_qualified_settings_and_delegates_lifecycle_once(fake_boundary):
    client = new_client()
    delegate = fake_boundary.instances[0]

    assert delegate.session.kwargs["drain_timeout"] == 3.0
    assert delegate.session.kwargs["reply_timeout"] == 10.0
    assert delegate.kwargs["freshness_target"].total_seconds() == 20

    with pytest.raises(LuxPowerSessionClosedError):
        await client.async_acquire()
    with pytest.raises(LuxPowerSessionClosedError):
        client.snapshot()

    await client.async_start()
    await client.async_start()
    snapshot = await client.async_acquire()

    assert delegate.connect_calls == 1
    assert delegate.refresh_calls == 1
    assert snapshot.api_version == 1
    assert snapshot.profile.observed_at is not None
    assert snapshot.fresh is True
    assert snapshot.acquisition_health is AcquisitionHealth.HEALTHY
    assert snapshot.field_definitions == client.profile.fields
    assert client.transport_metrics() == empty_transport_metrics()
    assert client.recovery_metrics().health is AcquisitionHealth.HEALTHY

    await client.async_close()
    await client.async_close()
    assert delegate.close_calls == 1
    with pytest.raises(LuxPowerSessionClosedError):
        client.snapshot()


@pytest.mark.asyncio
async def test_failed_start_is_not_retried_or_marked_started(fake_boundary):
    fake_boundary.fail_connect = True
    client = new_client()
    delegate = fake_boundary.instances[0]

    with pytest.raises(LuxPowerConnectionError):
        await client.async_start()

    assert delegate.connect_calls == 1
    with pytest.raises(LuxPowerSessionClosedError):
        client.snapshot()
    await client.async_close()
    assert delegate.close_calls == 1


@pytest.mark.asyncio
async def test_async_context_manager_owns_start_and_close(fake_boundary):
    client = new_client()
    delegate = fake_boundary.instances[0]

    async with client as active:
        assert active is client
        assert (await active.async_acquire()).fresh is True

    assert delegate.connect_calls == 1
    assert delegate.refresh_calls == 1
    assert delegate.close_calls == 1


@pytest.mark.asyncio
async def test_concurrent_start_and_close_leave_client_stopped(fake_boundary):
    fake_boundary.connect_gate = asyncio.Event()
    client = new_client()
    delegate = fake_boundary.instances[0]

    start = asyncio.create_task(client.async_start())
    await asyncio.sleep(0)
    close = asyncio.create_task(client.async_close())
    await asyncio.sleep(0)

    assert delegate.connect_calls == 1
    assert delegate.close_calls == 0
    fake_boundary.connect_gate.set()
    await asyncio.gather(start, close)

    assert delegate.close_calls == 1
    with pytest.raises(LuxPowerSessionClosedError):
        client.snapshot()


@pytest.mark.asyncio
async def test_concurrent_closes_share_one_completed_close(fake_boundary):
    client = new_client()
    delegate = fake_boundary.instances[0]
    await client.async_start()
    fake_boundary.close_gate = asyncio.Event()

    first = asyncio.create_task(client.async_close())
    await asyncio.sleep(0)
    second = asyncio.create_task(client.async_close())
    await asyncio.sleep(0)

    assert delegate.close_calls == 1
    assert not second.done()
    fake_boundary.close_gate.set()
    await asyncio.gather(first, second)
    assert delegate.close_calls == 1


@pytest.mark.asyncio
async def test_close_drains_in_flight_acquire_before_restart(fake_boundary):
    client = new_client()
    delegate = fake_boundary.instances[0]
    await client.async_start()
    fake_boundary.refresh_gate = asyncio.Event()

    acquire = asyncio.create_task(client.async_acquire())
    await asyncio.sleep(0)
    close = asyncio.create_task(client.async_close())
    await asyncio.sleep(0)
    restart = asyncio.create_task(client.async_start())
    await asyncio.sleep(0)

    assert delegate.refresh_calls == 1
    assert delegate.close_calls == 1
    assert delegate.connect_calls == 1
    fake_boundary.refresh_gate.set()

    with pytest.raises(LuxPowerSessionClosedError):
        await acquire
    await close
    await restart
    assert delegate.connect_calls == 2
    assert client.snapshot().fresh is True

    await client.async_close()
    assert delegate.close_calls == 2


def test_qualification_harness_uses_the_same_implementation_class():
    from luxpower import profile_validation
    from luxpower.hybrid import LuxPowerHybridReadClient

    assert qualified._QualificationLuxReadClient is LuxPowerHybridReadClient
    assert profile_validation.LuxPowerHybridReadClient is LuxPowerHybridReadClient
