"""Tests for the standalone experimental hybrid telemetry facade."""

import asyncio
from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess
import sys

import pytest

from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadSession,
    LuxReadSessionMetrics,
)
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes, utc_now
from custom_components.lxp_modbus.classes.read_session import LuxReadSessionSnapshot
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
)
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    GridTopology,
    InputReadBlock,
    LoadLayout,
)
from custom_components.lxp_modbus.exceptions import LuxPowerReadTimeoutError
from custom_components.lxp_modbus.exceptions import (
    LuxPowerConnectionError,
    LuxPowerConnectionLostError,
    LuxPowerReadRejectedError,
    LuxPowerRecoveryExhaustedError,
    LuxPowerSessionClosedError,
)
from custom_components.lxp_modbus.recovery import AcquisitionHealth, RecoveryPolicy
from luxpower.hybrid import (
    FULL_INPUT_READ_BLOCKS,
    OPERATIONAL_READ_BLOCKS,
    LuxPowerHybridReadClient,
    execute_live_validation,
)


def standard_profile():
    return EnergyFlowReadProfile(
        frozenset({1, 2, 3}),
        GridTopology.SINGLE_PHASE,
        LoadLayout.STANDARD,
    )


def test_hardware_proven_operational_and_full_read_plans():
    assert [(block.start, block.count) for block in OPERATIONAL_READ_BLOCKS] == [
        (0, 40),
        (40, 40),
        (80, 40),
        (120, 40),
        (160, 40),
        (200, 40),
    ]
    assert len(FULL_INPUT_READ_BLOCKS) == 19
    assert FULL_INPUT_READ_BLOCKS[-1].start == 720
    assert FULL_INPUT_READ_BLOCKS[-1].count == 30


def test_hybrid_and_session_public_interfaces_expose_no_write_operation():
    session = LuxReadSession(
        "192.0.2.1", "TESTDONGLE", "TESTINV001"
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", session=session
    )

    assert not any("write" in name for name in dir(session) if not name.startswith("_"))
    assert not any("write" in name for name in dir(client) if not name.startswith("_"))


def test_freshness_target_must_remain_positive():
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001"
    )

    with pytest.raises(ValueError):
        client.set_freshness_target(timedelta(0))


class FakeSession:
    def __init__(self, fresh_blocks=()):
        now = utc_now()
        self.values = {}
        self.observed = {}
        self.reads = []
        self.sources = {}
        self.explicit_observed = {}
        self.unsolicited_observed = {}
        for block in fresh_blocks:
            for register in block.addresses():
                if input_register_group(register) is TelemetryGroup.OPERATIONAL:
                    self.values[register] = register
                    self.observed[register] = now
                    self.sources[register] = LuxObservationSource.EXPLICIT
                    self.explicit_observed[register] = now

    def snapshot(self):
        return LuxReadSessionSnapshot(
            input_registers=dict(self.values),
            observed_at=LuxPowerObservationTimes(input_registers=dict(self.observed)),
            input_sources=dict(self.sources),
            explicit_observed_at=dict(self.explicit_observed),
            unsolicited_observed_at=dict(self.unsolicited_observed),
        )

    async def async_read_input(self, start, count):
        self.reads.append((start, count))
        now = utc_now()
        for register in range(start, start + count):
            self.values[register] = register
            self.observed[register] = now
            self.sources[register] = LuxObservationSource.EXPLICIT
            self.explicit_observed[register] = now

    def observe_unsolicited(self, block):
        now = utc_now()
        for register in block.addresses():
            self.values[register] = register
            self.observed[register] = now
            self.sources[register] = LuxObservationSource.UNSOLICITED
            self.unsolicited_observed[register] = now

    def observe_unsolicited_registers(self, registers):
        now = utc_now()
        for register in registers:
            self.values[register] = register
            self.observed[register] = now
            self.sources[register] = LuxObservationSource.UNSOLICITED
            self.unsolicited_observed[register] = now


@pytest.mark.asyncio
async def test_hybrid_requests_only_blocks_with_stale_operational_values():
    session = FakeSession(fresh_blocks=(OPERATIONAL_READ_BLOCKS[0],))
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", session=session
    )

    result = await client.async_refresh_operational()

    assert result.fresh_blocks_skipped == (OPERATIONAL_READ_BLOCKS[0],)
    assert session.reads == [
        (block.start, block.count) for block in OPERATIONAL_READ_BLOCKS[1:]
    ]


@pytest.mark.asyncio
async def test_profile_requests_only_blocks_with_stale_required_values():
    profile = standard_profile()
    session = FakeSession(fresh_blocks=(profile.read_blocks[0],))
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=session,
    )

    result = await client.async_refresh_profile()

    assert result.fresh_blocks_skipped == (profile.read_blocks[0],)
    assert session.reads == [(160, 40)]
    assert client.profile_snapshot().required_registers == profile.required_registers


@pytest.mark.asyncio
async def test_unsolicited_profile_block_avoids_one_request_without_double_credit():
    profile = standard_profile()
    session = FakeSession()
    session.observe_unsolicited(profile.read_blocks[0])
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=session,
    )

    first = await client.async_refresh_profile()
    second = await client.async_refresh_profile()

    assert first.blocks_satisfied_unsolicited == (profile.read_blocks[0],)
    assert second.blocks_satisfied_unsolicited == ()
    assert client.profile_metrics().explicit_requests_avoided_unsolicited == 1
    assert client.profile_metrics().explicit_requests_attempted == 1


@pytest.mark.asyncio
async def test_unrelated_unsolicited_block_cannot_satisfy_profile_block():
    profile = standard_profile()
    session = FakeSession()
    session.observe_unsolicited(InputReadBlock(40, 40))
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=session,
    )

    result = await client.async_refresh_profile()

    assert result.blocks_satisfied_unsolicited == ()
    assert session.reads == [(0, 40), (160, 40)]


@pytest.mark.asyncio
async def test_unsolicited_reception_does_not_claim_avoidance_before_read_was_due():
    profile = standard_profile()
    session = FakeSession(fresh_blocks=(profile.read_blocks[0],))
    session.observe_unsolicited(profile.read_blocks[0])
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=session,
    )

    result = await client.async_refresh_profile()

    assert result.blocks_satisfied_unsolicited == ()
    assert client.profile_metrics().explicit_requests_avoided_unsolicited == 0


@pytest.mark.asyncio
async def test_consecutive_unsolicited_frames_credit_one_request_opportunity():
    profile = standard_profile()
    session = FakeSession(fresh_blocks=(profile.read_blocks[0],))
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        freshness_target=timedelta(milliseconds=1),
        profile=profile,
        session=session,
    )
    await client.async_refresh_profile()
    await asyncio.sleep(0.003)
    session.observe_unsolicited(profile.read_blocks[0])
    session.observe_unsolicited(profile.read_blocks[0])

    await client.async_refresh_profile()
    await client.async_refresh_profile()

    assert client.profile_metrics().explicit_requests_avoided_unsolicited == 1


@pytest.mark.asyncio
async def test_mixed_source_refresh_credits_only_stale_register_displaced():
    profile = standard_profile()
    block = profile.read_blocks[0]
    session = FakeSession(fresh_blocks=(block,))
    oldest = min(profile.required_registers_in(block))
    almost_due = utc_now() - timedelta(milliseconds=40)
    session.observed[oldest] = almost_due
    session.explicit_observed[oldest] = almost_due
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        freshness_target=timedelta(milliseconds=50),
        profile=profile,
        session=session,
    )
    await client.async_refresh_profile()
    await asyncio.sleep(0.015)
    session.observe_unsolicited_registers((oldest,))

    result = await client.async_refresh_profile()

    assert result.blocks_satisfied_unsolicited == (block,)
    assert client.profile_metrics().explicit_requests_avoided_unsolicited == 1


@pytest.mark.asyncio
async def test_failed_profile_read_is_still_counted_as_an_attempt():
    class TimeoutSession(FakeSession):
        async def async_read_input(self, start, count):
            self.reads.append((start, count))
            raise LuxPowerReadTimeoutError("synthetic timeout")

    profile = standard_profile()
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=TimeoutSession(),
    )

    with pytest.raises(LuxPowerReadTimeoutError):
        await client.async_refresh_profile()

    assert client.profile_metrics().explicit_requests_attempted == 1
    assert client.last_profile_request_block == profile.read_blocks[0]


@pytest.mark.asyncio
async def test_profile_monitor_samples_through_final_in_flight_refresh():
    class SlowSession(FakeSession):
        async def async_read_input(self, start, count):
            await asyncio.sleep(0.03)
            await super().async_read_input(start, count)

    profile = standard_profile()
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=SlowSession(),
    )

    samples = await client.async_run_profile(0.01, sample_interval=0.002)
    elapsed = (
        datetime.fromisoformat(samples[-1]["at"])
        - datetime.fromisoformat(samples[0]["at"])
    ).total_seconds()

    assert elapsed >= 0.025
    assert samples[-1]["profile_freshness"]["known"] > 0


@pytest.mark.asyncio
async def test_experimental_full_scan_uses_only_the_proven_aligned_plan():
    session = FakeSession()
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", session=session
    )

    await client.async_full_scan()

    assert session.reads == [
        (block.start, block.count) for block in FULL_INPUT_READ_BLOCKS
    ]


@pytest.mark.asyncio
async def test_full_scan_does_not_receive_unsolicited_avoidance_credit():
    profile = standard_profile()
    session = FakeSession()
    client = LuxPowerHybridReadClient(
        "192.0.2.1",
        "TESTDONGLE",
        "TESTINV001",
        profile=profile,
        session=session,
    )

    await client.async_full_scan()
    result = await client.async_refresh_profile()

    assert result.requested_blocks == ()
    assert result.blocks_satisfied_unsolicited == ()
    assert client.profile_metrics().explicit_requests_attempted == 0
    assert client.profile_metrics().explicit_requests_avoided_unsolicited == 0


@pytest.mark.asyncio
async def test_explicit_routing_probe_forces_all_six_operational_blocks():
    session = FakeSession(fresh_blocks=OPERATIONAL_READ_BLOCKS)
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", session=session
    )

    result = await client.async_read_operational()

    assert result.requested_blocks == OPERATIONAL_READ_BLOCKS
    assert session.reads == [
        (block.start, block.count) for block in OPERATIONAL_READ_BLOCKS
    ]


@pytest.mark.asyncio
async def test_hybrid_scheduler_retains_full_scan_by_default():
    session = FakeSession()
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", session=session
    )

    await client.async_run_hybrid(0.01, sample_interval=0.001)

    assert session.reads[:19] == [
        (block.start, block.count) for block in FULL_INPUT_READ_BLOCKS
    ]


def zero_metrics(**changes):
    values = dict(
        connections=1,
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
    values.update(changes)
    return LuxReadSessionMetrics(**values)


class RecoverySession(FakeSession):
    """Small deterministic session double with no write surface."""

    def __init__(self, profile, *, fresh_blocks=(), failures=(), connect_failures=()):
        super().__init__()
        self.profile = profile
        self.failures = list(failures)
        self.connect_failures = list(connect_failures)
        self.connections = 1
        self.connect_calls = 0
        self.close_calls = 0
        for block in fresh_blocks:
            self._observe(block, LuxObservationSource.EXPLICIT)

    def _observe(self, block, source):
        now = utc_now()
        for register in block.addresses():
            self.values[register] = register
            self.observed[register] = now
            self.sources[register] = source
            if source is LuxObservationSource.EXPLICIT:
                self.explicit_observed[register] = now
            else:
                self.unsolicited_observed[register] = now

    async def async_read_input(self, start, count):
        self.reads.append((start, count))
        if self.failures:
            failure = self.failures.pop(0)
            if failure is not None:
                raise failure
        self._observe(InputReadBlock(start, count), LuxObservationSource.EXPLICIT)

    async def async_connect(self):
        self.connect_calls += 1
        if self.connect_failures:
            failure = self.connect_failures.pop(0)
            if failure is not None:
                raise failure
        self.connections += 1

    async def async_close(self):
        self.close_calls += 1

    def metrics(self):
        return zero_metrics(connections=self.connections)


def recovery_policy(**changes):
    values = dict(
        max_reconnects_per_acquisition=1,
        max_reconnects_per_window=2,
        rolling_window_seconds=300,
        initial_cooldown_seconds=0,
        repeated_cooldown_seconds=0,
    )
    values.update(changes)
    return RecoveryPolicy(**values)


@pytest.mark.asyncio
async def test_bounded_recovery_reacquires_only_stale_profile_block():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        fresh_blocks=(profile.read_blocks[0],),
        failures=(LuxPowerReadTimeoutError("synthetic"), None),
    )
    before = session.snapshot().observed_at.input_registers.copy()
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()

    result = await client.async_refresh_profile()

    assert session.reads == [(160, 40), (160, 40)]
    assert result.requested_blocks == (profile.read_blocks[1],)
    assert all(
        session.observed[register] == before[register]
        for register in profile.required_registers_in(profile.read_blocks[0])
    )
    metrics = client.recovery_metrics()
    assert metrics.health is AcquisitionHealth.HEALTHY
    assert metrics.timeout_count == 1
    assert metrics.successful_reconnects == 1
    assert metrics.completed_recoveries == 1
    assert metrics.events[0].outcome == "profile_recovered"


@pytest.mark.asyncio
async def test_repeated_failure_exhausts_per_acquisition_budget():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        failures=(
            LuxPowerReadTimeoutError("first"),
            LuxPowerReadTimeoutError("second"),
        ),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()

    with pytest.raises(LuxPowerRecoveryExhaustedError):
        await client.async_refresh_profile()

    metrics = client.recovery_metrics()
    assert metrics.reconnect_attempts == 1
    assert metrics.timeout_count == 2
    assert metrics.retry_budget_exhausted == 1
    assert metrics.acquisitions_abandoned == 1
    assert metrics.health is AcquisitionHealth.DEGRADED


@pytest.mark.asyncio
async def test_rolling_budget_prevents_reconnect_storm():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        failures=(LuxPowerReadTimeoutError("first"), None),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        freshness_target=timedelta(microseconds=1),
        profile=profile,
        session=session,
        recovery_policy=recovery_policy(max_reconnects_per_window=1),
    )
    await client.async_connect()
    await client.async_refresh_profile()
    await asyncio.sleep(0.002)
    session.failures = [LuxPowerReadTimeoutError("again")]

    with pytest.raises(LuxPowerRecoveryExhaustedError):
        await client.async_refresh_profile()

    assert session.connect_calls == 2  # initial client connect plus one recovery
    assert client.recovery_metrics().reconnect_attempts == 1


@pytest.mark.asyncio
async def test_shutdown_during_recovery_cooldown_never_reconnects():
    profile = standard_profile()
    session = RecoverySession(
        profile, failures=(LuxPowerReadTimeoutError("synthetic"),)
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile,
        session=session,
        recovery_policy=recovery_policy(initial_cooldown_seconds=1),
    )
    await client.async_connect()
    acquisition = asyncio.create_task(client.async_refresh_profile())
    while client.acquisition_health is not AcquisitionHealth.RECOVERING:
        await asyncio.sleep(0)
    await client.async_close()

    with pytest.raises(LuxPowerSessionClosedError):
        await acquisition
    assert session.connect_calls == 1


@pytest.mark.asyncio
async def test_cancellation_during_recovery_never_reconnects():
    profile = standard_profile()
    session = RecoverySession(
        profile, failures=(LuxPowerConnectionLostError("synthetic EOF"),)
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile,
        session=session,
        recovery_policy=recovery_policy(initial_cooldown_seconds=1),
    )
    await client.async_connect()
    acquisition = asyncio.create_task(client.async_refresh_profile())
    while client.acquisition_health is not AcquisitionHealth.RECOVERING:
        await asyncio.sleep(0)
    acquisition.cancel()

    with pytest.raises(asyncio.CancelledError):
        await acquisition
    assert session.connect_calls == 1
    assert client.recovery_metrics().events[0].outcome == "cancelled"


@pytest.mark.asyncio
async def test_cancellation_during_reacquisition_terminalizes_recovery():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        failures=(LuxPowerReadTimeoutError("first"), asyncio.CancelledError()),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()

    with pytest.raises(asyncio.CancelledError):
        await client.async_refresh_profile()

    metrics = client.recovery_metrics()
    assert metrics.events[-1].outcome == "reacquisition_cancelled"
    assert client._active_recovery is None


@pytest.mark.asyncio
async def test_modbus_rejection_during_reacquisition_terminalizes_without_retry():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        failures=(
            LuxPowerReadTimeoutError("first"),
            LuxPowerReadRejectedError("exception 3"),
        ),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()

    with pytest.raises(LuxPowerReadRejectedError):
        await client.async_refresh_profile()

    metrics = client.recovery_metrics()
    assert metrics.reconnect_attempts == 1
    assert metrics.retry_budget_exhausted == 0
    assert metrics.events[-1].outcome == "reacquisition_rejected"


@pytest.mark.asyncio
async def test_shutdown_during_reacquisition_is_not_budget_exhaustion():
    profile = standard_profile()

    class ShutdownDuringRetry(RecoverySession):
        def __init__(self):
            super().__init__(
                profile, failures=(LuxPowerReadTimeoutError("first"),)
            )
            self.retry_started = asyncio.Event()
            self.shutdown = asyncio.Event()

        async def async_read_input(self, start, count):
            if self.failures:
                return await super().async_read_input(start, count)
            self.reads.append((start, count))
            self.retry_started.set()
            await self.shutdown.wait()
            raise LuxPowerSessionClosedError("closed by test")

        async def async_close(self):
            await super().async_close()
            self.shutdown.set()

    session = ShutdownDuringRetry()
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()
    acquisition = asyncio.create_task(client.async_refresh_profile())
    await session.retry_started.wait()
    await client.async_close()

    with pytest.raises(LuxPowerSessionClosedError):
        await acquisition
    metrics = client.recovery_metrics()
    assert metrics.retry_budget_exhausted == 0
    assert metrics.events[-1].outcome == "reacquisition_shutdown"
    assert client._active_recovery is None


@pytest.mark.asyncio
async def test_eof_and_unsolicited_recovery_are_source_aware():
    profile = standard_profile()

    class UnsolicitedOnConnect(RecoverySession):
        async def async_connect(self):
            await super().async_connect()
            if self.connect_calls > 1:
                self._observe(profile.read_blocks[0], LuxObservationSource.UNSOLICITED)

    session = UnsolicitedOnConnect(
        profile, failures=(LuxPowerConnectionLostError("synthetic EOF"), None)
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=session, recovery_policy=recovery_policy(),
    )
    await client.async_connect()

    await client.async_refresh_profile()

    assert session.reads == [(0, 40), (160, 40)]
    assert client.recovery_metrics().connection_loss_count == 1


@pytest.mark.asyncio
async def test_failed_reconnect_and_modbus_rejection_are_not_blindly_retried():
    profile = standard_profile()
    failed_connect = RecoverySession(
        profile,
        failures=(LuxPowerReadTimeoutError("synthetic"),),
        connect_failures=(None, LuxPowerConnectionError("offline")),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=failed_connect, recovery_policy=recovery_policy(),
    )
    await client.async_connect()
    with pytest.raises(LuxPowerConnectionError):
        await client.async_refresh_profile()
    assert client.recovery_metrics().failed_reconnects == 1

    rejected = RecoverySession(
        profile, failures=(LuxPowerReadRejectedError("exception 3"),)
    )
    rejected_client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        profile=profile, session=rejected, recovery_policy=recovery_policy(),
    )
    await rejected_client.async_connect()
    with pytest.raises(LuxPowerReadRejectedError):
        await rejected_client.async_refresh_profile()
    assert rejected_client.recovery_metrics().reconnect_attempts == 0


@pytest.mark.asyncio
async def test_repeated_recovery_uses_configured_cooldown_without_unbounded_retry():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        failures=(LuxPowerReadTimeoutError("first"), None, None),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        freshness_target=timedelta(microseconds=1),
        profile=profile,
        session=session,
        recovery_policy=recovery_policy(
            initial_cooldown_seconds=0.001,
            repeated_cooldown_seconds=0.003,
        ),
    )
    await client.async_connect()
    await client.async_refresh_profile()
    await asyncio.sleep(0.002)
    session.failures = [LuxPowerReadTimeoutError("second"), None, None]
    await client.async_refresh_profile()

    events = client.recovery_metrics().events
    assert [event.cooldown_seconds for event in events] == [0.001, 0.003]
    assert client.recovery_metrics().reconnect_attempts == 2


@pytest.mark.asyncio
async def test_recovery_restarts_selection_and_reacquires_earlier_block_if_stale():
    profile = standard_profile()
    session = RecoverySession(
        profile,
        fresh_blocks=(profile.read_blocks[0],),
        failures=(LuxPowerReadTimeoutError("later block"), None, None),
    )
    client = LuxPowerHybridReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001",
        freshness_target=timedelta(milliseconds=1),
        profile=profile,
        session=session,
        recovery_policy=recovery_policy(initial_cooldown_seconds=0.003),
    )
    await client.async_connect()

    await client.async_refresh_profile()

    assert session.reads == [(160, 40), (0, 40), (160, 40)]
    assert client.acquisition_health is AcquisitionHealth.HEALTHY
    assert client.recovery_metrics().events[0].outcome == "profile_recovered"


@pytest.mark.asyncio
async def test_live_progression_stops_after_strict_five_second_miss_and_is_sanitized():
    class FakeValidationClient:
        def __init__(self):
            self.metric = zero_metrics()
            self.values = {}
            self.observed = {}
            self.targets = []

        async def async_connect(self):
            return None

        async def async_close(self):
            return None

        async def async_passive(self, _seconds):
            return None

        def drain_observations(self):
            return ()

        def metrics(self):
            return self.metric

        def snapshot(self):
            return LuxReadSessionSnapshot(
                input_registers=dict(self.values),
                observed_at=LuxPowerObservationTimes(input_registers=dict(self.observed)),
            )

        async def async_read_operational(self):
            from luxpower.hybrid import HybridRefreshResult
            self.metric = zero_metrics(
                explicit_requests=6,
                expected_fc4_responses=6,
                validated_fc4_frames=6,
                operational_registers_expected=89,
                request_latencies_ms=(1.0,) * 6,
                request_latency_samples_total=6,
            )
            return HybridRefreshResult(OPERATIONAL_READ_BLOCKS, (), 6.0)

        async def async_full_scan(self):
            now = utc_now()
            self.values = {register: register for register in range(750)}
            self.observed = {register: now for register in range(750)}
            self.metric = zero_metrics(
                explicit_requests=25,
                expected_fc4_responses=25,
                validated_fc4_frames=25,
                operational_registers_expected=178,
                request_latencies_ms=(1.0,) * 25,
                request_latency_samples_total=25,
            )
            return self.snapshot()

        def set_freshness_target(self, target):
            self.targets.append(target.total_seconds())

        async def async_run_hybrid(self, _duration, **_kwargs):
            self.metric = zero_metrics(
                explicit_requests=26,
                expected_fc4_responses=26,
                validated_fc4_frames=27,
                unmatched_fc4_observations=1,
                operational_registers_expected=193,
                operational_registers_unmatched=15,
                request_latencies_ms=(1.0,) * 26,
                request_latency_samples_total=26,
            )
            return [{
                "at": utc_now().isoformat(),
                "operational_freshness": {
                    "known": 89,
                    "required": 89,
                    "median_age_seconds": 2.0,
                    "max_age_seconds": 5.001,
                },
            }]

    client = FakeValidationClient()
    result = await execute_live_validation(
        client,
        passive_seconds=0.01,
        hybrid_targets=(5, 3, 2),
        hybrid_seconds=0.01,
    )

    assert client.targets == [5]
    assert [phase["name"] for phase in result["phases"]] == [
        "passive", "explicit_operational", "frame_aware_full_scan", "hybrid"
    ]
    hybrid = result["phases"][-1]
    assert hybrid["target_met"] is False
    assert hybrid["operational_receptions_by_route"]["unsolicited"] == 15
    serialized = __import__("json").dumps(result)
    assert "192.0.2.1" not in serialized
    assert "TESTDONGLE" not in serialized
    assert "TESTINV001" not in serialized


def test_frame_aware_public_import_remains_home_assistant_independent():
    repository = Path(__file__).resolve().parents[1]
    script = """
import builtins
import sys
real_import = builtins.__import__
def reject_home_assistant(name, *args, **kwargs):
    if name == 'homeassistant' or name.startswith('homeassistant.'):
        raise AssertionError(f'frame-aware import reached {name}')
    return real_import(name, *args, **kwargs)
builtins.__import__ = reject_home_assistant
from luxpower import LuxReadSession
from luxpower.hybrid import LuxPowerHybridReadClient, OPERATIONAL_READ_BLOCKS
assert len(OPERATIONAL_READ_BLOCKS) == 6
assert not hasattr(LuxReadSession, 'async_write_register')
assert not hasattr(LuxPowerHybridReadClient, 'async_write_register')
assert not any(name == 'homeassistant' or name.startswith('homeassistant.') for name in sys.modules)
"""
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [sys.executable, "-S", "-c", script],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
