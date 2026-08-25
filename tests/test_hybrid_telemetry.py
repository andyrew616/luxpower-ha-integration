"""Tests for the standalone experimental hybrid telemetry facade."""

from datetime import timedelta
import os
from pathlib import Path
import subprocess
import sys

import pytest

from custom_components.lxp_modbus.classes.read_session import (
    LuxReadSession,
    LuxReadSessionMetrics,
)
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes, utc_now
from custom_components.lxp_modbus.classes.read_session import LuxReadSessionSnapshot
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
)
from luxpower.hybrid import (
    FULL_INPUT_READ_BLOCKS,
    OPERATIONAL_READ_BLOCKS,
    LuxPowerHybridReadClient,
    execute_live_validation,
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
        for block in fresh_blocks:
            for register in block.addresses():
                if input_register_group(register) is TelemetryGroup.OPERATIONAL:
                    self.values[register] = register
                    self.observed[register] = now

    def snapshot(self):
        return LuxReadSessionSnapshot(
            input_registers=dict(self.values),
            observed_at=LuxPowerObservationTimes(input_registers=dict(self.observed)),
        )

    async def async_read_input(self, start, count):
        self.reads.append((start, count))
        now = utc_now()
        for register in range(start, start + count):
            self.values[register] = register
            self.observed[register] = now


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
