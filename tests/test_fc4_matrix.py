"""Tests for the isolated FC4 order and pacing matrix."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from custom_components.lxp_modbus.classes.read_session import LuxReadSession
from luxpower.fc4_matrix import (
    MATRIX_CELLS,
    counterbalanced_sequence,
    execute_fc4_matrix,
)
from test_frame_aware_session import (
    DONGLE,
    INVERTER,
    FakeWriter,
    QueueReader,
    input_response,
)
from test_data import FUNCTION_193_MESSAGE


class FakeMonotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    async def sleep(self, delay: float) -> None:
        self.value += delay


def responding_session_factory(clock, sessions, requested_starts):
    def factory():
        reader = QueueReader()

        def on_write(packet):
            assert packet[21] == 4
            start = int.from_bytes(packet[32:34], "little")
            count = int.from_bytes(packet[34:36], "little")
            requested_starts.append(start)
            reader.feed(input_response(start, count))

        writer = FakeWriter(on_write)

        async def connector(_host, _port):
            return reader, writer

        session = LuxReadSession(
            "192.0.2.1",
            DONGLE.decode(),
            INVERTER.decode(),
            connector=connector,
            monotonic=clock,
            diagnostic_monotonic=clock,
        )
        sessions.append((session, writer))
        return session

    return factory


def test_counterbalanced_sequence_is_deterministic_and_balanced():
    sequence = counterbalanced_sequence(10)

    assert len(sequence) == 40
    assert sequence[:8] == (
        "A1",
        "B2",
        "A2",
        "B1",
        "B1",
        "A2",
        "B2",
        "A1",
    )
    assert {cell: sequence.count(cell) for cell in MATRIX_CELLS} == {
        "A1": 10,
        "A2": 10,
        "B1": 10,
        "B2": 10,
    }
    with pytest.raises(ValueError):
        counterbalanced_sequence(0)


@pytest.mark.asyncio
async def test_matrix_reverses_blocks_applies_only_quiet_gaps_and_uses_fresh_sessions():
    clock = FakeMonotonic()
    sessions = []
    requested_starts = []
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)
        await clock.sleep(delay)

    report = await execute_fc4_matrix(
        responding_session_factory(clock, sessions, requested_starts),
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0,
        sleep=sleep,
        monotonic=clock,
        implementation_revision="a" * 40,
        implementation_revision_verified=True,
    )

    assert report["configuration"]["run_sequence"] == ["A1", "B2", "A2", "B1"]
    assert requested_starts == [0, 80, 80, 0, 0, 80, 80, 0]
    assert sleeps == [1.0, 1.0]
    assert len(sessions) == 4
    assert all(writer.closed for _session, writer in sessions)
    assert all(repetition["connection_generations_created"] == 1 for repetition in report["repetitions"])
    assert all(repetition["completed"] for repetition in report["repetitions"])
    assert report["analysis"]["per_cell"]["A1"]["actual_second_request_gap_seconds"]["max"] == 0.0
    assert report["analysis"]["per_cell"]["A2"]["actual_second_request_gap_seconds"]["min"] == 1.0


@pytest.mark.asyncio
async def test_fixed_between_repetition_cooldown_is_separate_from_quiet_period():
    clock = FakeMonotonic()
    sessions = []
    requested_starts = []
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)
        await clock.sleep(delay)

    await execute_fc4_matrix(
        responding_session_factory(clock, sessions, requested_starts),
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0.25,
        sleep=sleep,
        monotonic=clock,
        implementation_revision="b" * 40,
        implementation_revision_verified=True,
    )

    assert sleeps.count(1.0) == 2
    assert sleeps.count(0.25) == 3


@pytest.mark.asyncio
async def test_timeout_terminates_only_repetition_without_recovery_or_request_skip():
    clock = FakeMonotonic()
    sessions = []
    requested_starts = []
    factory_calls = 0

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        reader = QueueReader()

        def on_write(packet):
            start = int.from_bytes(packet[32:34], "little")
            count = int.from_bytes(packet[34:36], "little")
            requested_starts.append(start)
            # The first repetition's second read intentionally receives no reply.
            if factory_calls != 1 or len(requested_starts) == 1:
                reader.feed(input_response(start, count))

        writer = FakeWriter(on_write)

        async def connector(_host, _port):
            return reader, writer

        session = LuxReadSession(
            "192.0.2.1",
            DONGLE.decode(),
            INVERTER.decode(),
            connector=connector,
            request_timeout=0.01,
            monotonic=clock,
            diagnostic_monotonic=clock,
        )
        sessions.append(session)
        return session

    report = await execute_fc4_matrix(
        factory,
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0,
        monotonic=clock,
        implementation_revision="c" * 40,
        implementation_revision_verified=True,
    )

    assert len(report["repetitions"]) == 4
    assert report["repetitions"][0]["close_reason"] == "request_2_failure"
    assert report["repetitions"][0]["requests"][1]["outcome"] == "response_timeout"
    assert report["repetitions"][1]["completed"] is True
    assert report["safety"]["recovery_enabled"] is False
    assert report["safety"]["unsolicited_may_skip_planned_request"] is False
    assert report["configuration"]["request_timeout_seconds"] == 0.01
    assert len(sessions) == 4


@pytest.mark.asyncio
async def test_five_consecutive_timeout_repetitions_stop_extreme_instability():
    clock = FakeMonotonic()
    sessions = []

    def factory():
        reader = QueueReader()
        writer = FakeWriter()

        async def connector(_host, _port):
            return reader, writer

        session = LuxReadSession(
            "192.0.2.1",
            DONGLE.decode(),
            INVERTER.decode(),
            connector=connector,
            request_timeout=0.005,
            monotonic=clock,
            diagnostic_monotonic=clock,
        )
        sessions.append(session)
        return session

    report = await execute_fc4_matrix(
        factory,
        repetitions_per_cell=10,
        between_repetition_cooldown_seconds=0,
        sleep=clock.sleep,
        monotonic=clock,
        implementation_revision="1" * 40,
        implementation_revision_verified=True,
    )

    assert report["aborted"] is True
    assert report["abort_reason"] == "five_consecutive_timeout_repetitions"
    assert len(report["repetitions"]) == 5
    assert all(len(repetition["requests"]) == 1 for repetition in report["repetitions"])
    assert all(
        repetition["requests"][0]["outcome"] == "response_timeout"
        for repetition in report["repetitions"]
    )
    assert len(sessions) == 5


@pytest.mark.asyncio
async def test_unsolicited_fc4_and_fc193_cannot_skip_or_complete_planned_reads():
    clock = FakeMonotonic()
    requested_starts = []

    def factory():
        reader = QueueReader()

        def on_write(packet):
            start = int.from_bytes(packet[32:34], "little")
            count = int.from_bytes(packet[34:36], "little")
            requested_starts.append(start)
            unrelated = 80 if start == 0 else 0
            reader.feed(bytes.fromhex(FUNCTION_193_MESSAGE))
            reader.feed(input_response(unrelated, count) + input_response(start, count))

        writer = FakeWriter(on_write)

        async def connector(_host, _port):
            return reader, writer

        return LuxReadSession(
            "192.0.2.1",
            DONGLE.decode(),
            INVERTER.decode(),
            connector=connector,
            monotonic=clock,
            diagnostic_monotonic=clock,
        )

    report = await execute_fc4_matrix(
        factory,
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0,
        sleep=clock.sleep,
        monotonic=clock,
        implementation_revision="e" * 40,
        implementation_revision_verified=True,
    )

    assert requested_starts == [0, 80, 80, 0, 0, 80, 80, 0]
    assert all(repetition["completed"] for repetition in report["repetitions"])
    assert all(
        repetition["session_metrics"]["explicit_requests"] == 2
        for repetition in report["repetitions"]
    )
    assert sum(
        repetition["session_metrics"]["unmatched_fc4_observations"]
        for repetition in report["repetitions"]
    ) == 8
    assert sum(
        repetition["session_metrics"]["function_193_frames"]
        for repetition in report["repetitions"]
    ) == 8


@pytest.mark.asyncio
async def test_invalid_frame_aborts_after_safely_closing_current_repetition():
    clock = FakeMonotonic()
    sessions = []

    def factory():
        reader = QueueReader()

        def on_write(packet):
            start = int.from_bytes(packet[32:34], "little")
            count = int.from_bytes(packet[34:36], "little")
            invalid = bytearray(input_response(start, count))
            invalid[-1] ^= 1
            reader.feed(bytes(invalid) + input_response(start, count))

        writer = FakeWriter(on_write)

        async def connector(_host, _port):
            return reader, writer

        session = LuxReadSession(
            "192.0.2.1",
            DONGLE.decode(),
            INVERTER.decode(),
            connector=connector,
            monotonic=clock,
            diagnostic_monotonic=clock,
        )
        sessions.append((session, writer))
        return session

    report = await execute_fc4_matrix(
        factory,
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0,
        sleep=clock.sleep,
        monotonic=clock,
        implementation_revision="f" * 40,
        implementation_revision_verified=True,
    )

    assert report["aborted"] is True
    assert report["abort_reason"] == "invalid_frame_observed"
    assert len(report["repetitions"]) == 1
    assert report["repetitions"][0]["completed"] is False
    assert report["repetitions"][0]["close_reason"] == "protocol_safety_stop"
    assert len(report["repetitions"][0]["requests"]) == 1
    assert sessions[0][1].closed is True


@pytest.mark.asyncio
async def test_matrix_aggregation_and_artifact_are_sanitized():
    clock = FakeMonotonic()
    sessions = []
    requested_starts = []
    report = await execute_fc4_matrix(
        responding_session_factory(clock, sessions, requested_starts),
        repetitions_per_cell=1,
        between_repetition_cooldown_seconds=0,
        sleep=clock.sleep,
        monotonic=clock,
        implementation_revision="d" * 40,
        implementation_revision_verified=True,
    )

    assert report["analysis"]["by_block"]["0"]["attempts"] == 4
    assert report["analysis"]["by_block"]["80"]["attempts"] == 4
    assert report["analysis"]["by_ordinal_position"]["1"]["attempts"] == 4
    assert report["analysis"]["by_ordinal_position"]["2"]["attempts"] == 4
    assert report["analysis"]["by_pacing"]["immediate"]["attempts"] == 2
    assert report["analysis"]["by_pacing"]["quiet"]["attempts"] == 2
    assert report["analysis"]["by_cell_pacing_assignment"]["immediate"]["attempts"] == 4
    assert report["analysis"]["by_cell_pacing_assignment"]["quiet"]["attempts"] == 4
    assert all(
        repetition["requests"][0]["pacing_exposure"] == "not_exposed"
        for repetition in report["repetitions"]
    )
    serialized = json.dumps(report)
    assert "192.0.2.1" not in serialized
    assert DONGLE.decode() not in serialized
    assert INVERTER.decode() not in serialized
    assert "register_values" not in serialized
    assert "raw_packet" not in serialized


def test_matrix_import_is_home_assistant_independent_and_exposes_no_write_surface():
    script = """
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'homeassistant' or name.startswith('homeassistant.'):
        raise AssertionError(name)
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
import luxpower.fc4_matrix as matrix
assert matrix.FIRST_BLOCK.start == 0
assert matrix.SECOND_BLOCK.start == 80
assert not any('write_register' in name for name in dir(matrix))
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
