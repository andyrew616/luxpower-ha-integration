"""Synthetic tests for the strictly read-only hardware benchmark."""

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import luxpower.benchmark as benchmark_module
from custom_components.lxp_modbus.classes.lxp_request_builder import (
    LxpRequestBuilder,
)
from custom_components.lxp_modbus.classes.lxp_packet_utils import LxpPacketUtils
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_registers_for_group,
)
from luxpower.benchmark import (
    BENCHMARK_SCHEMA_VERSION,
    BenchmarkSafetyError,
    BenchmarkTarget,
    CycleMetric,
    FULL_INPUT_SHAPE,
    InitialCapture,
    ObservationTracker,
    OPERATIONAL_INPUT_SHAPE,
    READ_INPUT_FUNCTION_CODE,
    ReadOnlyBenchmarkClient,
    ReadRange,
    ReadShape,
    RequestMetric,
    _assert_read_only_packet,
    _record_effective_observation_intervals,
    analyse_payload,
    compare_read_shapes,
    describe_shape,
    execute_benchmark,
    percentile_nearest_rank,
    split_lux_frames,
    summarize_cycles,
    summarize_durations,
)
from test_data import FUNCTION_193_MESSAGE


TARGET = BenchmarkTarget(
    host="192.0.2.1",
    port=8000,
    dongle_serial="TESTDONGLE",
    inverter_serial="TESTINV001",
)


def synthetic_input_response(
    start: int = 0,
    count: int = 125,
    function_code: int = READ_INPUT_FUNCTION_CODE,
) -> bytes:
    """Build a CRC-valid protocol-5 response without real hardware identifiers."""
    values = b"".join(value.to_bytes(2, "little") for value in range(count))
    data_frame = (
        bytes([1, function_code])
        + TARGET.inverter_serial.encode()
        + start.to_bytes(2, "little")
        + len(values).to_bytes(1, "little")
        + values
    )
    crc = LxpPacketUtils.compute_crc(data_frame).to_bytes(2, "little")
    body = (
        bytes([1, LxpRequestBuilder.TRANSLATED_DATA])
        + TARGET.dongle_serial.encode()
        + (len(data_frame) + len(crc)).to_bytes(2, "little")
        + data_frame
        + crc
    )
    return (
        LxpRequestBuilder.PREFIX
        + (5).to_bytes(2, "little")
        + len(body).to_bytes(2, "little")
        + body
    )


def test_outgoing_safety_guard_accepts_only_input_reads():
    """Function-code 6 is rejected before a packet can reach a socket."""
    read_packet = LxpRequestBuilder.prepare_packet_for_read(
        b"TESTDONGLE", b"TESTINV001", 0, 125, READ_INPUT_FUNCTION_CODE
    )
    write_packet = LxpRequestBuilder.prepare_packet_for_write(
        b"TESTDONGLE", b"TESTINV001", 21, 1
    )

    _assert_read_only_packet(read_packet, TARGET, ReadRange(0, 125))
    with pytest.raises(BenchmarkSafetyError, match="unexpected device function"):
        _assert_read_only_packet(write_packet, TARGET, ReadRange(21, 1))


@pytest.mark.parametrize("byte_index", [2, 4, 6, 7, 8, 18, 20, 22, 32, 34, 36])
def test_outgoing_safety_guard_rejects_any_mutated_envelope(byte_index):
    packet = bytearray(LxpRequestBuilder.prepare_packet_for_read(
        TARGET.dongle_serial.encode(),
        TARGET.inverter_serial.encode(),
        0,
        125,
        READ_INPUT_FUNCTION_CODE,
    ))
    packet[byte_index] ^= 0x01

    with pytest.raises(BenchmarkSafetyError, match="refusing outgoing packet"):
        _assert_read_only_packet(bytes(packet), TARGET, ReadRange(0, 125))


def test_benchmark_client_exposes_no_write_operation():
    """Architectural API prevention complements the byte-level safety guard."""
    client = ReadOnlyBenchmarkClient(TARGET)
    public_names = [name for name in dir(client) if not name.startswith("_")]

    assert "run_cycle" in public_names
    assert "passive_probe" in public_names
    assert not any("write" in name for name in public_names)
    assert not hasattr(benchmark_module, "LxpRequestBuilder")


def test_statistics_and_nearest_rank_percentile_are_deterministic():
    values = [10.0, 20.0, 30.0, 40.0, 100.0]

    assert percentile_nearest_rank(values, 95) == 100.0
    assert summarize_durations(values) == {
        "samples": 5,
        "mean_ms": 40.0,
        "median_ms": 30.0,
        "p95_ms": 100.0,
        "min_ms": 10.0,
        "max_ms": 100.0,
    }
    assert summarize_durations([])["mean_ms"] is None


def test_combined_unsolicited_frames_are_classified_without_identifiers():
    """Synthetic type-193 and input frames may arrive in one socket chunk."""
    message_193 = bytes.fromhex(FUNCTION_193_MESSAGE)
    input_frame = synthetic_input_response()
    payload = message_193 + input_frame

    analyses, batch = analyse_payload(payload)

    assert len(split_lux_frames(payload).frames) == 2
    assert batch.trailing_bytes == 0
    assert analyses[0].summary["tcp_function"] == 193
    assert analyses[0].summary["structure_status"] == "accepted"
    assert analyses[0].summary["integrity_status"] == "unknown"
    assert analyses[1].summary["device_function"] == READ_INPUT_FUNCTION_CODE
    assert analyses[1].summary["integrity_status"] == "validated"
    assert analyses[1].summary["register_start"] == 0
    assert analyses[1].summary["register_count"] == 125
    serialized = json.dumps([analysis.summary for analysis in analyses])
    assert "DG99999999" not in serialized
    assert "TESTDONGLE" not in serialized
    assert "TESTINV001" not in serialized


def test_incomplete_frame_is_retained_as_trailing_bytes():
    frame = synthetic_input_response()

    batch = split_lux_frames(frame[:-10])

    assert batch.frames == ()
    assert batch.trailing_bytes == len(frame) - 10


def test_operational_plan_is_minimal_two_request_cover_and_full_plan_unchanged():
    """Experimental metadata does not replace the production six-block scan."""
    operational = input_registers_for_group(TelemetryGroup.OPERATIONAL)

    assert [(item.start, item.count) for item in FULL_INPUT_SHAPE.ranges] == [
        (0, 125),
        (125, 125),
        (250, 125),
        (375, 125),
        (500, 125),
        (625, 125),
    ]
    assert [(item.start, item.count) for item in OPERATIONAL_INPUT_SHAPE.ranges] == [
        (0, 108),
        (114, 119),
    ]
    assert operational <= OPERATIONAL_INPUT_SHAPE.requested_registers
    assert max(operational) - min(operational) + 1 > 125
    assert describe_shape(OPERATIONAL_INPUT_SHAPE)["request_count"] == 2


def test_target_and_structured_output_do_not_contain_connection_secrets():
    target = BenchmarkTarget(
        host="192.168.55.44",
        port=8000,
        dongle_serial="TESTSER001",
        inverter_serial="TESTINV002",
    )
    output = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "target": target.sanitized(),
        "read_shapes": [describe_shape(OPERATIONAL_INPUT_SHAPE)],
    }

    serialized = json.dumps(output)

    assert "192.168.55.44" not in serialized
    assert "TESTSER001" not in serialized
    assert "TESTINV002" not in serialized
    assert "target_fingerprint" in serialized
    assert "192.168.55.44" not in repr(target)


def test_observation_tracker_advances_only_explicitly_accepted_registers():
    times = iter([
        datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 10, 0, 2, tzinfo=timezone.utc),
    ])
    tracker = ObservationTracker(lambda: next(times))

    tracker.accept([0, 1])
    before = tracker.snapshot()
    tracker.accept([0])
    after = tracker.snapshot()

    assert after.input_registers[0] > before.input_registers[0]
    assert after.input_registers[1] == before.input_registers[1]
    assert after.input_registers is not before.input_registers


def test_cycle_summary_counts_outcomes_timeouts_and_interval_consumption():
    request = RequestMetric(
        sequence=1,
        read_range=ReadRange(0, 10),
        sent_at="2026-01-02T10:00:00+00:00",
        first_read_ms=10,
        complete_ms=20,
        response_bytes=57,
        parsed_registers=10,
        status="success",
        timeout=False,
        malformed=False,
        recovery_attempts=0,
        recovery_successes=0,
        unexpected_frames=[],
        error=None,
    )
    initial = InitialCapture(1000, None, 0, True)
    cycles = [
        CycleMetric(
            shape="synthetic",
            cadence_seconds=2,
            sequence=sequence,
            started_at="2026-01-02T10:00:00+00:00",
            duration_ms=duration,
            connect_ms=5,
            initial=initial,
            close_ms=1,
            requests=[request] if status != "failed" else [],
            status=status,
            connection_error="OSError" if status == "failed" else None,
            bytes_sent=38 if status != "failed" else 0,
            bytes_received=57 if status != "failed" else 0,
            recovery_attempts=0,
            recovery_successes=0,
            freshness_advanced=10 if status == "success" else 0,
            unread_freshness_changes=0,
            initial_comparison=None,
        )
        for sequence, (status, duration) in enumerate(
            (("success", 1000), ("partial", 1200), ("failed", 800)), start=1
        )
    ]

    summary = summarize_cycles(cycles, 2)

    assert summary["attempted_cycles"] == 3
    assert summary["successful_cycles"] == 1
    assert summary["partial_cycles"] == 1
    assert summary["failed_cycles"] == 1
    assert summary["connection_failures"] == 1
    assert summary["mean_interval_consumed_percent"] == 50.0
    assert summary["stable_for_faster_test"] is False


def test_full_selective_comparison_uses_only_matching_measured_cadences():
    runs = [
        {
            "shape": "full",
            "cadence_seconds": 5.0,
            "summary": {"cycle_duration": {"mean_ms": 1000.0}},
        },
        {
            "shape": "operational",
            "cadence_seconds": 5.0,
            "summary": {"cycle_duration": {"mean_ms": 400.0}},
        },
        {
            "shape": "full",
            "cadence_seconds": 3.0,
            "summary": {"cycle_duration": {"mean_ms": 900.0}},
        },
    ]

    comparison = compare_read_shapes(runs)

    assert len(comparison) == 1
    assert comparison[0]["cadence_seconds"] == 5.0
    assert comparison[0]["mean_cycle_reduction_percent"] == 60.0
    assert comparison[0]["full_requests_per_cycle"] == 6
    assert comparison[0]["operational_requests_per_cycle"] == 2


@pytest.mark.asyncio
async def test_synthetic_cycle_sends_only_read_packet_and_records_metrics():
    """Exercise connect, initial handling, request parsing, freshness, and close."""
    response = synthetic_input_response()
    reader = AsyncMock(spec=asyncio.StreamReader)
    reader.read = AsyncMock(side_effect=[b"", response])
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    writer.wait_closed = AsyncMock()

    async def connector(_host, _port):
        return reader, writer

    client = ReadOnlyBenchmarkClient(TARGET, connector=connector)
    shape = ReadShape("synthetic", (ReadRange(0, 125),))

    cycle = await client.run_cycle(shape, cadence_seconds=10, sequence=1)

    assert cycle.status == "success"
    assert cycle.requests[0].parsed_registers == 125
    assert cycle.requests[0].status == "success"
    assert cycle.freshness_advanced == 125
    assert cycle.unread_freshness_changes == 0
    writer.write.assert_called_once()
    sent_packet = writer.write.call_args.args[0]
    assert sent_packet[21] == READ_INPUT_FUNCTION_CODE
    assert sent_packet.hex() == (
        "a11a0100200001c254455354444f4e474c451200000454455354494e56303031"
        "00007d008e7e"
    )
    writer.close.assert_called_once_with()
    writer.wait_closed.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_initial_holding_frame_is_not_compared_with_input_registers():
    holding_frame = synthetic_input_response(function_code=3)
    input_frame = synthetic_input_response()
    reader = AsyncMock(spec=asyncio.StreamReader)
    reader.read = AsyncMock(side_effect=[holding_frame, input_frame])
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    writer.wait_closed = AsyncMock()

    async def connector(_host, _port):
        return reader, writer

    cycle = await ReadOnlyBenchmarkClient(
        TARGET, connector=connector
    ).run_cycle(ReadShape("synthetic", (ReadRange(0, 125),)), 10, 1)

    assert cycle.status == "success"
    assert cycle.initial.frames[0].summary["device_function"] == 3
    assert cycle.initial_comparison is None


@pytest.mark.asyncio
async def test_drain_failure_is_counted_as_an_attempted_failed_request():
    reader = AsyncMock(spec=asyncio.StreamReader)
    reader.read = AsyncMock(return_value=b"")
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    writer.drain = AsyncMock(side_effect=BrokenPipeError())
    writer.wait_closed = AsyncMock()

    async def connector(_host, _port):
        return reader, writer

    cycle = await ReadOnlyBenchmarkClient(
        TARGET, connector=connector
    ).run_cycle(ReadShape("synthetic", (ReadRange(0, 125),)), 10, 1)
    summary = summarize_cycles([cycle], 10)

    assert cycle.status == "failed"
    assert len(cycle.requests) == 1
    assert cycle.requests[0].error_phase == "drain"
    assert cycle.requests[0].request_bytes_queued == 38
    assert cycle.requests[0].drain_completed is False
    assert cycle.bytes_sent == 38
    assert summary["attempted_requests"] == 1
    assert summary["drain_phase_failures"] == 1


def test_freshness_corruption_and_recovery_block_faster_cadences():
    base = CycleMetric(
        shape="synthetic",
        cadence_seconds=10,
        sequence=1,
        started_at="2026-01-02T10:00:00+00:00",
        duration_ms=100,
        connect_ms=5,
        initial=None,
        close_ms=1,
        requests=[],
        status="success",
        connection_error=None,
        bytes_sent=0,
        bytes_received=0,
        recovery_attempts=0,
        recovery_successes=0,
        freshness_advanced=1,
        unread_freshness_changes=1,
        initial_comparison=None,
    )

    freshness_summary = summarize_cycles([base], 10)
    base.unread_freshness_changes = 0
    base.recovery_attempts = 1
    recovery_summary = summarize_cycles([base], 10)

    assert freshness_summary["stable_for_faster_test"] is False
    assert freshness_summary["stability_stop_reasons"] == [
        "unread_freshness_changed"
    ]
    assert recovery_summary["stable_for_faster_test"] is False
    assert recovery_summary["stability_stop_reasons"] == [
        "packet_recovery_required"
    ]


def test_effective_freshness_intervals_are_monotonic_and_run_local():
    def cycle_at(accepted_monotonic):
        request = RequestMetric(
            sequence=1,
            read_range=ReadRange(0, 1),
            sent_at="2026-01-02T10:00:00+00:00",
            first_read_ms=10,
            complete_ms=20,
            response_bytes=39,
            parsed_registers=1,
            status="success",
            timeout=False,
            malformed=False,
            recovery_attempts=0,
            recovery_successes=0,
            unexpected_frames=[],
            error=None,
            accepted_monotonic=accepted_monotonic,
            values={0: 123},
        )
        return CycleMetric(
            shape="synthetic",
            cadence_seconds=2,
            sequence=1,
            started_at="2026-01-02T10:00:00+00:00",
            duration_ms=20,
            connect_ms=1,
            initial=None,
            close_ms=1,
            requests=[request],
            status="success",
            connection_error=None,
            bytes_sent=38,
            bytes_received=39,
            recovery_attempts=0,
            recovery_successes=0,
            freshness_advanced=1,
            unread_freshness_changes=0,
            initial_comparison=None,
        )

    run_baseline = {}
    first = cycle_at(100.0)
    second = cycle_at(102.0)
    _record_effective_observation_intervals(first, run_baseline)
    _record_effective_observation_intervals(second, run_baseline)

    separate_run_first = cycle_at(500.0)
    _record_effective_observation_intervals(separate_run_first, {})

    assert first.observation_intervals_ms == ()
    assert second.observation_intervals_ms == (2000.0,)
    assert separate_run_first.observation_intervals_ms == ()


@pytest.mark.asyncio
async def test_passive_probe_never_sends_and_classifies_unsolicited_data():
    message = bytes.fromhex(FUNCTION_193_MESSAGE)
    reader = AsyncMock(spec=asyncio.StreamReader)
    reader.read = AsyncMock(side_effect=[message, b""])
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    writer.wait_closed = AsyncMock()

    async def connector(_host, _port):
        return reader, writer

    client = ReadOnlyBenchmarkClient(TARGET, connector=connector)

    result = await client.passive_probe(window_seconds=0.1)

    assert result["bytes_received"] == len(message)
    assert result["frames"][0]["tcp_function"] == 193
    writer.write.assert_not_called()


@pytest.mark.asyncio
async def test_cadence_shapes_are_paired_and_instability_stops_only_that_shape(
    monkeypatch,
):
    """Comparison order alternates without hiding a stable shape's faster runs."""
    calls = []

    class FakeClient:
        def __init__(self, _target):
            pass

        async def passive_probe(self, _window_seconds):
            raise AssertionError("no probes requested")

        async def run_cadence(self, shape, cadence, cycles):
            calls.append((shape.name, cadence, cycles))
            stable = not (shape.name == "full" and cadence == 5)
            return {
                "shape": shape.name,
                "cadence_seconds": cadence,
                "cycles": [],
                "summary": {
                    "stable_for_faster_test": stable,
                    "stability_stop_reasons": (
                        [] if stable else ["partial_or_failed_cycle"]
                    ),
                    "cycle_duration": {"mean_ms": None},
                },
            }

    monkeypatch.setattr(benchmark_module, "ReadOnlyBenchmarkClient", FakeClient)

    result = await execute_benchmark(
        TARGET,
        cadences=(10, 5, 3),
        cycles=2,
        unsolicited_probes=0,
    )

    assert calls == [
        ("full", 10, 2),
        ("operational", 10, 2),
        ("operational", 5, 2),
        ("full", 5, 2),
        ("operational", 3, 2),
    ]
    assert result["stopped_early"] == [{
        "shape": "full",
        "after_cadence_seconds": 5,
        "reasons": ["partial_or_failed_cycle"],
    }]


def test_benchmark_import_is_home_assistant_independent():
    repository = Path(__file__).resolve().parents[1]
    script = """
import builtins
import sys
real_import = builtins.__import__
def reject_home_assistant(name, *args, **kwargs):
    if name == 'homeassistant' or name.startswith('homeassistant.'):
        raise AssertionError(f'benchmark import reached {name}')
    return real_import(name, *args, **kwargs)
builtins.__import__ = reject_home_assistant
from luxpower.benchmark import OPERATIONAL_INPUT_SHAPE, ReadOnlyBenchmarkClient
assert len(OPERATIONAL_INPUT_SHAPE.ranges) == 2
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
