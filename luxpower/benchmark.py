"""Strictly read-only LuxPower hardware benchmark.

This development probe is intentionally separate from production polling.  It can
connect, passively receive data, and issue only Modbus input-register reads
(function code 4).  It has no write API and validates every outgoing packet before
it reaches the socket.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Awaitable, Callable, Iterable, Mapping, Sequence

from custom_components.lxp_modbus.classes.lxp_request_builder import (
    LxpRequestBuilder as _LxpRequestBuilder,
)
from custom_components.lxp_modbus.classes.lxp_packet_utils import (
    LxpPacketUtils as _LxpPacketUtils,
)
from custom_components.lxp_modbus.classes.lxp_response import LxpResponse
from custom_components.lxp_modbus.classes.packet_recovery import (
    PacketRecoveryHandler,
)
from custom_components.lxp_modbus.const import (
    MAX_PACKET_SIZE,
    READ_TIMEOUT,
    RESPONSE_OVERHEAD,
    TOTAL_REGISTERS,
)
from custom_components.lxp_modbus.observation import (
    LuxPowerObservationTimes,
    ObservationClock,
    require_aware_utc,
    utc_now,
)
from custom_components.lxp_modbus.constants import input_registers as input_constants
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
    input_registers_for_group,
)

BENCHMARK_SCHEMA_VERSION = 1
BENCHMARK_VERSION = "1.0"
READ_INPUT_FUNCTION_CODE = 4
MAX_READ_REGISTERS = 125
CONNECTION_TIMEOUT = 10.0
PRODUCTION_INITIAL_READ_SIZE = 300
PRODUCTION_INITIAL_TIMEOUT = 1.0
DEFAULT_CADENCES = (10.0, 5.0, 3.0, 2.0)
DEFAULT_CYCLES = 10


class BenchmarkSafetyError(RuntimeError):
    """Raised before any packet outside the read-only boundary can be sent."""


@dataclass(frozen=True)
class ReadRange:
    """One Modbus input-register read."""

    start: int
    count: int

    def __post_init__(self) -> None:
        if not 0 <= self.start <= 0xFFFF:
            raise ValueError(f"start register out of range: {self.start}")
        if not 1 <= self.count <= MAX_READ_REGISTERS:
            raise ValueError(f"register count must be 1-{MAX_READ_REGISTERS}")
        if self.start + self.count - 1 > 0xFFFF:
            raise ValueError("register range exceeds 16-bit address space")

    @property
    def end(self) -> int:
        return self.start + self.count - 1

    @property
    def expected_response_bytes(self) -> int:
        return RESPONSE_OVERHEAD + (self.count * 2)

    def addresses(self) -> range:
        return range(self.start, self.end + 1)


@dataclass(frozen=True)
class ReadShape:
    """A named experimental register layout."""

    name: str
    ranges: tuple[ReadRange, ...]

    @property
    def requested_registers(self) -> frozenset[int]:
        return frozenset(
            register
            for read_range in self.ranges
            for register in read_range.addresses()
        )


FULL_INPUT_SHAPE = ReadShape(
    "full",
    tuple(
        ReadRange(start, min(MAX_READ_REGISTERS, TOTAL_REGISTERS - start))
        for start in range(0, TOTAL_REGISTERS, MAX_READ_REGISTERS)
    ),
)

# This is the smallest two-request cover of every Stage 2 OPERATIONAL address.
# It is benchmark metadata only and is not used by production polling.
OPERATIONAL_INPUT_SHAPE = ReadShape(
    "operational",
    (ReadRange(0, 108), ReadRange(114, 119)),
)


def validate_operational_shape(shape: ReadShape = OPERATIONAL_INPUT_SHAPE) -> None:
    """Fail if the experimental shape misses a Stage 2 operational register."""
    missing = (
        input_registers_for_group(TelemetryGroup.OPERATIONAL)
        - shape.requested_registers
    )
    if missing:
        raise ValueError(f"operational shape misses registers: {sorted(missing)}")


validate_operational_shape()


@dataclass(frozen=True, repr=False)
class BenchmarkTarget:
    """Live connection details that must never enter benchmark output."""

    host: str = field(repr=False)
    port: int
    dongle_serial: str = field(repr=False)
    inverter_serial: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host is required")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be 1-65535")
        if len(self.dongle_serial.encode()) != 10:
            raise ValueError("dongle serial must be exactly 10 bytes")
        if len(self.inverter_serial.encode()) != 10:
            raise ValueError("inverter serial must be exactly 10 bytes")

    def sanitized(self) -> dict:
        """Return a stable target identifier without connection details."""
        material = "\0".join(
            (self.host, str(self.port), self.dongle_serial, self.inverter_serial)
        ).encode()
        return {
            "target_fingerprint": hashlib.sha256(material).hexdigest()[:12],
            "port": self.port,
        }


@dataclass(frozen=True)
class FrameBatch:
    """Complete Lux frames plus non-frame bytes retained for diagnostics."""

    frames: tuple[bytes, ...]
    trailing_bytes: int
    leading_bytes: int


def split_lux_frames(payload: bytes) -> FrameBatch:
    """Split one socket chunk into complete length-prefixed Lux frames.

    No raw data is returned by the structured serializer.  This helper exists to
    detect multiple combined frames and incomplete trailing data safely.
    """
    frames: list[bytes] = []
    leading_bytes = 0
    position = 0
    prefix = _LxpRequestBuilder.PREFIX

    while position < len(payload):
        frame_start = payload.find(prefix, position)
        if frame_start < 0:
            return FrameBatch(tuple(frames), len(payload) - position, leading_bytes)
        leading_bytes += frame_start - position
        if len(payload) - frame_start < 6:
            return FrameBatch(
                tuple(frames), len(payload) - frame_start, leading_bytes
            )

        frame_length = int.from_bytes(payload[frame_start + 4:frame_start + 6], "little")
        total_length = frame_length + 6
        if total_length < 8 or total_length > MAX_PACKET_SIZE:
            leading_bytes += 2
            position = frame_start + 2
            continue
        if len(payload) - frame_start < total_length:
            return FrameBatch(
                tuple(frames), len(payload) - frame_start, leading_bytes
            )

        frames.append(payload[frame_start:frame_start + total_length])
        position = frame_start + total_length

    return FrameBatch(tuple(frames), 0, leading_bytes)


@dataclass(frozen=True)
class FrameAnalysis:
    """Sanitized classification plus internal register values for comparison."""

    summary: Mapping[str, object]
    parsed_values: Mapping[int, int] = field(repr=False, compare=False)
    serial_number: bytes | None = field(repr=False, compare=False)


def _analyse_response(response: LxpResponse, byte_count: int) -> FrameAnalysis:
    """Create a sanitized analysis from an already parsed response."""
    if response.packet_error:
        integrity_status = "failed"
    elif response.tcp_function == _LxpRequestBuilder.TRANSLATED_DATA:
        integrity_status = "validated"
    else:
        # The existing parser can structurally accept function 193, but its own
        # protocol notes state that the integrity/CRC semantics are unknown.
        integrity_status = "unknown"
    parsed = (
        response.parsed_values_dictionary
        if integrity_status == "validated" else {}
    )
    register_end = (
        response.register + len(parsed) - 1
        if response.register >= 0 and parsed
        else None
    )
    return FrameAnalysis(
        summary={
            "bytes": byte_count,
            "structure_status": (
                "accepted" if not response.packet_error else "rejected"
            ),
            "integrity_status": integrity_status,
            "error": response.error_type if response.packet_error else None,
            "protocol": response.protocol_number,
            "tcp_function": response.tcp_function,
            "device_function": response.device_function,
            "register_start": response.register if response.register >= 0 else None,
            "register_end": register_end,
            "register_count": len(parsed),
            "exception": response.exception or None,
        },
        parsed_values=dict(parsed),
        serial_number=response.serial_number,
    )


def analyse_frame(frame: bytes) -> FrameAnalysis:
    """Classify a Lux frame without exposing serial numbers or raw bytes."""
    response = LxpResponse(frame)
    return _analyse_response(response, len(frame))


def analyse_payload(payload: bytes) -> tuple[list[FrameAnalysis], FrameBatch]:
    """Return sanitized analyses for every complete frame in a socket chunk."""
    batch = split_lux_frames(payload)
    return [analyse_frame(frame) for frame in batch.frames], batch


def percentile_nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Calculate a deterministic nearest-rank percentile."""
    if not values:
        raise ValueError("cannot calculate a percentile of no values")
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be in (0, 100]")
    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100) * len(ordered)))
    return ordered[rank - 1]


def summarize_durations(values: Sequence[float]) -> dict[str, float | int | None]:
    """Summarize millisecond durations without implying absent measurements."""
    if not values:
        return {
            "samples": 0,
            "mean_ms": None,
            "median_ms": None,
            "p95_ms": None,
            "min_ms": None,
            "max_ms": None,
        }
    return {
        "samples": len(values),
        "mean_ms": round(statistics.fmean(values), 3),
        "median_ms": round(statistics.median(values), 3),
        "p95_ms": round(percentile_nearest_rank(values, 95), 3),
        "min_ms": round(min(values), 3),
        "max_ms": round(max(values), 3),
    }


class ObservationTracker:
    """Benchmark-local Stage 2-compatible per-register freshness sidecar."""

    def __init__(self, clock: ObservationClock = utc_now) -> None:
        self._clock = clock
        self._input: dict[int, datetime] = {}

    def accept(self, registers: Iterable[int]) -> datetime:
        """Advance only registers actually accepted from a successful response."""
        observed_at = require_aware_utc(self._clock())
        for register in registers:
            self._input[register] = observed_at
        return observed_at

    def snapshot(self) -> LuxPowerObservationTimes:
        """Return detached Stage 2 observation metadata."""
        return LuxPowerObservationTimes(input_registers=dict(self._input))


def _assert_read_only_packet(
    packet: bytes,
    target: BenchmarkTarget,
    read_range: ReadRange,
) -> None:
    """Validate the complete known FC4 envelope immediately before sending."""
    if len(packet) != 38:
        raise BenchmarkSafetyError("refusing outgoing packet with unexpected length")
    checks = (
        (packet[:2] == _LxpRequestBuilder.PREFIX, "Lux header"),
        (int.from_bytes(packet[2:4], "little") == 1, "protocol"),
        (int.from_bytes(packet[4:6], "little") == len(packet) - 6, "frame length"),
        (packet[6] == 1, "address action"),
        (packet[7] == _LxpRequestBuilder.TRANSLATED_DATA, "TCP function"),
        (packet[8:18] == target.dongle_serial.encode(), "dongle target"),
        (int.from_bytes(packet[18:20], "little") == 18, "data length"),
        (packet[20] == _LxpRequestBuilder.ACTION_WRITE, "data action"),
        (packet[21] == READ_INPUT_FUNCTION_CODE, "device function"),
        (packet[22:32] == target.inverter_serial.encode(), "inverter target"),
        (int.from_bytes(packet[32:34], "little") == read_range.start, "start register"),
        (int.from_bytes(packet[34:36], "little") == read_range.count, "register count"),
        (
            int.from_bytes(packet[36:38], "little")
            == _LxpPacketUtils.compute_crc(packet[20:36]),
            "CRC",
        ),
    )
    for accepted, field_name in checks:
        if not accepted:
            raise BenchmarkSafetyError(
                f"refusing outgoing packet with unexpected {field_name}"
            )


@dataclass
class InitialCapture:
    """Sanitized result of the production-equivalent initial socket read."""

    handling_ms: float
    first_data_ms: float | None
    bytes_received: int
    timed_out: bool
    frames: list[FrameAnalysis] = field(default_factory=list, repr=False)
    trailing_bytes: int = 0
    leading_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            "handling_ms": round(self.handling_ms, 3),
            "first_data_ms": (
                round(self.first_data_ms, 3)
                if self.first_data_ms is not None else None
            ),
            "bytes_received": self.bytes_received,
            "timed_out": self.timed_out,
            "frames": [dict(frame.summary) for frame in self.frames],
            "trailing_bytes": self.trailing_bytes,
            "leading_bytes": self.leading_bytes,
        }


@dataclass
class RequestMetric:
    """One explicit input-register request measurement."""

    sequence: int
    read_range: ReadRange
    sent_at: str
    first_read_ms: float | None
    complete_ms: float
    response_bytes: int
    parsed_registers: int
    status: str
    timeout: bool
    malformed: bool
    recovery_attempts: int
    recovery_successes: int
    unexpected_frames: list[Mapping[str, object]]
    error: str | None
    error_phase: str | None = None
    request_bytes_queued: int = 0
    drain_completed: bool = False
    accepted_monotonic: float | None = field(default=None, repr=False)
    values: Mapping[int, int] = field(default_factory=dict, repr=False)
    observed_at: datetime | None = field(default=None, repr=False)
    invalid_response: bool = False

    def to_dict(self) -> dict:
        return {
            "sequence": self.sequence,
            "register_start": self.read_range.start,
            "register_count": self.read_range.count,
            "register_end": self.read_range.end,
            "function_code": READ_INPUT_FUNCTION_CODE,
            "sent_at": self.sent_at,
            "first_read_ms": (
                round(self.first_read_ms, 3)
                if self.first_read_ms is not None else None
            ),
            "complete_ms": round(self.complete_ms, 3),
            "response_bytes": self.response_bytes,
            "parsed_registers": self.parsed_registers,
            "status": self.status,
            "timeout": self.timeout,
            "malformed": self.malformed,
            "invalid_response": self.invalid_response,
            "recovery_attempts": self.recovery_attempts,
            "recovery_successes": self.recovery_successes,
            "unexpected_frames": [dict(frame) for frame in self.unexpected_frames],
            "error": self.error,
            "error_phase": self.error_phase,
            "request_bytes_queued": self.request_bytes_queued,
            "drain_completed": self.drain_completed,
            "observed_at": (
                self.observed_at.isoformat() if self.observed_at else None
            ),
        }


@dataclass
class CycleMetric:
    """One reconnect-per-cycle benchmark measurement."""

    shape: str
    cadence_seconds: float
    sequence: int
    started_at: str
    duration_ms: float
    connect_ms: float | None
    initial: InitialCapture | None
    close_ms: float | None
    requests: list[RequestMetric]
    status: str
    connection_error: str | None
    bytes_sent: int
    bytes_received: int
    recovery_attempts: int
    recovery_successes: int
    freshness_advanced: int
    unread_freshness_changes: int
    initial_comparison: Mapping[str, int] | None
    connect_started_at: str | None = None
    connected_at: str | None = None
    completed_at: str | None = None
    oldest_requested_observation_age_ms: float | None = None
    unknown_requested_freshness: int = 0
    observation_intervals_ms: tuple[float, ...] = field(
        default_factory=tuple, repr=False
    )

    def to_dict(self) -> dict:
        return {
            "shape": self.shape,
            "cadence_seconds": self.cadence_seconds,
            "sequence": self.sequence,
            "started_at": self.started_at,
            "connect_started_at": self.connect_started_at,
            "connected_at": self.connected_at,
            "completed_at": self.completed_at,
            "duration_ms": round(self.duration_ms, 3),
            "connect_ms": round(self.connect_ms, 3) if self.connect_ms is not None else None,
            "initial": self.initial.to_dict() if self.initial else None,
            "close_ms": round(self.close_ms, 3) if self.close_ms is not None else None,
            "requests": [request.to_dict() for request in self.requests],
            "status": self.status,
            "connection_error": self.connection_error,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "recovery_attempts": self.recovery_attempts,
            "recovery_successes": self.recovery_successes,
            "freshness_advanced": self.freshness_advanced,
            "unread_freshness_changes": self.unread_freshness_changes,
            "oldest_requested_observation_age_ms": (
                round(self.oldest_requested_observation_age_ms, 3)
                if self.oldest_requested_observation_age_ms is not None else None
            ),
            "unknown_requested_freshness": self.unknown_requested_freshness,
            "effective_observation_interval": summarize_durations(
                self.observation_intervals_ms
            ),
            "initial_comparison": dict(self.initial_comparison) if self.initial_comparison else None,
        }


Connector = Callable[[str, int], Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]]]


class ReadOnlyBenchmarkClient:
    """Benchmark transport exposing passive capture and input reads only."""

    __slots__ = (
        "_target",
        "_connector",
        "_monotonic",
        "_wall_clock",
        "_sleep",
        "_recovery",
        "_observations",
        "_observed_monotonic",
    )

    def __init__(
        self,
        target: BenchmarkTarget,
        *,
        connector: Connector = asyncio.open_connection,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: ObservationClock = utc_now,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._target = target
        self._connector = connector
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._recovery = PacketRecoveryHandler()
        self._observations = ObservationTracker(wall_clock)
        self._observed_monotonic: dict[int, float] = {}

    def observation_times(self) -> LuxPowerObservationTimes:
        """Return current benchmark-local Stage 2 freshness metadata."""
        return self._observations.snapshot()

    async def _connect(self):
        return await asyncio.wait_for(
            self._connector(self._target.host, self._target.port),
            timeout=CONNECTION_TIMEOUT,
        )

    async def _close(self, writer) -> float:
        started = self._monotonic()
        if writer is not None:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5.0)
            except (asyncio.TimeoutError, ConnectionError):
                pass
        return (self._monotonic() - started) * 1000

    async def _capture_initial_once(self, reader) -> InitialCapture:
        started = self._monotonic()
        timed_out = False
        payload = bytearray()
        try:
            payload = await asyncio.wait_for(
                reader.read(PRODUCTION_INITIAL_READ_SIZE),
                timeout=PRODUCTION_INITIAL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            timed_out = True
        handling_ms = (self._monotonic() - started) * 1000
        analyses, batch = analyse_payload(payload)
        return InitialCapture(
            handling_ms=handling_ms,
            first_data_ms=handling_ms if payload else None,
            bytes_received=len(payload),
            timed_out=timed_out,
            frames=analyses,
            trailing_bytes=batch.trailing_bytes,
            leading_bytes=batch.leading_bytes,
        )

    async def passive_probe(self, window_seconds: float) -> dict:
        """Connect and receive for a bounded window without sending any bytes."""
        started_at = require_aware_utc(self._wall_clock()).isoformat()
        overall_started = self._monotonic()
        connect_started = self._monotonic()
        connect_started_at = require_aware_utc(self._wall_clock()).isoformat()
        reader = writer = None
        chunks: list[dict] = []
        payload = bytearray()
        error = None
        connect_ms = None
        close_ms = None
        connected_at = None

        try:
            reader, writer = await self._connect()
            connected = self._monotonic()
            connected_at = require_aware_utc(self._wall_clock()).isoformat()
            connect_ms = (connected - connect_started) * 1000
            deadline = connected + window_seconds
            while self._monotonic() < deadline:
                remaining = deadline - self._monotonic()
                try:
                    chunk = await asyncio.wait_for(
                        reader.read(PRODUCTION_INITIAL_READ_SIZE), timeout=remaining
                    )
                except asyncio.TimeoutError:
                    break
                if not chunk:
                    break
                payload.extend(chunk)
                chunks.append({
                    "arrival_ms": round((self._monotonic() - connected) * 1000, 3),
                    "bytes": len(chunk),
                })
        except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
            error = type(exc).__name__
        finally:
            close_ms = await self._close(writer)

        analyses, batch = analyse_payload(bytes(payload))
        return {
            "started_at": started_at,
            "connect_started_at": connect_started_at,
            "connected_at": connected_at,
            "window_seconds": window_seconds,
            "duration_ms": round((self._monotonic() - overall_started) * 1000, 3),
            "connect_ms": round(connect_ms, 3) if connect_ms is not None else None,
            "close_ms": round(close_ms, 3),
            "bytes_received": len(payload),
            "chunks": chunks,
            "frames": [dict(frame.summary) for frame in analyses],
            "trailing_bytes": batch.trailing_bytes,
            "leading_bytes": batch.leading_bytes,
            "error": error,
        }

    async def _request(self, writer, reader, sequence: int, read_range: ReadRange) -> RequestMetric:
        packet = _LxpRequestBuilder.prepare_packet_for_read(
            self._target.dongle_serial.encode(),
            self._target.inverter_serial.encode(),
            read_range.start,
            read_range.count,
            READ_INPUT_FUNCTION_CODE,
        )
        _assert_read_only_packet(packet, self._target, read_range)
        sent_at = require_aware_utc(self._wall_clock()).isoformat()
        started = self._monotonic()

        first_read_ms = None
        payload = b""
        timeout = False
        error = None
        error_phase = None
        request_bytes_queued = 0
        drain_completed = False
        matching: FrameAnalysis | None = None
        response_packet_error = False
        unexpected: list[Mapping[str, object]] = []
        recovery_before = self._recovery.get_stats()

        def is_matching(analysis: FrameAnalysis) -> bool:
            summary = analysis.summary
            return bool(
                summary["integrity_status"] == "validated"
                and summary["device_function"] == READ_INPUT_FUNCTION_CODE
                and summary["register_start"] == read_range.start
                and analysis.serial_number == self._target.inverter_serial.encode()
            )

        try:
            error_phase = "write"
            writer.write(packet)
            request_bytes_queued = len(packet)
            error_phase = "drain"
            await asyncio.wait_for(writer.drain(), timeout=READ_TIMEOUT)
            drain_completed = True
            error_phase = "response"
            # Intentionally mirror production: one bounded socket read, followed
            # only by its existing conditional packet-recovery path.  The passive
            # probe separately investigates combined or later unsolicited frames.
            payload = await asyncio.wait_for(
                reader.read(read_range.expected_response_bytes), timeout=READ_TIMEOUT
            )
            first_read_ms = (self._monotonic() - started) * 1000
            if payload:
                response = LxpResponse(payload)
                if (
                    response.packet_error
                    and response.packet_length_calced
                    > read_range.expected_response_bytes
                ):
                    response = await self._recovery.async_attempt_recovery(
                        reader,
                        payload,
                        read_range.expected_response_bytes,
                        "benchmark/input",
                        READ_INPUT_FUNCTION_CODE,
                    )
                response_packet_error = response.packet_error
                response_analysis = _analyse_response(
                    response,
                    max(len(payload), response.packet_length_calced),
                )
                if is_matching(response_analysis):
                    matching = response_analysis

                analyses, _batch = analyse_payload(payload)
                unexpected = [
                    analysis.summary
                    for analysis in analyses
                    if not is_matching(analysis) or analysis is not analyses[0]
                ]
        except asyncio.TimeoutError:
            timeout = True
            error = "TimeoutError"
        except (ConnectionError, OSError) as exc:
            error = type(exc).__name__
        else:
            error_phase = None

        complete_ms = (self._monotonic() - started) * 1000
        recovery_after = self._recovery.get_stats()
        recovery_attempts = (
            recovery_after["total_recovery_attempts"]
            - recovery_before["total_recovery_attempts"]
        )
        recovery_successes = (
            recovery_after["successful_recoveries"]
            - recovery_before["successful_recoveries"]
        )

        values = dict(matching.parsed_values) if matching else {}
        malformed = bool(payload) and response_packet_error
        invalid_response = bool(payload) and matching is None
        if matching and len(values) == read_range.count:
            status = "success"
        elif matching and values:
            status = "partial"
        else:
            status = "failed"
        accepted_monotonic = self._monotonic() if values else None
        observed_at = self._observations.accept(values) if values else None
        if accepted_monotonic is not None:
            for register in values:
                self._observed_monotonic[register] = accepted_monotonic

        return RequestMetric(
            sequence=sequence,
            read_range=read_range,
            sent_at=sent_at,
            first_read_ms=first_read_ms,
            complete_ms=complete_ms,
            response_bytes=max(
                len(payload),
                int(matching.summary["bytes"]) if matching else 0,
            ),
            parsed_registers=len(values),
            status=status,
            timeout=timeout,
            malformed=malformed,
            recovery_attempts=recovery_attempts,
            recovery_successes=recovery_successes,
            unexpected_frames=unexpected,
            error=error,
            error_phase=error_phase if error else None,
            request_bytes_queued=request_bytes_queued,
            drain_completed=drain_completed,
            accepted_monotonic=accepted_monotonic,
            values=values,
            observed_at=observed_at,
            invalid_response=invalid_response,
        )

    async def run_cycle(
        self,
        shape: ReadShape,
        cadence_seconds: float,
        sequence: int,
    ) -> CycleMetric:
        """Run one reconnect-per-cycle input-read experiment."""
        started_at = require_aware_utc(self._wall_clock()).isoformat()
        cycle_started = self._monotonic()
        freshness_before = dict(self.observation_times().input_registers)
        reader = writer = None
        connect_ms = None
        close_ms = None
        initial = None
        requests: list[RequestMetric] = []
        connection_error = None
        recovery_before = self._recovery.get_stats()
        connect_started_at = None
        connected_at = None

        try:
            connect_started = self._monotonic()
            connect_started_at = require_aware_utc(self._wall_clock()).isoformat()
            reader, writer = await self._connect()
            connect_ms = (self._monotonic() - connect_started) * 1000
            connected_at = require_aware_utc(self._wall_clock()).isoformat()
            initial = await self._capture_initial_once(reader)
            for request_sequence, read_range in enumerate(shape.ranges, start=1):
                request = await self._request(
                    writer, reader, request_sequence, read_range
                )
                requests.append(request)
                if request.status == "failed":
                    break
        except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
            connection_error = type(exc).__name__
        finally:
            close_ms = await self._close(writer)

        statuses = [request.status for request in requests]
        if connection_error or not requests or all(status == "failed" for status in statuses):
            status = "failed"
        elif len(requests) != len(shape.ranges) or any(status != "success" for status in statuses):
            status = "partial"
        else:
            status = "success"

        freshness_after = dict(self.observation_times().input_registers)
        advanced = sum(
            1 for register, observed_at in freshness_after.items()
            if freshness_before.get(register) != observed_at
        )
        unread_changes = sum(
            1 for register in freshness_before.keys() - shape.requested_registers
            if freshness_before[register] != freshness_after.get(register)
        )

        explicit_values = {
            register: value
            for request in requests
            for register, value in request.values.items()
        }
        initial_values = {
            register: value
            for frame in (initial.frames if initial else [])
            if frame.summary["integrity_status"] == "validated"
            and frame.summary["device_function"] == READ_INPUT_FUNCTION_CODE
            if frame.serial_number == self._target.inverter_serial.encode()
            for register, value in frame.parsed_values.items()
        }
        common = set(explicit_values) & set(initial_values)
        initial_comparison = None
        if initial_values:
            initial_comparison = {
                "unsolicited_registers": len(initial_values),
                "common_registers": len(common),
                "matching_values": sum(
                    explicit_values[register] == initial_values[register]
                    for register in common
                ),
                "different_values": sum(
                    explicit_values[register] != initial_values[register]
                    for register in common
                ),
            }

        recovery_after = self._recovery.get_stats()
        completed_monotonic = self._monotonic()
        completed_at_value = require_aware_utc(self._wall_clock())
        requested_observations = [
            freshness_after[register]
            for register in shape.requested_registers
            if register in freshness_after
        ]
        oldest_age_ms = (
            max(
                (completed_monotonic - self._observed_monotonic[register]) * 1000
                for register in shape.requested_registers
                if register in self._observed_monotonic
            )
            if requested_observations else None
        )
        return CycleMetric(
            shape=shape.name,
            cadence_seconds=cadence_seconds,
            sequence=sequence,
            started_at=started_at,
            duration_ms=(completed_monotonic - cycle_started) * 1000,
            connect_ms=connect_ms,
            initial=initial,
            close_ms=close_ms,
            requests=requests,
            status=status,
            connection_error=connection_error,
            bytes_sent=sum(request.request_bytes_queued for request in requests),
            bytes_received=(initial.bytes_received if initial else 0)
            + sum(request.response_bytes for request in requests),
            recovery_attempts=(
                recovery_after["total_recovery_attempts"]
                - recovery_before["total_recovery_attempts"]
            ),
            recovery_successes=(
                recovery_after["successful_recoveries"]
                - recovery_before["successful_recoveries"]
            ),
            freshness_advanced=advanced,
            unread_freshness_changes=unread_changes,
            initial_comparison=initial_comparison,
            connect_started_at=connect_started_at,
            connected_at=connected_at,
            completed_at=completed_at_value.isoformat(),
            oldest_requested_observation_age_ms=oldest_age_ms,
            unknown_requested_freshness=(
                len(shape.requested_registers) - len(requested_observations)
            ),
        )

    async def run_cadence(
        self,
        shape: ReadShape,
        cadence_seconds: float,
        cycles: int,
    ) -> dict:
        """Run bounded cycles scheduled from monotonic start times."""
        results: list[CycleMetric] = []
        last_observed_monotonic: dict[int, float] = {}
        next_start = self._monotonic()
        for sequence in range(1, cycles + 1):
            if sequence > 1:
                await self._sleep(max(0.0, next_start - self._monotonic()))
            cycle_start = self._monotonic()
            cycle = await self.run_cycle(shape, cadence_seconds, sequence)
            _record_effective_observation_intervals(
                cycle, last_observed_monotonic
            )
            results.append(cycle)
            next_start = cycle_start + cadence_seconds
            if results[-1].unread_freshness_changes:
                break

        summary = summarize_cycles(results, cadence_seconds)
        return {
            "shape": shape.name,
            "cadence_seconds": cadence_seconds,
            "cycles": [cycle.to_dict() for cycle in results],
            "summary": summary,
        }


def _record_effective_observation_intervals(
    cycle: CycleMetric,
    last_observed_monotonic: dict[int, float],
) -> None:
    """Attach run-local monotonic intervals, excluding each first observation."""
    intervals: list[float] = []
    for request in cycle.requests:
        accepted = request.accepted_monotonic
        if accepted is None:
            continue
        for register in request.values:
            previous = last_observed_monotonic.get(register)
            if previous is not None:
                intervals.append((accepted - previous) * 1000)
            last_observed_monotonic[register] = accepted
    cycle.observation_intervals_ms = tuple(intervals)


def summarize_cycles(cycles: Sequence[CycleMetric], cadence_seconds: float) -> dict:
    """Aggregate cycle, request, failure, and timing measurements."""
    durations = [cycle.duration_ms for cycle in cycles]
    requests = [request for cycle in cycles for request in cycle.requests]
    successful = sum(cycle.status == "success" for cycle in cycles)
    partial = sum(cycle.status == "partial" for cycle in cycles)
    failed = sum(cycle.status == "failed" for cycle in cycles)
    unread_freshness_changes = sum(
        cycle.unread_freshness_changes for cycle in cycles
    )
    recovery_attempts = sum(cycle.recovery_attempts for cycle in cycles)
    stability_stop_reasons = []
    if partial or failed:
        stability_stop_reasons.append("partial_or_failed_cycle")
    if unread_freshness_changes:
        stability_stop_reasons.append("unread_freshness_changed")
    if recovery_attempts:
        stability_stop_reasons.append("packet_recovery_required")
    duration_summary = summarize_durations(durations)
    mean_ms = duration_summary["mean_ms"]
    return {
        "attempted_cycles": len(cycles),
        "successful_cycles": successful,
        "partial_cycles": partial,
        "failed_cycles": failed,
        "cycle_duration": duration_summary,
        "connect_duration": summarize_durations([
            cycle.connect_ms for cycle in cycles if cycle.connect_ms is not None
        ]),
        "initial_handling_duration": summarize_durations([
            cycle.initial.handling_ms for cycle in cycles if cycle.initial is not None
        ]),
        "close_duration": summarize_durations([
            cycle.close_ms for cycle in cycles if cycle.close_ms is not None
        ]),
        "request_duration": summarize_durations([
            request.complete_ms for request in requests
        ]),
        "attempted_requests": len(requests),
        "successful_requests": sum(request.status == "success" for request in requests),
        "partial_requests": sum(request.status == "partial" for request in requests),
        "failed_requests": sum(request.status == "failed" for request in requests),
        "request_timeouts": sum(request.timeout for request in requests),
        "write_phase_failures": sum(
            request.error_phase == "write" for request in requests
        ),
        "drain_phase_failures": sum(
            request.error_phase == "drain" for request in requests
        ),
        "response_phase_failures": sum(
            request.error_phase == "response" for request in requests
        ),
        "malformed_responses": sum(request.malformed for request in requests),
        "invalid_responses": sum(request.invalid_response for request in requests),
        "connection_failures": sum(cycle.connection_error is not None for cycle in cycles),
        "recovery_attempts": recovery_attempts,
        "recovery_successes": sum(cycle.recovery_successes for cycle in cycles),
        "bytes_sent": sum(cycle.bytes_sent for cycle in cycles),
        "bytes_received": sum(cycle.bytes_received for cycle in cycles),
        "mean_interval_consumed_percent": (
            round((mean_ms / (cadence_seconds * 1000)) * 100, 3)
            if mean_ms is not None and cadence_seconds > 0 else None
        ),
        "freshness_advanced": sum(cycle.freshness_advanced for cycle in cycles),
        "unread_freshness_changes": unread_freshness_changes,
        "oldest_requested_observation_age": summarize_durations([
            cycle.oldest_requested_observation_age_ms
            for cycle in cycles
            if cycle.oldest_requested_observation_age_ms is not None
        ]),
        "effective_observation_interval": summarize_durations([
            interval
            for cycle in cycles
            for interval in cycle.observation_intervals_ms
        ]),
        "unknown_requested_freshness": sum(
            cycle.unknown_requested_freshness for cycle in cycles
        ),
        "stable_for_faster_test": bool(cycles) and not stability_stop_reasons,
        "stability_stop_reasons": stability_stop_reasons,
    }


def _compact_register_ranges(registers: Iterable[int]) -> list[str]:
    """Render sorted register addresses as compact inclusive ranges."""
    ordered = sorted(set(registers))
    if not ordered:
        return []
    compact: list[str] = []
    start = previous = ordered[0]
    for register in ordered[1:]:
        if register == previous + 1:
            previous = register
            continue
        compact.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = register
    compact.append(str(start) if start == previous else f"{start}-{previous}")
    return compact


def _operational_fields(read_range: ReadRange) -> list[dict[str, object]]:
    """List documented constant names gained by one experimental range."""
    names_by_register: dict[int, list[str]] = {}
    for name, value in vars(input_constants).items():
        if (
            name.startswith("I_")
            and isinstance(value, int)
            and value in read_range.addresses()
            and input_register_group(value) is TelemetryGroup.OPERATIONAL
        ):
            names_by_register.setdefault(value, []).append(name)
    return [
        {"register": register, "names": sorted(names)}
        for register, names in sorted(names_by_register.items())
    ]


def describe_shape(shape: ReadShape) -> dict:
    """Return semantic and wire-size metadata for a read shape."""
    operational = input_registers_for_group(TelemetryGroup.OPERATIONAL)
    return {
        "name": shape.name,
        "request_count": len(shape.ranges),
        "requested_registers": len(shape.requested_registers),
        "operational_registers": len(shape.requested_registers & operational),
        "ranges": [
            {
                "start": read_range.start,
                "count": read_range.count,
                "end": read_range.end,
                "operational_registers": sum(
                    input_register_group(register) is TelemetryGroup.OPERATIONAL
                    for register in read_range.addresses()
                ),
                "incidental_registers": sum(
                    input_register_group(register) is not TelemetryGroup.OPERATIONAL
                    for register in read_range.addresses()
                ),
                "incidental_register_ranges": _compact_register_ranges(
                    register
                    for register in read_range.addresses()
                    if input_register_group(register) is not TelemetryGroup.OPERATIONAL
                ),
                "operational_fields": _operational_fields(read_range),
                "expected_response_bytes": read_range.expected_response_bytes,
            }
            for read_range in shape.ranges
        ],
    }


def compare_read_shapes(runs: Sequence[Mapping[str, object]]) -> list[dict]:
    """Compare full and operational runs only where both cadences were measured."""
    indexed = {
        (run["shape"], float(run["cadence_seconds"])): run
        for run in runs
    }
    comparisons: list[dict] = []
    full_cadences = {
        cadence for shape, cadence in indexed if shape == FULL_INPUT_SHAPE.name
    }
    operational_cadences = {
        cadence for shape, cadence in indexed
        if shape == OPERATIONAL_INPUT_SHAPE.name
    }
    for cadence in sorted(full_cadences & operational_cadences, reverse=True):
        full = indexed.get((FULL_INPUT_SHAPE.name, cadence))
        operational = indexed.get((OPERATIONAL_INPUT_SHAPE.name, cadence))
        if not full or not operational:
            continue
        full_mean = full["summary"]["cycle_duration"]["mean_ms"]
        operational_mean = operational["summary"]["cycle_duration"]["mean_ms"]
        comparisons.append({
            "cadence_seconds": cadence,
            "full_mean_cycle_ms": full_mean,
            "operational_mean_cycle_ms": operational_mean,
            "mean_cycle_reduction_percent": (
                round((1 - (operational_mean / full_mean)) * 100, 3)
                if full_mean not in (None, 0) and operational_mean is not None
                else None
            ),
            "full_requests_per_cycle": len(FULL_INPUT_SHAPE.ranges),
            "operational_requests_per_cycle": len(OPERATIONAL_INPUT_SHAPE.ranges),
            "theoretical_response_bytes_full": sum(
                read_range.expected_response_bytes
                for read_range in FULL_INPUT_SHAPE.ranges
            ),
            "theoretical_response_bytes_operational": sum(
                read_range.expected_response_bytes
                for read_range in OPERATIONAL_INPUT_SHAPE.ranges
            ),
        })
    return comparisons


def load_target_from_environment(environ: Mapping[str, str]) -> BenchmarkTarget:
    """Load secrets without placing them in command-line arguments."""
    required = (
        "LUXPOWER_HOST",
        "LUXPOWER_DONGLE_SERIAL",
        "LUXPOWER_INVERTER_SERIAL",
    )
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise ValueError(f"missing required environment variables: {', '.join(missing)}")
    try:
        port = int(environ.get("LUXPOWER_PORT", "8000"))
    except ValueError as exc:
        raise ValueError("LUXPOWER_PORT must be an integer") from exc
    return BenchmarkTarget(
        host=environ["LUXPOWER_HOST"],
        port=port,
        dongle_serial=environ["LUXPOWER_DONGLE_SERIAL"],
        inverter_serial=environ["LUXPOWER_INVERTER_SERIAL"],
    )


async def execute_benchmark(
    target: BenchmarkTarget,
    *,
    cadences: Sequence[float] = DEFAULT_CADENCES,
    cycles: int = DEFAULT_CYCLES,
    unsolicited_probes: int = 3,
    unsolicited_window: float = 2.0,
) -> dict:
    """Execute passive probes followed by conservative full/selective runs."""
    client = ReadOnlyBenchmarkClient(target)
    started_at = utc_now().isoformat()
    probes = [
        await client.passive_probe(unsolicited_window)
        for _ in range(unsolicited_probes)
    ]
    runs: list[dict] = []
    stopped: list[dict] = []

    active_shapes = {
        FULL_INPUT_SHAPE.name: True,
        OPERATIONAL_INPUT_SHAPE.name: True,
    }
    shapes = (FULL_INPUT_SHAPE, OPERATIONAL_INPUT_SHAPE)
    for cadence_index, cadence in enumerate(cadences):
        # Pair the two shapes at each cadence and alternate their order. This
        # reduces time-of-run and first-run bias without issuing concurrent
        # requests to the dongle.
        cadence_shapes = shapes if cadence_index % 2 == 0 else tuple(reversed(shapes))
        for shape in cadence_shapes:
            if not active_shapes[shape.name]:
                continue
            run = await client.run_cadence(shape, cadence, cycles)
            runs.append(run)
            if not run["summary"]["stable_for_faster_test"]:
                active_shapes[shape.name] = False
                stopped.append({
                    "shape": shape.name,
                    "after_cadence_seconds": cadence,
                    "reasons": run["summary"]["stability_stop_reasons"],
                })

    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "benchmark_version": BENCHMARK_VERSION,
        "safety": {
            "read_only": True,
            "permitted_function_codes": [READ_INPUT_FUNCTION_CODE],
            "writes_exposed": False,
        },
        "started_at": started_at,
        "completed_at": utc_now().isoformat(),
        "target": target.sanitized(),
        "configuration": {
            "cadences_seconds": list(cadences),
            "cycles_per_cadence": cycles,
            "unsolicited_probes": unsolicited_probes,
            "unsolicited_window_seconds": unsolicited_window,
            "connection_model": "reconnect_per_cycle",
            "initial_read_size": PRODUCTION_INITIAL_READ_SIZE,
            "initial_read_timeout_seconds": PRODUCTION_INITIAL_TIMEOUT,
        },
        "read_shapes": [
            describe_shape(FULL_INPUT_SHAPE),
            describe_shape(OPERATIONAL_INPUT_SHAPE),
        ],
        "unsolicited_probes": probes,
        "runs": runs,
        "full_vs_operational": compare_read_shapes(runs),
        "stopped_early": stopped,
    }


def parse_cadences(value: str) -> tuple[float, ...]:
    """Parse a descending, positive cadence list."""
    try:
        cadences = tuple(float(item.strip()) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cadences must be comma-separated numbers") from exc
    if not cadences or any(cadence <= 0 for cadence in cadences):
        raise argparse.ArgumentTypeError("cadences must be positive")
    if tuple(sorted(cadences, reverse=True)) != cadences:
        raise argparse.ArgumentTypeError("cadences must run slowest to fastest")
    return cadences


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Strictly read-only LuxPower hardware benchmark"
    )
    parser.add_argument(
        "--confirm-read-only",
        action="store_true",
        help="required acknowledgement before live network access",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="print the sanitized read plan without loading secrets or connecting",
    )
    parser.add_argument("--cadences", type=parse_cadences, default=DEFAULT_CADENCES)
    parser.add_argument("--cycles", type=int, default=DEFAULT_CYCLES)
    parser.add_argument("--unsolicited-probes", type=int, default=3)
    parser.add_argument("--unsolicited-window", type=float, default=2.0)
    parser.add_argument(
        "--output",
        type=Path,
        help="optional path for sanitized JSON output; live results should remain untracked",
    )
    return parser


def render_human_summary(result: Mapping[str, object]) -> str:
    """Render a compact human summary without target secrets."""
    lines = [
        "LuxPower READ-ONLY hardware benchmark",
        f"Target fingerprint: {result['target']['target_fingerprint']}",
    ]
    for run in result["runs"]:
        summary = run["summary"]
        lines.append(
            f"{run['shape']} @ {run['cadence_seconds']:g}s: "
            f"{summary['successful_cycles']}/{summary['attempted_cycles']} successful, "
            f"mean={summary['cycle_duration']['mean_ms']} ms, "
            f"timeouts={summary['request_timeouts']}"
        )
    return "\n".join(lines)


async def _async_main(arguments: argparse.Namespace) -> int:
    if arguments.plan:
        print(json.dumps({
            "schema_version": BENCHMARK_SCHEMA_VERSION,
            "benchmark_version": BENCHMARK_VERSION,
            "safety": {
                "read_only": True,
                "permitted_function_codes": [READ_INPUT_FUNCTION_CODE],
                "writes_exposed": False,
            },
            "read_shapes": [
                describe_shape(FULL_INPUT_SHAPE),
                describe_shape(OPERATIONAL_INPUT_SHAPE),
            ],
        }, indent=2, sort_keys=True))
        return 0
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    if arguments.cycles < 1 or arguments.unsolicited_probes < 0:
        raise ValueError("cycles must be positive and unsolicited probes non-negative")
    if arguments.unsolicited_window <= 0:
        raise ValueError("unsolicited window must be positive")

    target = load_target_from_environment(os.environ)
    result = await execute_benchmark(
        target,
        cadences=arguments.cadences,
        cycles=arguments.cycles,
        unsolicited_probes=arguments.unsolicited_probes,
        unsolicited_window=arguments.unsolicited_window,
    )
    serialized = json.dumps(result, indent=2, sort_keys=True)
    print(render_human_summary(result), file=sys.stderr)
    if arguments.output:
        arguments.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
    try:
        return asyncio.run(_async_main(arguments))
    except (BenchmarkSafetyError, ValueError) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
