"""Persistent, single-reader LuxPower FC4 session for experimental telemetry."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import time
from typing import Awaitable, Callable, Mapping

from ..const import MAX_PACKET_SIZE, READ_TIMEOUT
from ..exceptions import (
    LuxPowerCommunicationError,
    LuxPowerReadRejectedError,
    LuxPowerReadTimeoutError,
    LuxPowerSessionClosedError,
)
from ..observation import (
    LuxPowerObservationTimes,
    ObservationClock,
    require_aware_utc,
    utc_now,
)
from ..telemetry_groups import TelemetryGroup, input_register_group
from .data_validator import is_data_sane
from .connection_manager import CONNECTION_TIMEOUT
from .frame_decoder import LuxFrameDecoder
from .lxp_packet_utils import LxpPacketUtils
from .lxp_request_builder import LxpRequestBuilder as _LxpRequestBuilder
from .lxp_response import LxpResponse

READ_INPUT_FUNCTION_CODE = 4
MAX_READ_REGISTERS = 125
_READER_CHUNK_SIZE = MAX_PACKET_SIZE
_PARTIAL_FRAME_TIMEOUT = READ_TIMEOUT

Connector = Callable[
    [str, int],
    Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
]


@dataclass(frozen=True)
class LuxReadObservation:
    """One locally accepted, integrity-validated FC4 register observation."""

    register_start: int
    register_count: int
    values: Mapping[int, int]
    observed_at: datetime
    explicit_response: bool
    duplicate: bool

    @property
    def register_end(self) -> int:
        return self.register_start + self.register_count - 1


@dataclass(frozen=True)
class LuxReadSessionSnapshot:
    """Detached current input-register values and their observation times."""

    input_registers: Mapping[int, int] = field(default_factory=dict)
    observed_at: LuxPowerObservationTimes = field(
        default_factory=LuxPowerObservationTimes,
    )


@dataclass(frozen=True)
class LuxReadSessionMetrics:
    """Sanitized metrics; connection identifiers and raw packets are excluded."""

    connections: int
    reconnects: int
    bytes_received: int
    frames_received: int
    validated_fc4_frames: int
    expected_fc4_responses: int
    unmatched_fc4_observations: int
    duplicate_fc4_frames: int
    invalid_frames: int
    function_193_frames: int
    explicit_requests: int
    request_timeouts: int
    connection_losses: int
    operational_registers_expected: int
    operational_registers_unmatched: int
    observation_queue_drops: int
    request_latencies_ms: tuple[float, ...]
    request_latency_samples_total: int
    decoder_discarded_bytes: int
    decoder_buffered_bytes: int


@dataclass
class _PendingRead:
    start: int
    count: int
    sent_monotonic: float
    future: asyncio.Future[LuxReadObservation]


class LuxReadSession:
    """Own one TCP reader and safely route read-only Lux FC4 traffic.

    The reader task is the sole consumer of ``StreamReader.read``. Explicit read
    coroutines only write an FC4 request and await a future completed by the
    router. At most one explicit request is outstanding.
    """

    def __init__(
        self,
        host: str,
        dongle_serial: str,
        inverter_serial: str,
        *,
        port: int = 8000,
        connector: Connector = asyncio.open_connection,
        clock: ObservationClock = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        request_timeout: float = READ_TIMEOUT,
    ) -> None:
        if not host:
            raise ValueError("host is required")
        if not 1 <= port <= 65535:
            raise ValueError("port must be 1-65535")
        if len(dongle_serial.encode()) != 10:
            raise ValueError("dongle_serial must be exactly 10 bytes")
        if len(inverter_serial.encode()) != 10:
            raise ValueError("inverter_serial must be exactly 10 bytes")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")

        self._host = host
        self._port = port
        self._dongle_serial = dongle_serial.encode()
        self._inverter_serial = inverter_serial.encode()
        self._connector = connector
        self._clock = clock
        self._monotonic = monotonic
        self._request_timeout = request_timeout

        self._lifecycle_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._generation = 0
        self._decoder = LuxFrameDecoder()
        self._pending: _PendingRead | None = None
        self._observations: asyncio.Queue[LuxReadObservation] = asyncio.Queue(
            maxsize=1024
        )

        self._input_registers: dict[int, int] = {}
        self._input_observed_at: dict[int, datetime] = {}
        self._last_block_values: dict[tuple[int, int], dict[int, int]] = {}

        self._connections = 0
        self._bytes_received = 0
        self._frames_received = 0
        self._validated_fc4_frames = 0
        self._expected_fc4_responses = 0
        self._unmatched_fc4_observations = 0
        self._duplicate_fc4_frames = 0
        self._invalid_frames = 0
        self._function_193_frames = 0
        self._explicit_requests = 0
        self._request_timeouts = 0
        self._connection_losses = 0
        self._operational_registers_expected = 0
        self._operational_registers_unmatched = 0
        self._observation_queue_drops = 0
        self._request_latencies_ms: deque[float] = deque(maxlen=512)
        self._request_latency_samples_total = 0
        self._decoder_discarded_total = 0

    @property
    def connected(self) -> bool:
        """Whether the current connection generation has a live reader task."""
        return bool(
            self._writer is not None
            and self._reader_task is not None
            and not self._reader_task.done()
        )

    async def async_connect(self) -> None:
        """Connect and immediately start the sole socket reader."""
        async with self._lifecycle_lock:
            if self.connected:
                return
            reader, writer = await asyncio.wait_for(
                self._connector(self._host, self._port),
                timeout=CONNECTION_TIMEOUT,
            )
            self._generation += 1
            generation = self._generation
            self._decoder = LuxFrameDecoder()
            self._reader = reader
            self._writer = writer
            self._connections += 1
            self._reader_task = asyncio.create_task(
                self._reader_loop(generation, reader),
                name=f"lux-fc4-reader-{generation}",
            )

    async def async_close(self) -> None:
        """Stop the reader, fail pending work, and close the active socket."""
        async with self._lifecycle_lock:
            task = self._reader_task
            writer = self._writer
            self._reader_task = None
            self._reader = None
            self._writer = None
            self._generation += 1
            self._fail_pending(LuxPowerSessionClosedError("read session closed"))
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await self._close_writer(writer)
            decoder_stats = self._decoder.stats()
            self._decoder_discarded_total += decoder_stats.discarded_bytes
            if decoder_stats.buffered_bytes:
                self._invalid_frames += 1
            self._decoder.reset()

    async def async_reconnect(self, *, delay: float = 0) -> None:
        """Start a clean connection generation after an optional bounded delay."""
        if delay < 0:
            raise ValueError("reconnect delay cannot be negative")
        await self.async_close()
        if delay:
            await asyncio.sleep(delay)
        await self.async_connect()

    async def async_read_input(
        self,
        start_register: int,
        register_count: int,
        *,
        timeout: float | None = None,
    ) -> LuxReadObservation:
        """Issue one FC4 read and await only its exactly correlated response."""
        self._validate_read_range(start_register, register_count)
        response_timeout = self._request_timeout if timeout is None else timeout
        if response_timeout <= 0:
            raise ValueError("timeout must be positive")

        async with self._request_lock:
            if not self.connected or self._writer is None:
                raise LuxPowerSessionClosedError("read session is not connected")
            if self._pending is not None:
                raise RuntimeError("an explicit FC4 request is already pending")

            packet = _LxpRequestBuilder.prepare_packet_for_read(
                self._dongle_serial,
                self._inverter_serial,
                start_register,
                register_count,
                READ_INPUT_FUNCTION_CODE,
            )
            self._assert_read_only_packet(packet, start_register, register_count)
            future = asyncio.get_running_loop().create_future()
            pending = _PendingRead(
                start=start_register,
                count=register_count,
                sent_monotonic=self._monotonic(),
                future=future,
            )
            self._pending = pending
            self._explicit_requests += 1

            loop = asyncio.get_running_loop()
            deadline = loop.time() + response_timeout
            try:
                self._writer.write(packet)
                await asyncio.wait_for(
                    self._writer.drain(), timeout=max(0, deadline - loop.time())
                )
                return await asyncio.wait_for(
                    asyncio.shield(future), timeout=max(0, deadline - loop.time())
                )
            except asyncio.TimeoutError as exc:
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                self._request_timeouts += 1
                # FC4 has no transaction identifier. A late response on this
                # generation could otherwise satisfy a later same-range request.
                await self.async_close()
                raise LuxPowerReadTimeoutError(
                    f"timed out waiting for FC4 registers "
                    f"{start_register}-{start_register + register_count - 1}"
                ) from exc
            except asyncio.CancelledError:
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                await self.async_close()
                raise
            except (ConnectionError, OSError) as exc:
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                await self.async_close()
                raise LuxPowerCommunicationError("FC4 request failed") from exc
            except (LuxPowerReadRejectedError, LuxPowerCommunicationError):
                raise
            except Exception as exc:
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                await self.async_close()
                raise LuxPowerCommunicationError(
                    "FC4 request ended ambiguously"
                ) from exc

    async def async_next_observation(
        self, *, timeout: float | None = None
    ) -> LuxReadObservation:
        """Return the next accepted expected or unsolicited FC4 observation."""
        if timeout is None:
            return await self._observations.get()
        return await asyncio.wait_for(self._observations.get(), timeout=timeout)

    def drain_observations(self) -> tuple[LuxReadObservation, ...]:
        """Return and remove currently queued observations without socket access."""
        observations: list[LuxReadObservation] = []
        while True:
            try:
                observations.append(self._observations.get_nowait())
            except asyncio.QueueEmpty:
                return tuple(observations)

    def snapshot(self) -> LuxReadSessionSnapshot:
        """Return detached values and per-register local observation times."""
        return LuxReadSessionSnapshot(
            input_registers=dict(self._input_registers),
            observed_at=LuxPowerObservationTimes(
                input_registers=dict(self._input_observed_at)
            ),
        )

    def metrics(self) -> LuxReadSessionMetrics:
        """Return sanitized immutable session metrics."""
        decoder_stats = self._decoder.stats()
        return LuxReadSessionMetrics(
            connections=self._connections,
            reconnects=max(0, self._connections - 1),
            bytes_received=self._bytes_received,
            frames_received=self._frames_received,
            validated_fc4_frames=self._validated_fc4_frames,
            expected_fc4_responses=self._expected_fc4_responses,
            unmatched_fc4_observations=self._unmatched_fc4_observations,
            duplicate_fc4_frames=self._duplicate_fc4_frames,
            invalid_frames=self._invalid_frames,
            function_193_frames=self._function_193_frames,
            explicit_requests=self._explicit_requests,
            request_timeouts=self._request_timeouts,
            connection_losses=self._connection_losses,
            operational_registers_expected=self._operational_registers_expected,
            operational_registers_unmatched=self._operational_registers_unmatched,
            observation_queue_drops=self._observation_queue_drops,
            request_latencies_ms=tuple(self._request_latencies_ms),
            request_latency_samples_total=self._request_latency_samples_total,
            decoder_discarded_bytes=(
                self._decoder_discarded_total + decoder_stats.discarded_bytes
            ),
            decoder_buffered_bytes=decoder_stats.buffered_bytes,
        )

    async def _reader_loop(
        self, generation: int, reader: asyncio.StreamReader
    ) -> None:
        error: BaseException | None = None
        try:
            while generation == self._generation:
                if self._decoder.stats().buffered_bytes:
                    try:
                        chunk = await asyncio.wait_for(
                            reader.read(_READER_CHUNK_SIZE),
                            timeout=_PARTIAL_FRAME_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        if self._decoder.discard_partial():
                            self._invalid_frames += 1
                        continue
                else:
                    chunk = await reader.read(_READER_CHUNK_SIZE)
                if not chunk:
                    raise ConnectionResetError("LuxPower socket closed")
                self._bytes_received += len(chunk)
                malformed_before = self._decoder.stats().malformed_lengths
                frames = self._decoder.feed(chunk)
                malformed_after = self._decoder.stats().malformed_lengths
                self._invalid_frames += malformed_after - malformed_before
                for frame in frames:
                    self._route_frame(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # reader failure must release every waiter
            error = exc
        finally:
            if generation == self._generation and error is not None:
                await self._reader_failed(generation, error)

    def _route_frame(self, frame: bytes) -> None:
        self._frames_received += 1
        response = LxpResponse(frame)

        if not response.packet_error and response.tcp_function == 193:
            # Integrity and semantics are not established; diagnostics only.
            self._function_193_frames += 1
            return

        targeted = bool(
            not response.packet_error
            and response.tcp_function == _LxpRequestBuilder.TRANSLATED_DATA
            and response.dongle_serial == self._dongle_serial
            and response.serial_number == self._inverter_serial
        )
        pending = self._pending
        if (
            targeted
            and pending is not None
            and response.device_function == (READ_INPUT_FUNCTION_CODE | 0x80)
            and response.register == pending.start
        ):
            self._pending = None
            if not pending.future.done():
                pending.future.set_exception(
                    LuxPowerReadRejectedError(
                        f"FC4 read rejected with exception {response.exception}"
                    )
                )
            return

        values = response.parsed_values_dictionary
        count = len(values)
        contiguous = bool(
            values
            and tuple(values) == tuple(range(response.register, response.register + count))
        )
        if not (
            targeted
            and response.device_function == READ_INPUT_FUNCTION_CODE
            and not response.exception
            and values
            and response.address_action == 1
            and response.data_length == response.frame_length - 14
            and response.value_length == count * 2
            and len(response.value) == response.value_length
            and contiguous
            and response.register >= 0
            and response.register + count <= 750
            and is_data_sane(values, "input")
        ):
            self._invalid_frames += 1
            return

        key = (response.register, count)
        duplicate = self._last_block_values.get(key) == values
        observed_at = require_aware_utc(self._clock())
        self._input_registers.update(values)
        self._input_observed_at.update(
            {register: observed_at for register in values}
        )
        self._last_block_values[key] = dict(values)
        self._validated_fc4_frames += 1
        if duplicate:
            self._duplicate_fc4_frames += 1

        explicit = bool(
            pending is not None
            and response.register == pending.start
            and count == pending.count
        )
        observation = LuxReadObservation(
            register_start=response.register,
            register_count=count,
            values=dict(values),
            observed_at=observed_at,
            explicit_response=explicit,
            duplicate=duplicate,
        )
        self._publish_observation(observation)

        if explicit and pending is not None:
            self._pending = None
            self._expected_fc4_responses += 1
            self._request_latencies_ms.append(
                (self._monotonic() - pending.sent_monotonic) * 1000
            )
            self._request_latency_samples_total += 1
            if not pending.future.done():
                pending.future.set_result(observation)
            self._operational_registers_expected += sum(
                input_register_group(register) is TelemetryGroup.OPERATIONAL
                for register in values
            )
        else:
            self._unmatched_fc4_observations += 1
            self._operational_registers_unmatched += sum(
                input_register_group(register) is TelemetryGroup.OPERATIONAL
                for register in values
            )

    def _publish_observation(self, observation: LuxReadObservation) -> None:
        if self._observations.full():
            with suppress(asyncio.QueueEmpty):
                self._observations.get_nowait()
                self._observation_queue_drops += 1
        self._observations.put_nowait(observation)

    async def _reader_failed(self, generation: int, error: BaseException) -> None:
        async with self._lifecycle_lock:
            if generation != self._generation:
                return
            writer = self._writer
            self._reader = None
            self._writer = None
            self._reader_task = None
            self._generation += 1
            self._connection_losses += 1
            self._fail_pending(
                LuxPowerCommunicationError("frame-aware read connection lost")
            )
            await self._close_writer(writer)
            decoder_stats = self._decoder.stats()
            self._decoder_discarded_total += decoder_stats.discarded_bytes
            if decoder_stats.buffered_bytes:
                self._invalid_frames += 1
            self._decoder.reset()

    def _fail_pending(self, error: Exception) -> None:
        pending = self._pending
        self._pending = None
        if pending is not None and not pending.future.done():
            pending.future.set_exception(error)

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter | None) -> None:
        if writer is None:
            return
        writer.close()
        with suppress(asyncio.TimeoutError, ConnectionError, OSError):
            await asyncio.wait_for(writer.wait_closed(), timeout=5)

    @staticmethod
    def _validate_read_range(start: int, count: int) -> None:
        if not 0 <= start < 750:
            raise ValueError("input-register start must be in the 0-749 range")
        if not 1 <= count <= MAX_READ_REGISTERS:
            raise ValueError(f"register count must be 1-{MAX_READ_REGISTERS}")
        if start + count > 750:
            raise ValueError("input-register range exceeds 0-749")

    def _assert_read_only_packet(self, packet: bytes, start: int, count: int) -> None:
        """Architecturally prevent any non-FC4 packet from reaching the socket."""
        checks = (
            len(packet) == 38,
            packet[:2] == _LxpRequestBuilder.PREFIX,
            int.from_bytes(packet[2:4], "little") == _LxpRequestBuilder.PROTOCOL,
            int.from_bytes(packet[4:6], "little") == len(packet) - 6,
            packet[6] == 1,
            packet[7] == _LxpRequestBuilder.TRANSLATED_DATA,
            packet[8:18] == self._dongle_serial,
            int.from_bytes(packet[18:20], "little") == _LxpRequestBuilder.DATA_LENGTH,
            packet[20] == _LxpRequestBuilder.ACTION_WRITE,
            packet[21] == READ_INPUT_FUNCTION_CODE,
            packet[22:32] == self._inverter_serial,
            int.from_bytes(packet[32:34], "little") == start,
            int.from_bytes(packet[34:36], "little") == count,
            int.from_bytes(packet[36:38], "little")
            == LxpPacketUtils.compute_crc(packet[20:36]),
        )
        if not all(checks):
            raise LuxPowerCommunicationError("refusing unsafe outgoing packet")
