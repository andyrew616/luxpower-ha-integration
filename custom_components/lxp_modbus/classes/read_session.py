"""Persistent, single-reader LuxPower FC4 session for experimental telemetry."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import math
import socket
import time
from types import MappingProxyType
from typing import Awaitable, Callable, Mapping

from ..const import MAX_PACKET_SIZE, READ_TIMEOUT
from ..exceptions import (
    LuxPowerAmbiguousRequestError,
    LuxPowerCommunicationError,
    LuxPowerConnectionError,
    LuxPowerConnectionLostError,
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
from ..timeout_diagnostics import (
    LuxDiagnosticEventKind,
    LuxInvalidFrameReason,
    LuxReadDiagnosticJournal,
    LuxReadDiagnosticsSnapshot,
    LuxReadRequestContext,
    LuxReadRequestOutcome,
    _DiagnosticRequestState,
)
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
_REQUEST_LATENCY_HISTORY = 4096
_DEFAULT_TCP_KEEPALIVE_IDLE_SECONDS = 60
_DEFAULT_RECEIVE_INACTIVITY_TIMEOUT = 900.0

Connector = Callable[
    [str, int],
    Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
]

_OBSERVATION_SUBSCRIPTION_CLOSED = object()


@dataclass(frozen=True)
class LuxReadObservation:
    """One locally accepted, integrity-validated FC4 register observation."""

    register_start: int
    register_count: int
    values: Mapping[int, int]
    observed_at: datetime
    explicit_response: bool
    duplicate: bool
    sequence: int = 0

    def __post_init__(self) -> None:
        """Detach and freeze register values before sharing the observation."""
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    @property
    def register_end(self) -> int:
        return self.register_start + self.register_count - 1


@dataclass(frozen=True)
class LuxObservationSubscriptionSnapshot:
    """Detached delivery state for one opt-in observation subscriber."""

    active: bool
    queued: int
    capacity: int
    observations_received: int
    observations_dropped: int
    last_sequence_received: int | None


@dataclass
class _ObservationSubscriptionState:
    queue: asyncio.Queue[object]
    capacity: int
    observations_received: int = 0
    observations_dropped: int = 0
    last_sequence_received: int | None = None
    active: bool = True
    consumer_waiting: bool = False


class LuxObservationSubscription:
    """One independent bounded stream of accepted FC4 observations.

    Delivery is deliberately opt-in: sessions with no subscribers retain no
    observation-event copies. A slow subscriber drops its own oldest queued
    observation and exposes that gap explicitly without delaying the socket
    reader or affecting authoritative register state.
    """

    def __init__(
        self,
        session: "LuxReadSession",
        identifier: int,
        state: _ObservationSubscriptionState,
    ) -> None:
        self._session = session
        self._identifier = identifier
        self._state = state

    async def async_next(
        self, *, timeout: float | None = None
    ) -> LuxReadObservation:
        """Return the next observation for this subscription's sole consumer."""
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if not self._state.active:
            raise LuxPowerSessionClosedError("observation subscription is closed")
        if self._state.consumer_waiting:
            raise RuntimeError(
                "only one async_next waiter is allowed per observation subscription"
            )
        self._state.consumer_waiting = True
        try:
            if timeout is None:
                item = await self._state.queue.get()
            else:
                item = await asyncio.wait_for(
                    self._state.queue.get(), timeout=timeout
                )
        finally:
            self._state.consumer_waiting = False
        if item is _OBSERVATION_SUBSCRIPTION_CLOSED:
            raise LuxPowerSessionClosedError("observation subscription is closed")
        return item  # type: ignore[return-value]

    def drain(self) -> tuple[LuxReadObservation, ...]:
        """Return and remove currently queued observations without socket I/O."""
        observations: list[LuxReadObservation] = []
        while True:
            try:
                item = self._state.queue.get_nowait()
            except asyncio.QueueEmpty:
                return tuple(observations)
            if item is _OBSERVATION_SUBSCRIPTION_CLOSED:
                return tuple(observations)
            observations.append(item)  # type: ignore[arg-type]

    def snapshot(self) -> LuxObservationSubscriptionSnapshot:
        """Return sanitized delivery and gap counters for this subscriber."""
        return LuxObservationSubscriptionSnapshot(
            active=self._state.active,
            queued=self._state.queue.qsize(),
            capacity=self._state.capacity,
            observations_received=self._state.observations_received,
            observations_dropped=self._state.observations_dropped,
            last_sequence_received=self._state.last_sequence_received,
        )

    def close(self) -> None:
        """Unsubscribe and wake a pending consumer without touching the socket."""
        self._session._remove_observation_subscription(  # noqa: SLF001
            self._identifier,
            self._state,
        )

    async def __aenter__(self) -> "LuxObservationSubscription":
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        self.close()


class LuxObservationSource(str, Enum):
    """How the latest accepted value for one register was observed."""

    EXPLICIT = "explicit"
    UNSOLICITED = "unsolicited"


@dataclass(frozen=True)
class LuxReadSessionSnapshot:
    """Detached current input-register values and their observation times."""

    input_registers: Mapping[int, int] = field(default_factory=dict)
    observed_at: LuxPowerObservationTimes = field(
        default_factory=LuxPowerObservationTimes,
    )
    input_sources: Mapping[int, LuxObservationSource] = field(default_factory=dict)
    explicit_observed_at: Mapping[int, datetime] = field(default_factory=dict)
    unsolicited_observed_at: Mapping[int, datetime] = field(default_factory=dict)


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
    connection_attempts: int = 0
    connection_failures: int = 0
    ambiguous_requests: int = 0
    modbus_rejections: int = 0
    tcp_keepalive_applied_connections: int = 0
    tcp_keepalive_idle_applied_connections: int = 0
    tcp_keepalive_configuration_failures: int = 0
    tcp_keepalive_configuration_unavailable: int = 0
    receive_inactivity_timeouts: int = 0


@dataclass
class _PendingRead:
    start: int
    count: int
    generation: int
    sent_monotonic: float
    future: asyncio.Future[LuxReadObservation]
    diagnostic: _DiagnosticRequestState


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
        drain_timeout: float | None = None,
        reply_timeout: float | None = None,
        tcp_keepalive: bool = True,
        tcp_keepalive_idle_seconds: int = _DEFAULT_TCP_KEEPALIVE_IDLE_SECONDS,
        receive_inactivity_timeout: float | None = (
            _DEFAULT_RECEIVE_INACTIVITY_TIMEOUT
        ),
        diagnostic_monotonic: Callable[[], float] = time.monotonic,
        diagnostic_event_capacity: int = 512,
        diagnostic_request_capacity: int = 4096,
        diagnostic_failure_capacity: int = 64,
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
        if drain_timeout is not None and drain_timeout <= 0:
            raise ValueError("drain_timeout must be positive")
        if reply_timeout is not None and reply_timeout <= 0:
            raise ValueError("reply_timeout must be positive")
        if (
            isinstance(tcp_keepalive_idle_seconds, bool)
            or not isinstance(tcp_keepalive_idle_seconds, int)
            or tcp_keepalive_idle_seconds <= 0
        ):
            raise ValueError("tcp_keepalive_idle_seconds must be positive")
        if receive_inactivity_timeout is not None and (
            receive_inactivity_timeout <= 0
            or not math.isfinite(receive_inactivity_timeout)
        ):
            raise ValueError(
                "receive_inactivity_timeout must be finite and positive"
            )

        self._host = host
        self._port = port
        self._dongle_serial = dongle_serial.encode()
        self._inverter_serial = inverter_serial.encode()
        self._connector = connector
        self._clock = clock
        self._monotonic = monotonic
        self._request_timeout = request_timeout
        self._drain_timeout = (
            request_timeout if drain_timeout is None else drain_timeout
        )
        self._reply_timeout = (
            request_timeout if reply_timeout is None else reply_timeout
        )
        # Preserve the historic combined deadline unless a caller explicitly
        # opts into independently timed drain and reply phases.
        self._split_request_deadlines = (
            drain_timeout is not None or reply_timeout is not None
        )
        self._tcp_keepalive = tcp_keepalive
        self._tcp_keepalive_idle_seconds = tcp_keepalive_idle_seconds
        self._receive_inactivity_timeout = receive_inactivity_timeout
        self._diagnostics = LuxReadDiagnosticJournal(
            monotonic=diagnostic_monotonic,
            event_capacity=diagnostic_event_capacity,
            request_capacity=diagnostic_request_capacity,
            failure_capacity=diagnostic_failure_capacity,
        )

        self._lifecycle_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._generation = 0
        self._active_connection_generation: int | None = None
        self._decoder = LuxFrameDecoder()
        self._pending: _PendingRead | None = None
        self._connection_lost = False
        self._connection_opened_diagnostic_monotonic = self._diagnostics.now()
        self._requests_on_generation = 0
        self._observation_subscriptions: dict[
            int, _ObservationSubscriptionState
        ] = {}
        self._next_observation_subscription_id = 1
        self._legacy_observation_subscription: LuxObservationSubscription | None = (
            None
        )
        self._observation_sequence = 0

        self._input_registers: dict[int, int] = {}
        self._input_observed_at: dict[int, datetime] = {}
        self._input_sources: dict[int, LuxObservationSource] = {}
        self._explicit_observed_at: dict[int, datetime] = {}
        self._unsolicited_observed_at: dict[int, datetime] = {}
        self._last_block_values: dict[tuple[int, int], dict[int, int]] = {}

        self._connections = 0
        self._connection_attempts = 0
        self._connection_failures = 0
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
        self._ambiguous_requests = 0
        self._modbus_rejections = 0
        self._tcp_keepalive_applied_connections = 0
        self._tcp_keepalive_idle_applied_connections = 0
        self._tcp_keepalive_configuration_failures = 0
        self._tcp_keepalive_configuration_unavailable = 0
        self._receive_inactivity_timeouts = 0
        self._connection_losses = 0
        self._operational_registers_expected = 0
        self._operational_registers_unmatched = 0
        self._observation_queue_drops = 0
        # Bounded but large enough to retain an hour-scale qualification run.
        self._request_latencies_ms: deque[float] = deque(
            maxlen=_REQUEST_LATENCY_HISTORY
        )
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

    @property
    def request_timeout_seconds(self) -> float:
        """Historic combined deadline, retained for backwards compatibility."""
        return self._request_timeout

    @property
    def drain_timeout_seconds(self) -> float:
        """Maximum time allowed for an explicit request's writer drain."""
        return self._drain_timeout

    @property
    def reply_timeout_seconds(self) -> float:
        """Maximum time allowed for a reply after a successful writer drain."""
        return self._reply_timeout

    @property
    def split_request_deadlines(self) -> bool:
        """Whether drain and reply phases use independent timeout budgets."""
        return self._split_request_deadlines

    @property
    def tcp_keepalive_enabled(self) -> bool:
        """Whether new sockets request best-effort OS TCP keepalive."""
        return self._tcp_keepalive

    @property
    def tcp_keepalive_idle_seconds(self) -> int:
        """Requested idle time before the OS begins TCP keepalive probing."""
        return self._tcp_keepalive_idle_seconds

    @property
    def receive_inactivity_timeout_seconds(self) -> float | None:
        """Maximum application-byte silence before retiring a generation."""
        return self._receive_inactivity_timeout

    async def async_connect(self) -> None:
        """Connect and immediately start the sole socket reader."""
        async with self._lifecycle_lock:
            await self._connect_locked()

    async def async_close(self) -> None:
        """Stop the reader, fail pending work, and close the active socket."""
        async with self._lifecycle_lock:
            task = self._reader_task
            writer = self._writer
            closing_generation = self._active_connection_generation
            close_event_generation = (
                closing_generation
                if closing_generation is not None
                else self._generation
            )
            if task is not None or writer is not None:
                self._diagnostics.record_event(
                    LuxDiagnosticEventKind.CLOSE_STARTED,
                    close_event_generation,
                )
            self._reader_task = None
            self._reader = None
            self._writer = None
            self._active_connection_generation = None
            self._connection_lost = False
            self._generation += 1
            self._fail_pending(LuxPowerSessionClosedError("read session closed"))
            if task is not None and task is not asyncio.current_task():
                task.cancel()
            await self._close_writer(writer)
            if task is not None and task is not asyncio.current_task():
                # Generation invalidation already makes a cancellation-resistant
                # reader harmless. Bound shutdown so such a reader cannot hang it.
                with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                    await asyncio.wait_for(task, timeout=1)
            decoder_stats = self._decoder.stats()
            self._decoder_discarded_total += decoder_stats.discarded_bytes
            if decoder_stats.buffered_bytes:
                self._invalid_frames += 1
            self._decoder.reset()
            if task is not None or writer is not None:
                self._diagnostics.record_event(
                    LuxDiagnosticEventKind.CLOSE_COMPLETED,
                    close_event_generation,
                )

    async def async_reconnect(self, *, delay: float = 0) -> None:
        """Start a clean connection generation after an optional bounded delay."""
        if delay < 0:
            raise ValueError("reconnect delay cannot be negative")
        starting_generation = self._generation
        await self.async_close()
        closed_generation = starting_generation + 1
        if delay:
            await asyncio.sleep(delay)
        async with self._lifecycle_lock:
            if self._generation != closed_generation:
                raise LuxPowerSessionClosedError(
                    "reconnect superseded by another lifecycle operation"
                )
            await self._connect_locked()

    async def _connect_locked(self) -> None:
        """Connect while the lifecycle lock is held by the caller."""
        if self.connected:
            return
        self._connection_attempts += 1
        try:
            reader, writer = await asyncio.wait_for(
                self._connector(self._host, self._port),
                timeout=CONNECTION_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
            self._connection_failures += 1
            raise LuxPowerConnectionError(
                "frame-aware read connection could not be established"
            ) from exc
        try:
            self._configure_tcp_keepalive(writer)
        except BaseException:
            await self._close_writer(writer)
            raise
        self._generation += 1
        generation = self._generation
        decoder = LuxFrameDecoder()
        self._decoder = decoder
        self._reader = reader
        self._writer = writer
        self._active_connection_generation = generation
        self._connection_lost = False
        self._connections += 1
        self._connection_opened_diagnostic_monotonic = self._diagnostics.now()
        self._requests_on_generation = 0
        self._diagnostics.record_event(
            LuxDiagnosticEventKind.CONNECTION_OPENED,
            generation,
            at=self._connection_opened_diagnostic_monotonic,
        )
        self._reader_task = asyncio.create_task(
            self._reader_loop(generation, reader, decoder),
            name=f"lux-fc4-reader-{generation}",
        )

    async def async_read_input(
        self,
        start_register: int,
        register_count: int,
        *,
        timeout: float | None = None,
        drain_timeout: float | None = None,
        reply_timeout: float | None = None,
        context: LuxReadRequestContext | None = None,
    ) -> LuxReadObservation:
        """Issue one FC4 read and await only its exactly correlated response."""
        self._validate_read_range(start_register, register_count)
        if timeout is not None and (
            drain_timeout is not None or reply_timeout is not None
        ):
            raise ValueError(
                "timeout cannot be combined with drain_timeout or reply_timeout"
            )
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if drain_timeout is not None and drain_timeout <= 0:
            raise ValueError("drain_timeout must be positive")
        if reply_timeout is not None and reply_timeout <= 0:
            raise ValueError("reply_timeout must be positive")

        if timeout is not None:
            effective_drain_timeout = timeout
            effective_reply_timeout = timeout
            split_deadlines = False
        else:
            effective_drain_timeout = (
                self._drain_timeout if drain_timeout is None else drain_timeout
            )
            effective_reply_timeout = (
                self._reply_timeout if reply_timeout is None else reply_timeout
            )
            split_deadlines = (
                self._split_request_deadlines
                or drain_timeout is not None
                or reply_timeout is not None
            )

        async with self._request_lock:
            if not self.connected or self._writer is None:
                if self._connection_lost:
                    raise LuxPowerConnectionLostError(
                        "frame-aware read connection was lost"
                    )
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
            diagnostic = self._diagnostics.begin_request(
                generation=self._generation,
                register_start=start_register,
                register_count=register_count,
                timeout_seconds=(
                    effective_reply_timeout if not split_deadlines else None
                ),
                drain_timeout_seconds=effective_drain_timeout,
                reply_timeout_seconds=effective_reply_timeout,
                split_deadlines=split_deadlines,
                context=context or LuxReadRequestContext(),
                connection_opened_monotonic=(
                    self._connection_opened_diagnostic_monotonic
                ),
                requests_previously_on_generation=self._requests_on_generation,
            )
            self._requests_on_generation += 1
            pending = _PendingRead(
                start=start_register,
                count=register_count,
                generation=self._generation,
                sent_monotonic=self._monotonic(),
                future=future,
                diagnostic=diagnostic,
            )
            self._pending = pending
            self._explicit_requests += 1

            loop = asyncio.get_running_loop()
            combined_deadline = (
                None
                if split_deadlines
                else loop.time() + effective_reply_timeout
            )
            drain_completed = False
            outcome: LuxReadRequestOutcome | None = None
            try:
                self._writer.write(packet)
                self._diagnostics.mark_write_returned(diagnostic)
                drain_budget = (
                    effective_drain_timeout
                    if combined_deadline is None
                    else max(0, combined_deadline - loop.time())
                )
                await asyncio.wait_for(
                    self._writer.drain(), timeout=drain_budget
                )
                drain_completed = True
                self._diagnostics.mark_drain_completed(diagnostic)
                reply_budget = (
                    effective_reply_timeout
                    if combined_deadline is None
                    else max(0, combined_deadline - loop.time())
                )
                observation = await asyncio.wait_for(
                    asyncio.shield(future), timeout=reply_budget
                )
                outcome = LuxReadRequestOutcome.SUCCESS
                return observation
            except asyncio.TimeoutError as exc:
                # The timeout callback has already fired; this samples only the
                # state observed when the handler runs after rescheduling.
                diagnostic.future_done_when_timeout_handled = future.done()
                self._diagnostics.record_event(
                    (
                        LuxDiagnosticEventKind.REPLY_DEADLINE_EXPIRED
                        if drain_completed
                        else LuxDiagnosticEventKind.DRAIN_DEADLINE_EXPIRED
                    ),
                    pending.generation,
                    request=diagnostic,
                    register_start=start_register,
                    register_count=register_count,
                )
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                if drain_completed:
                    self._request_timeouts += 1
                    outcome = LuxReadRequestOutcome.RESPONSE_TIMEOUT
                else:
                    self._ambiguous_requests += 1
                    outcome = LuxReadRequestOutcome.AMBIGUOUS_DRAIN_TIMEOUT
                # FC4 has no transaction identifier. A late response on this
                # generation could otherwise satisfy a later same-range request.
                self._taint_current_generation(diagnostic)
                await self.async_close()
                if drain_completed:
                    raise LuxPowerReadTimeoutError(
                        f"timed out waiting for FC4 registers "
                        f"{start_register}-{start_register + register_count - 1}"
                    ) from exc
                raise LuxPowerAmbiguousRequestError(
                    "FC4 request drain timed out ambiguously"
                ) from exc
            except asyncio.CancelledError:
                outcome = LuxReadRequestOutcome.CANCELLED
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                self._taint_current_generation(diagnostic)
                await self.async_close()
                raise
            except (ConnectionError, OSError) as exc:
                outcome = LuxReadRequestOutcome.AMBIGUOUS_IO_FAILURE
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                self._ambiguous_requests += 1
                self._taint_current_generation(diagnostic)
                await self.async_close()
                raise LuxPowerAmbiguousRequestError(
                    "FC4 request failed ambiguously"
                ) from exc
            except LuxPowerReadRejectedError:
                outcome = LuxReadRequestOutcome.MODBUS_REJECTED
                raise
            except LuxPowerConnectionLostError:
                outcome = LuxReadRequestOutcome.CONNECTION_LOST
                raise
            except LuxPowerSessionClosedError:
                outcome = LuxReadRequestOutcome.SESSION_CLOSED
                raise
            except LuxPowerCommunicationError:
                outcome = LuxReadRequestOutcome.COMMUNICATION_FAILURE
                raise
            except Exception as exc:
                outcome = LuxReadRequestOutcome.AMBIGUOUS_IO_FAILURE
                if self._pending is pending:
                    self._pending = None
                if not future.done():
                    future.cancel()
                self._taint_current_generation(diagnostic)
                await self.async_close()
                raise LuxPowerAmbiguousRequestError(
                    "FC4 request ended ambiguously"
                ) from exc
            finally:
                self._diagnostics.finalize_request(
                    diagnostic,
                    outcome or LuxReadRequestOutcome.COMMUNICATION_FAILURE,
                )

    async def async_next_observation(
        self, *, timeout: float | None = None
    ) -> LuxReadObservation:
        """Return the next observation from the lazily enabled legacy stream.

        New consumers should call :meth:`subscribe_observations` before they
        expect delivery. The legacy stream begins with the first call to this
        method or :meth:`drain_observations`; observations received before then
        are available in the authoritative snapshot but are not replayed.
        """
        return await self._legacy_subscription().async_next(timeout=timeout)

    def drain_observations(self) -> tuple[LuxReadObservation, ...]:
        """Drain the lazily enabled legacy observation subscription."""
        return self._legacy_subscription().drain()

    def subscribe_observations(
        self, *, max_queue_size: int = 1024
    ) -> LuxObservationSubscription:
        """Create an independent, bounded, single-consumer observation stream."""
        if type(max_queue_size) is not int or max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer")
        identifier = self._next_observation_subscription_id
        self._next_observation_subscription_id += 1
        state = _ObservationSubscriptionState(
            queue=asyncio.Queue(maxsize=max_queue_size),
            capacity=max_queue_size,
        )
        self._observation_subscriptions[identifier] = state
        return LuxObservationSubscription(self, identifier, state)

    def _legacy_subscription(self) -> LuxObservationSubscription:
        subscription = self._legacy_observation_subscription
        if subscription is None or not subscription.snapshot().active:
            subscription = self.subscribe_observations()
            self._legacy_observation_subscription = subscription
        return subscription

    def _remove_observation_subscription(
        self,
        identifier: int,
        state: _ObservationSubscriptionState,
    ) -> None:
        if not state.active:
            return
        if self._observation_subscriptions.get(identifier) is state:
            del self._observation_subscriptions[identifier]
        state.active = False
        consumer_waiting = state.consumer_waiting
        while True:
            try:
                state.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        if consumer_waiting:
            state.queue.put_nowait(_OBSERVATION_SUBSCRIPTION_CLOSED)

    def snapshot(self) -> LuxReadSessionSnapshot:
        """Return detached values and per-register local observation times."""
        return LuxReadSessionSnapshot(
            input_registers=dict(self._input_registers),
            observed_at=LuxPowerObservationTimes(
                input_registers=dict(self._input_observed_at)
            ),
            input_sources=dict(self._input_sources),
            explicit_observed_at=dict(self._explicit_observed_at),
            unsolicited_observed_at=dict(self._unsolicited_observed_at),
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
            connection_attempts=self._connection_attempts,
            connection_failures=self._connection_failures,
            ambiguous_requests=self._ambiguous_requests,
            modbus_rejections=self._modbus_rejections,
            tcp_keepalive_applied_connections=(
                self._tcp_keepalive_applied_connections
            ),
            tcp_keepalive_idle_applied_connections=(
                self._tcp_keepalive_idle_applied_connections
            ),
            tcp_keepalive_configuration_failures=(
                self._tcp_keepalive_configuration_failures
            ),
            tcp_keepalive_configuration_unavailable=(
                self._tcp_keepalive_configuration_unavailable
            ),
            receive_inactivity_timeouts=self._receive_inactivity_timeouts,
        )

    def diagnostics(self) -> LuxReadDiagnosticsSnapshot:
        """Return detached sanitized request and event diagnostics."""
        return self._diagnostics.snapshot()

    async def _reader_loop(
        self,
        generation: int,
        reader: asyncio.StreamReader,
        decoder: LuxFrameDecoder,
    ) -> None:
        error: BaseException | None = None
        loop = asyncio.get_running_loop()
        receive_deadline = (
            loop.time() + self._receive_inactivity_timeout
            if self._receive_inactivity_timeout is not None
            else None
        )
        try:
            while generation == self._generation:
                partial_frame = bool(decoder.stats().buffered_bytes)
                read_timeout = _PARTIAL_FRAME_TIMEOUT if partial_frame else None
                if receive_deadline is not None:
                    remaining_inactivity = max(
                        0.0,
                        receive_deadline - loop.time(),
                    )
                    read_timeout = (
                        remaining_inactivity
                        if read_timeout is None
                        else min(read_timeout, remaining_inactivity)
                    )
                if read_timeout is not None:
                    try:
                        chunk = await asyncio.wait_for(
                            reader.read(_READER_CHUNK_SIZE),
                            timeout=read_timeout,
                        )
                    except asyncio.TimeoutError:
                        if generation != self._generation:
                            return
                        if (
                            receive_deadline is not None
                            and loop.time() >= receive_deadline
                        ):
                            self._receive_inactivity_timeouts += 1
                            self._diagnostics.record_event(
                                LuxDiagnosticEventKind.RECEIVE_INACTIVITY_TIMEOUT,
                                generation,
                                request=self._pending_diagnostic(generation),
                            )
                            raise ConnectionResetError(
                                "LuxPower application receive inactivity timeout"
                            )
                        if partial_frame and decoder.discard_partial():
                            self._invalid_frames += 1
                            self._diagnostics.observe_invalid(
                                generation,
                                self._pending_diagnostic(generation),
                                LuxInvalidFrameReason.PARTIAL_FRAME_TIMEOUT,
                            )
                        continue
                else:
                    chunk = await reader.read(_READER_CHUNK_SIZE)
                # A cancellation-resistant StreamReader implementation must not
                # be able to deliver bytes from a closed generation.
                if generation != self._generation:
                    return
                if not chunk:
                    raise ConnectionResetError("LuxPower socket closed")
                if self._receive_inactivity_timeout is not None:
                    receive_deadline = (
                        loop.time() + self._receive_inactivity_timeout
                    )
                self._bytes_received += len(chunk)
                malformed_before = decoder.stats().malformed_lengths
                frames = decoder.feed(chunk)
                malformed_after = decoder.stats().malformed_lengths
                self._invalid_frames += malformed_after - malformed_before
                for _ in range(malformed_after - malformed_before):
                    self._diagnostics.observe_invalid(
                        generation,
                        self._pending_diagnostic(generation),
                        LuxInvalidFrameReason.MALFORMED_LENGTH,
                    )
                for frame in frames:
                    self._route_frame(frame, generation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # reader failure must release every waiter
            error = exc
        finally:
            self._diagnostics.record_event(
                LuxDiagnosticEventKind.READER_EXIT,
                generation,
            )
            if generation == self._generation and error is not None:
                await self._reader_failed(generation, error)

    def _configure_tcp_keepalive(
        self,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Apply conservative TCP keepalive without making connect depend on it."""
        if not self._tcp_keepalive:
            return
        get_extra_info = getattr(writer, "get_extra_info", None)
        if get_extra_info is None:
            self._tcp_keepalive_configuration_unavailable += 1
            return
        try:
            transport_socket = get_extra_info("socket")
        except Exception:
            self._tcp_keepalive_configuration_failures += 1
            return
        if transport_socket is None or not hasattr(transport_socket, "setsockopt"):
            self._tcp_keepalive_configuration_unavailable += 1
            return
        try:
            transport_socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1,
            )
        except Exception:
            self._tcp_keepalive_configuration_failures += 1
            return
        self._tcp_keepalive_applied_connections += 1

        idle_option = getattr(socket, "TCP_KEEPIDLE", None)
        if idle_option is None:
            idle_option = getattr(socket, "TCP_KEEPALIVE", None)
        if idle_option is None:
            self._tcp_keepalive_configuration_unavailable += 1
            return
        try:
            transport_socket.setsockopt(
                socket.IPPROTO_TCP,
                idle_option,
                self._tcp_keepalive_idle_seconds,
            )
        except Exception:
            self._tcp_keepalive_configuration_failures += 1
            return
        self._tcp_keepalive_idle_applied_connections += 1

    def _route_frame(self, frame: bytes, generation: int) -> None:
        if generation != self._generation:
            return
        self._frames_received += 1
        response = LxpResponse(frame)

        if not response.packet_error and response.tcp_function == 193:
            # Integrity and semantics are not established; diagnostics only.
            self._function_193_frames += 1
            self._diagnostics.observe_fc193(
                generation,
                self._pending_diagnostic(generation),
            )
            return

        targeted = bool(
            not response.packet_error
            and response.tcp_function == _LxpRequestBuilder.TRANSLATED_DATA
            and response.dongle_serial == self._dongle_serial
            and response.serial_number == self._inverter_serial
        )
        pending = self._pending
        if pending is not None and pending.generation != generation:
            pending = None
        if (
            targeted
            and pending is not None
            and response.device_function == (READ_INPUT_FUNCTION_CODE | 0x80)
            and response.register == pending.start
        ):
            self._pending = None
            self._modbus_rejections += 1
            self._diagnostics.record_event(
                LuxDiagnosticEventKind.MODBUS_REJECTED,
                generation,
                request=pending.diagnostic,
                register_start=response.register,
                register_count=pending.count,
            )
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
            self._diagnostics.observe_invalid(
                generation,
                self._pending_diagnostic(generation),
                self._invalid_frame_reason(
                    response,
                    values,
                    count,
                    contiguous,
                ),
            )
            return

        explicit = bool(
            pending is not None
            and response.register == pending.start
            and count == pending.count
        )
        source = (
            LuxObservationSource.EXPLICIT
            if explicit
            else LuxObservationSource.UNSOLICITED
        )
        key = (response.register, count)
        duplicate = self._last_block_values.get(key) == values
        observed_at = require_aware_utc(self._clock())
        self._input_registers.update(values)
        self._input_observed_at.update(
            {register: observed_at for register in values}
        )
        self._input_sources.update({register: source for register in values})
        source_times = (
            self._explicit_observed_at
            if explicit
            else self._unsolicited_observed_at
        )
        source_times.update({register: observed_at for register in values})
        self._last_block_values[key] = dict(values)
        self._validated_fc4_frames += 1
        if duplicate:
            self._duplicate_fc4_frames += 1

        self._observation_sequence += 1
        observation = LuxReadObservation(
            register_start=response.register,
            register_count=count,
            values=dict(values),
            observed_at=observed_at,
            explicit_response=explicit,
            duplicate=duplicate,
            sequence=self._observation_sequence,
        )
        self._publish_observation(observation)

        if explicit and pending is not None:
            self._diagnostics.observe_matched(
                pending.diagnostic,
                response.register,
                count,
            )
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
            self._diagnostics.observe_unmatched(
                generation,
                response.register,
                count,
                self._pending_diagnostic(generation),
            )
            self._unmatched_fc4_observations += 1
            self._operational_registers_unmatched += sum(
                input_register_group(register) is TelemetryGroup.OPERATIONAL
                for register in values
            )

    def _invalid_frame_reason(
        self,
        response: LxpResponse,
        values: Mapping[int, int],
        count: int,
        contiguous: bool,
    ) -> LuxInvalidFrameReason:
        """Classify a rejected frame without exposing packet or value content."""
        if response.packet_error:
            return LuxInvalidFrameReason.PACKET_INTEGRITY
        if response.tcp_function != _LxpRequestBuilder.TRANSLATED_DATA:
            return LuxInvalidFrameReason.TCP_FUNCTION
        if response.dongle_serial != self._dongle_serial:
            return LuxInvalidFrameReason.DONGLE_TARGET_MISMATCH
        if response.serial_number != self._inverter_serial:
            return LuxInvalidFrameReason.INVERTER_TARGET_MISMATCH
        if response.device_function != READ_INPUT_FUNCTION_CODE:
            return LuxInvalidFrameReason.DEVICE_FUNCTION
        if response.exception:
            return LuxInvalidFrameReason.MODBUS_EXCEPTION
        if not values:
            return LuxInvalidFrameReason.EMPTY_VALUES
        if response.address_action != 1:
            return LuxInvalidFrameReason.ADDRESS_ACTION
        if response.data_length != response.frame_length - 14:
            return LuxInvalidFrameReason.DATA_LENGTH
        if (
            response.value_length != count * 2
            or len(response.value) != response.value_length
        ):
            return LuxInvalidFrameReason.VALUE_LENGTH
        if not contiguous:
            return LuxInvalidFrameReason.NONCONTIGUOUS_REGISTERS
        if response.register < 0 or response.register + count > 750:
            return LuxInvalidFrameReason.REGISTER_RANGE
        return LuxInvalidFrameReason.DATA_SANITY

    def _publish_observation(self, observation: LuxReadObservation) -> None:
        for state in tuple(self._observation_subscriptions.values()):
            if not state.active:
                continue
            if state.queue.full():
                with suppress(asyncio.QueueEmpty):
                    state.queue.get_nowait()
                    state.observations_dropped += 1
                    self._observation_queue_drops += 1
            state.queue.put_nowait(observation)
            state.observations_received += 1
            state.last_sequence_received = observation.sequence

    async def _reader_failed(self, generation: int, error: BaseException) -> None:
        async with self._lifecycle_lock:
            if generation != self._generation:
                return
            self._diagnostics.record_event(
                LuxDiagnosticEventKind.CONNECTION_LOST,
                generation,
                request=self._pending_diagnostic(generation),
            )
            writer = self._writer
            self._reader = None
            self._writer = None
            self._active_connection_generation = None
            self._reader_task = None
            self._generation += 1
            self._connection_lost = True
            self._connection_losses += 1
            self._fail_pending(
                LuxPowerConnectionLostError("frame-aware read connection lost")
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
            self._diagnostics.record_event(
                LuxDiagnosticEventKind.PENDING_FAILED,
                pending.generation,
                request=pending.diagnostic,
                register_start=pending.start,
                register_count=pending.count,
            )
            pending.future.set_exception(error)

    def _taint_current_generation(
        self, diagnostic: _DiagnosticRequestState | None = None
    ) -> None:
        """Synchronously reject all further bytes before cleanup can await."""
        if diagnostic is not None:
            self._diagnostics.mark_generation_invalidated(diagnostic)
        self._generation += 1

    def _pending_diagnostic(
        self, generation: int
    ) -> _DiagnosticRequestState | None:
        pending = self._pending
        if pending is None or pending.generation != generation:
            return None
        return pending.diagnostic

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
