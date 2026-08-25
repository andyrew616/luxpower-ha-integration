"""Bounded, sanitized diagnostics for experimental read-only FC4 sessions."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Callable, Mapping


class LuxReadPurpose(str, Enum):
    """Why one explicit FC4 request was issued."""

    DIRECT = "direct"
    NORMAL_PROFILE = "normal_profile"
    RECOVERY_REACQUISITION = "recovery_reacquisition"
    FORCED_PREFLIGHT = "forced_preflight"
    OPERATIONAL_PROBE = "operational_probe"
    FULL_SCAN = "full_scan"


class LuxReadRequestOutcome(str, Enum):
    """Sanitized terminal result of one explicit FC4 request."""

    SUCCESS = "success"
    RESPONSE_TIMEOUT = "response_timeout"
    AMBIGUOUS_DRAIN_TIMEOUT = "ambiguous_drain_timeout"
    AMBIGUOUS_IO_FAILURE = "ambiguous_io_failure"
    CONNECTION_LOST = "connection_lost"
    SESSION_CLOSED = "session_closed"
    MODBUS_REJECTED = "modbus_rejected"
    CANCELLED = "cancelled"
    COMMUNICATION_FAILURE = "communication_failure"


class LuxDiagnosticEventKind(str, Enum):
    """Safe event types retained by the in-memory diagnostic journal."""

    CONNECTION_OPENED = "connection_opened"
    REQUEST_REGISTERED = "request_registered"
    WRITE_RETURNED = "write_returned"
    DRAIN_COMPLETED = "drain_completed"
    MATCHED_FC4 = "matched_fc4"
    UNMATCHED_FC4 = "unmatched_fc4"
    FC193 = "fc193"
    INVALID_FRAME = "invalid_frame"
    MODBUS_REJECTED = "modbus_rejected"
    PENDING_FAILED = "pending_failed"
    DEADLINE_EXPIRED = "deadline_expired"
    DRAIN_DEADLINE_EXPIRED = "drain_deadline_expired"
    REPLY_DEADLINE_EXPIRED = "reply_deadline_expired"
    GENERATION_TAINTED = "generation_tainted"
    CLOSE_STARTED = "close_started"
    CLOSE_COMPLETED = "close_completed"
    READER_EXIT = "reader_exit"
    CONNECTION_LOST = "connection_lost"
    REQUEST_TERMINAL = "request_terminal"


@dataclass(frozen=True)
class LuxReadRequestContext:
    """Optional profile state supplied without coupling transport to the profile."""

    purpose: LuxReadPurpose = LuxReadPurpose.DIRECT
    profile_worst_age_seconds: float | None = None
    profile_health: str | None = None

    def __post_init__(self) -> None:
        if self.profile_worst_age_seconds is not None:
            if self.profile_worst_age_seconds < 0:
                raise ValueError("profile_worst_age_seconds cannot be negative")
        if self.profile_health not in (None, "healthy", "recovering", "degraded"):
            raise ValueError("profile_health must be a supported acquisition state")


@dataclass(frozen=True)
class LuxReadDiagnosticEvent:
    """One sanitized event ordered by an in-process sequence number."""

    sequence: int
    relative_monotonic_seconds: float
    generation: int
    kind: LuxDiagnosticEventKind
    request_sequence: int | None = None
    register_start: int | None = None
    register_count: int | None = None


@dataclass(frozen=True)
class LuxReadRequestDiagnostic:
    """Terminal summary for one explicit request, without packet or value data."""

    request_sequence: int
    generation: int
    purpose: LuxReadPurpose
    register_start: int
    register_count: int
    started_monotonic_seconds: float
    time_since_previous_request_start_seconds: float | None
    time_since_previous_accepted_response_seconds: float | None
    time_since_previous_unmatched_fc4_seconds: float | None
    time_since_previous_fc193_seconds: float | None
    connection_age_seconds: float
    requests_previously_on_generation: int
    profile_worst_age_seconds: float | None
    profile_health: str | None
    timeout_budget_ms: float
    drain_timeout_budget_ms: float
    reply_timeout_budget_ms: float
    split_deadlines: bool
    write_returned: bool
    drain_completed: bool
    drain_duration_ms: float | None
    reply_wait_duration_ms: float | None
    matching_response_routed: bool
    matched_before_drain_completion_observed: bool
    accepted_response_latency_ms: float | None
    unmatched_fc4_while_pending: int
    fc193_while_pending: int
    invalid_frames_while_pending: int
    future_done_when_timeout_handled: bool
    generation_invalidated: bool
    outcome: LuxReadRequestOutcome
    elapsed_ms: float
    first_event_sequence: int
    terminal_event_sequence: int

    @property
    def register_end(self) -> int:
        return self.register_start + self.register_count - 1


@dataclass(frozen=True)
class LuxTimeoutDiagnosticEpisode:
    """Bounded event context captured for one response timeout."""

    request: LuxReadRequestDiagnostic
    recent_events: tuple[LuxReadDiagnosticEvent, ...]
    late_old_generation_frame_observation_supported: bool = False


@dataclass(frozen=True)
class LuxReadDiagnosticsSnapshot:
    """Detached bounded diagnostic state suitable for sanitized artifacts."""

    schema_version: int
    run_duration_seconds: float
    event_capacity: int
    events_total: int
    events_dropped: int
    events: tuple[LuxReadDiagnosticEvent, ...]
    request_capacity: int
    requests_total: int
    requests_dropped: int
    requests: tuple[LuxReadRequestDiagnostic, ...]
    failure_capacity: int
    failures_total: int
    failures_dropped: int
    timeout_episodes: tuple[LuxTimeoutDiagnosticEpisode, ...]
    outcome_counts: Mapping[str, int] = field(default_factory=dict)
    purpose_counts: Mapping[str, int] = field(default_factory=dict)
    block_attempt_counts: Mapping[str, int] = field(default_factory=dict)
    late_old_generation_frame_observation_supported: bool = False


@dataclass
class _DiagnosticRequestState:
    """Mutable hot-path state used only until a request terminalizes."""

    request_sequence: int
    generation: int
    context: LuxReadRequestContext
    register_start: int
    register_count: int
    timeout_budget_ms: float
    drain_timeout_budget_ms: float
    reply_timeout_budget_ms: float
    split_deadlines: bool
    started_monotonic: float
    started_relative: float
    previous_request_elapsed: float | None
    previous_response_elapsed: float | None
    previous_unmatched_elapsed: float | None
    previous_fc193_elapsed: float | None
    connection_age: float
    requests_previously_on_generation: int
    first_event_sequence: int
    write_returned: bool = False
    drain_started_monotonic: float | None = None
    drain_completed: bool = False
    drain_completed_monotonic: float | None = None
    drain_duration_ms: float | None = None
    reply_wait_duration_ms: float | None = None
    matching_response_routed: bool = False
    matched_before_drain_completion_observed: bool = False
    accepted_response_latency_ms: float | None = None
    unmatched_fc4_while_pending: int = 0
    fc193_while_pending: int = 0
    invalid_frames_while_pending: int = 0
    future_done_when_timeout_handled: bool = False
    generation_invalidated: bool = False
    finalized: bool = False


class LuxReadDiagnosticJournal:
    """Non-blocking bounded journal which never stores packets or register values."""

    SCHEMA_VERSION = 2

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        event_capacity: int = 512,
        request_capacity: int = 4096,
        failure_capacity: int = 64,
        failure_event_context: int = 32,
    ) -> None:
        if min(
            event_capacity,
            request_capacity,
            failure_capacity,
            failure_event_context,
        ) <= 0:
            raise ValueError("diagnostic retention capacities must be positive")
        self._monotonic = monotonic
        self._origin = monotonic()
        self._event_capacity = event_capacity
        self._request_capacity = request_capacity
        self._failure_capacity = failure_capacity
        self._failure_event_context = failure_event_context
        self._events: deque[LuxReadDiagnosticEvent] = deque(maxlen=event_capacity)
        self._requests: deque[LuxReadRequestDiagnostic] = deque(
            maxlen=request_capacity
        )
        self._timeout_episodes: deque[LuxTimeoutDiagnosticEpisode] = deque(
            maxlen=failure_capacity
        )
        self._event_total = 0
        self._request_total = 0
        self._failure_total = 0
        self._next_request_sequence = 1
        self._outcome_counts: Counter[str] = Counter()
        self._purpose_counts: Counter[str] = Counter()
        self._block_attempt_counts: Counter[str] = Counter()
        self._last_request_started: float | None = None
        self._last_accepted_response: float | None = None
        self._last_unmatched_fc4: float | None = None
        self._last_fc193: float | None = None

    @property
    def request_total(self) -> int:
        return self._request_total

    def now(self) -> float:
        return self._monotonic()

    def record_event(
        self,
        kind: LuxDiagnosticEventKind,
        generation: int,
        *,
        request: _DiagnosticRequestState | None = None,
        register_start: int | None = None,
        register_count: int | None = None,
        at: float | None = None,
    ) -> LuxReadDiagnosticEvent:
        observed = self.now() if at is None else at
        self._event_total += 1
        event = LuxReadDiagnosticEvent(
            sequence=self._event_total,
            relative_monotonic_seconds=round(observed - self._origin, 6),
            generation=generation,
            kind=kind,
            request_sequence=(request.request_sequence if request else None),
            register_start=register_start,
            register_count=register_count,
        )
        self._events.append(event)
        return event

    def begin_request(
        self,
        *,
        generation: int,
        register_start: int,
        register_count: int,
        timeout_seconds: float | None = None,
        drain_timeout_seconds: float | None = None,
        reply_timeout_seconds: float | None = None,
        split_deadlines: bool = False,
        context: LuxReadRequestContext,
        connection_opened_monotonic: float,
        requests_previously_on_generation: int,
    ) -> _DiagnosticRequestState:
        if timeout_seconds is None and (
            drain_timeout_seconds is None or reply_timeout_seconds is None
        ):
            raise ValueError(
                "timeout_seconds or both phase timeout values are required"
            )
        legacy_timeout = timeout_seconds
        effective_drain_timeout = (
            legacy_timeout
            if drain_timeout_seconds is None
            else drain_timeout_seconds
        )
        effective_reply_timeout = (
            legacy_timeout
            if reply_timeout_seconds is None
            else reply_timeout_seconds
        )
        if (
            effective_drain_timeout is None
            or effective_reply_timeout is None
            or effective_drain_timeout <= 0
            or effective_reply_timeout <= 0
        ):
            raise ValueError("diagnostic timeout budgets must be positive")
        compatibility_timeout = (
            legacy_timeout
            if legacy_timeout is not None
            else effective_reply_timeout
        )
        now = self.now()
        request_sequence = self._next_request_sequence
        self._next_request_sequence += 1
        state = _DiagnosticRequestState(
            request_sequence=request_sequence,
            generation=generation,
            context=context,
            register_start=register_start,
            register_count=register_count,
            timeout_budget_ms=compatibility_timeout * 1000,
            drain_timeout_budget_ms=effective_drain_timeout * 1000,
            reply_timeout_budget_ms=effective_reply_timeout * 1000,
            split_deadlines=split_deadlines,
            started_monotonic=now,
            started_relative=round(now - self._origin, 6),
            previous_request_elapsed=self._elapsed(now, self._last_request_started),
            previous_response_elapsed=self._elapsed(
                now, self._last_accepted_response
            ),
            previous_unmatched_elapsed=self._elapsed(now, self._last_unmatched_fc4),
            previous_fc193_elapsed=self._elapsed(now, self._last_fc193),
            connection_age=max(0.0, now - connection_opened_monotonic),
            requests_previously_on_generation=requests_previously_on_generation,
            first_event_sequence=self._event_total + 1,
        )
        self._last_request_started = now
        self._purpose_counts[context.purpose.value] += 1
        self._block_attempt_counts[f"{register_start}:{register_count}"] += 1
        self.record_event(
            LuxDiagnosticEventKind.REQUEST_REGISTERED,
            generation,
            request=state,
            register_start=register_start,
            register_count=register_count,
            at=now,
        )
        return state

    def mark_write_returned(self, state: _DiagnosticRequestState) -> None:
        now = self.now()
        state.write_returned = True
        state.drain_started_monotonic = now
        self.record_event(
            LuxDiagnosticEventKind.WRITE_RETURNED,
            state.generation,
            request=state,
            register_start=state.register_start,
            register_count=state.register_count,
            at=now,
        )

    def mark_drain_completed(self, state: _DiagnosticRequestState) -> None:
        now = self.now()
        state.drain_completed = True
        state.drain_completed_monotonic = now
        if state.drain_started_monotonic is not None:
            state.drain_duration_ms = (
                now - state.drain_started_monotonic
            ) * 1000
        self.record_event(
            LuxDiagnosticEventKind.DRAIN_COMPLETED,
            state.generation,
            request=state,
            register_start=state.register_start,
            register_count=state.register_count,
            at=now,
        )

    def observe_matched(
        self,
        state: _DiagnosticRequestState,
        register_start: int,
        register_count: int,
    ) -> None:
        now = self.now()
        state.matching_response_routed = True
        # This records task-observed event order only. A drain awaitable may
        # already be complete before its awaiting task resumes to mark it.
        state.matched_before_drain_completion_observed = not state.drain_completed
        state.accepted_response_latency_ms = (
            now - state.started_monotonic
        ) * 1000
        state.reply_wait_duration_ms = (
            0.0
            if state.drain_completed_monotonic is None
            else (now - state.drain_completed_monotonic) * 1000
        )
        self._last_accepted_response = now
        self.record_event(
            LuxDiagnosticEventKind.MATCHED_FC4,
            state.generation,
            request=state,
            register_start=register_start,
            register_count=register_count,
            at=now,
        )

    def observe_unmatched(
        self,
        generation: int,
        register_start: int,
        register_count: int,
        pending: _DiagnosticRequestState | None,
    ) -> None:
        now = self.now()
        self._last_unmatched_fc4 = now
        if pending is not None:
            pending.unmatched_fc4_while_pending += 1
        self.record_event(
            LuxDiagnosticEventKind.UNMATCHED_FC4,
            generation,
            request=pending,
            register_start=register_start,
            register_count=register_count,
            at=now,
        )

    def observe_fc193(
        self,
        generation: int,
        pending: _DiagnosticRequestState | None,
    ) -> None:
        now = self.now()
        self._last_fc193 = now
        if pending is not None:
            pending.fc193_while_pending += 1
        self.record_event(
            LuxDiagnosticEventKind.FC193,
            generation,
            request=pending,
            at=now,
        )

    def observe_invalid(
        self,
        generation: int,
        pending: _DiagnosticRequestState | None,
    ) -> None:
        if pending is not None:
            pending.invalid_frames_while_pending += 1
        self.record_event(
            LuxDiagnosticEventKind.INVALID_FRAME,
            generation,
            request=pending,
        )

    def mark_generation_invalidated(self, state: _DiagnosticRequestState) -> None:
        state.generation_invalidated = True
        self.record_event(
            LuxDiagnosticEventKind.GENERATION_TAINTED,
            state.generation,
            request=state,
            register_start=state.register_start,
            register_count=state.register_count,
        )

    def finalize_request(
        self,
        state: _DiagnosticRequestState,
        outcome: LuxReadRequestOutcome,
    ) -> LuxReadRequestDiagnostic:
        if state.finalized:
            raise RuntimeError("diagnostic request already finalized")
        state.finalized = True
        now = self.now()
        terminal = self.record_event(
            LuxDiagnosticEventKind.REQUEST_TERMINAL,
            state.generation,
            request=state,
            register_start=state.register_start,
            register_count=state.register_count,
            at=now,
        )
        summary = LuxReadRequestDiagnostic(
            request_sequence=state.request_sequence,
            generation=state.generation,
            purpose=state.context.purpose,
            register_start=state.register_start,
            register_count=state.register_count,
            started_monotonic_seconds=state.started_relative,
            time_since_previous_request_start_seconds=self._rounded(
                state.previous_request_elapsed
            ),
            time_since_previous_accepted_response_seconds=self._rounded(
                state.previous_response_elapsed
            ),
            time_since_previous_unmatched_fc4_seconds=self._rounded(
                state.previous_unmatched_elapsed
            ),
            time_since_previous_fc193_seconds=self._rounded(
                state.previous_fc193_elapsed
            ),
            connection_age_seconds=round(state.connection_age, 6),
            requests_previously_on_generation=(
                state.requests_previously_on_generation
            ),
            profile_worst_age_seconds=self._rounded(
                state.context.profile_worst_age_seconds
            ),
            profile_health=state.context.profile_health,
            timeout_budget_ms=round(state.timeout_budget_ms, 3),
            drain_timeout_budget_ms=round(
                state.drain_timeout_budget_ms, 3
            ),
            reply_timeout_budget_ms=round(
                state.reply_timeout_budget_ms, 3
            ),
            split_deadlines=state.split_deadlines,
            write_returned=state.write_returned,
            drain_completed=state.drain_completed,
            drain_duration_ms=self._rounded(state.drain_duration_ms, digits=3),
            reply_wait_duration_ms=self._rounded(
                state.reply_wait_duration_ms, digits=3
            ),
            matching_response_routed=state.matching_response_routed,
            matched_before_drain_completion_observed=(
                state.matched_before_drain_completion_observed
            ),
            accepted_response_latency_ms=self._rounded(
                state.accepted_response_latency_ms, digits=3
            ),
            unmatched_fc4_while_pending=state.unmatched_fc4_while_pending,
            fc193_while_pending=state.fc193_while_pending,
            invalid_frames_while_pending=state.invalid_frames_while_pending,
            future_done_when_timeout_handled=(
                state.future_done_when_timeout_handled
            ),
            generation_invalidated=state.generation_invalidated,
            outcome=outcome,
            elapsed_ms=round((now - state.started_monotonic) * 1000, 3),
            first_event_sequence=state.first_event_sequence,
            terminal_event_sequence=terminal.sequence,
        )
        self._request_total += 1
        self._outcome_counts[outcome.value] += 1
        self._requests.append(summary)
        if outcome is LuxReadRequestOutcome.RESPONSE_TIMEOUT:
            self._failure_total += 1
            recent_events = tuple(self._events)[-self._failure_event_context :]
            self._timeout_episodes.append(
                LuxTimeoutDiagnosticEpisode(
                    request=summary,
                    recent_events=recent_events,
                )
            )
        return summary

    def snapshot(self) -> LuxReadDiagnosticsSnapshot:
        return LuxReadDiagnosticsSnapshot(
            schema_version=self.SCHEMA_VERSION,
            run_duration_seconds=round(self.now() - self._origin, 6),
            event_capacity=self._event_capacity,
            events_total=self._event_total,
            events_dropped=max(0, self._event_total - len(self._events)),
            events=tuple(self._events),
            request_capacity=self._request_capacity,
            requests_total=self._request_total,
            requests_dropped=max(0, self._request_total - len(self._requests)),
            requests=tuple(self._requests),
            failure_capacity=self._failure_capacity,
            failures_total=self._failure_total,
            failures_dropped=max(
                0, self._failure_total - len(self._timeout_episodes)
            ),
            timeout_episodes=tuple(self._timeout_episodes),
            outcome_counts=dict(self._outcome_counts),
            purpose_counts=dict(self._purpose_counts),
            block_attempt_counts=dict(self._block_attempt_counts),
        )

    @staticmethod
    def _elapsed(now: float, previous: float | None) -> float | None:
        return None if previous is None else max(0.0, now - previous)

    @staticmethod
    def _rounded(value: float | None, *, digits: int = 6) -> float | None:
        return round(value, digits) if value is not None else None
