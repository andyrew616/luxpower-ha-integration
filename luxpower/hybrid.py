"""Experimental read-only hybrid telemetry over the frame-aware Lux session."""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Mapping, Sequence

from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSubscription,
    LuxObservationSource,
    LuxReadSession,
    LuxReadSessionMetrics,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.const import READ_TIMEOUT, TOTAL_REGISTERS
from custom_components.lxp_modbus.exceptions import (
    LuxPowerAmbiguousRequestError,
    LuxPowerCommunicationError,
    LuxPowerConnectionError,
    LuxPowerConnectionLostError,
    LuxPowerReadRejectedError,
    LuxPowerReadTimeoutError,
    LuxPowerRecoveryExhaustedError,
    LuxPowerSessionClosedError,
)
from custom_components.lxp_modbus.observation import utc_now
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    EnergyFlowSnapshot,
    DiagnosticReadProfile,
    DiagnosticSnapshot,
    InputReadBlock,
)
from custom_components.lxp_modbus.recovery import (
    AcquisitionHealth,
    RecoveryEvent,
    RecoveryFailureKind,
    RecoveryMetrics,
    RecoveryPolicy,
)
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
    input_registers_for_group,
)
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxReadDiagnosticsSnapshot,
    LuxReadPurpose,
    LuxReadRequestContext,
)

HYBRID_SCHEMA_VERSION = 1
HYBRID_VERSION = "1.0"
HARDWARE_READ_BLOCK_SIZE = 40
RECOVERY_EVENT_CAPACITY = 512


OPERATIONAL_READ_BLOCKS = tuple(
    InputReadBlock(start, HARDWARE_READ_BLOCK_SIZE)
    for start in range(0, 240, HARDWARE_READ_BLOCK_SIZE)
)
FULL_INPUT_READ_BLOCKS = tuple(
    InputReadBlock(
        start,
        min(HARDWARE_READ_BLOCK_SIZE, TOTAL_REGISTERS - start),
    )
    for start in range(0, TOTAL_REGISTERS, HARDWARE_READ_BLOCK_SIZE)
)


def _validate_operational_blocks() -> None:
    covered = {
        register
        for block in OPERATIONAL_READ_BLOCKS
        for register in block.addresses()
    }
    missing = input_registers_for_group(TelemetryGroup.OPERATIONAL) - covered
    if missing:
        raise RuntimeError(f"operational blocks miss registers: {sorted(missing)}")


_validate_operational_blocks()


@dataclass(frozen=True)
class HybridRefreshResult:
    """Outcome of one stale-driven operational refresh pass."""

    requested_blocks: tuple[InputReadBlock, ...]
    fresh_blocks_skipped: tuple[InputReadBlock, ...]
    duration_ms: float


@dataclass(frozen=True)
class HybridProfileRefreshResult:
    """One freshness-driven read-profile acquisition decision."""

    requested_blocks: tuple[InputReadBlock, ...]
    fresh_blocks_skipped: tuple[InputReadBlock, ...]
    blocks_satisfied_unsolicited: tuple[InputReadBlock, ...]
    duration_ms: float


@dataclass(frozen=True)
class HybridProfileMetrics:
    """Source-aware profile decisions, excluding forced reads and full scans."""

    explicit_requests_attempted: int
    explicit_requests_avoided_unsolicited: int
    blocks_satisfied_unsolicited: int

    @property
    def avoidance_percent(self) -> float | None:
        opportunities = (
            self.explicit_requests_attempted
            + self.explicit_requests_avoided_unsolicited
        )
        if not opportunities:
            return None
        return 100 * self.explicit_requests_avoided_unsolicited / opportunities


@dataclass
class _ActiveRecovery:
    failure_kind: RecoveryFailureKind
    block: InputReadBlock
    started_monotonic: float
    episode_started_at: str
    recovery_started_at: str
    cooldown_seconds: float
    reconnect_started_at: str | None = None
    connection_established_at: str | None = None
    failure_to_connection_seconds: float | None = None
    maximum_profile_age_seconds: float | None = None
    connection_dial_attempts: int = 0
    failed_connection_dial_attempts: int = 0


class LuxPowerHybridReadClient:
    """Experimental persistent FC4 client with freshness-driven read suppression.

    This API exposes no write operation and is not wired into Home Assistant.
    """

    def __init__(
        self,
        host: str,
        dongle_serial: str,
        inverter_serial: str,
        *,
        port: int = 8000,
        freshness_target: timedelta = timedelta(seconds=5),
        full_scan_interval: timedelta = timedelta(seconds=60),
        profile: EnergyFlowReadProfile | DiagnosticReadProfile | None = None,
        session: LuxReadSession | None = None,
        recovery_policy: RecoveryPolicy | None = None,
        tcp_keepalive: bool = True,
        tcp_keepalive_idle_seconds: int = 60,
        receive_inactivity_timeout: float | None = 900.0,
        monotonic=time.monotonic,
    ) -> None:
        if freshness_target.total_seconds() <= 0:
            raise ValueError("freshness_target must be positive")
        if full_scan_interval.total_seconds() <= 0:
            raise ValueError("full_scan_interval must be positive")
        self._session = session or LuxReadSession(
            host,
            dongle_serial,
            inverter_serial,
            port=port,
            tcp_keepalive=tcp_keepalive,
            tcp_keepalive_idle_seconds=tcp_keepalive_idle_seconds,
            receive_inactivity_timeout=receive_inactivity_timeout,
        )
        self._freshness_target = freshness_target
        self._full_scan_interval = full_scan_interval
        self._profile = profile
        self._recovery_policy = recovery_policy
        self._monotonic = monotonic
        self._last_full_scan_completed_at: datetime | None = None
        self._profile_explicit_requests = 0
        self._profile_unsolicited_avoided = 0
        self._profile_accounted_observations: dict[
            InputReadBlock, tuple[datetime, ...]
        ] = {}
        self._last_profile_request_block: InputReadBlock | None = None
        self._profile_refresh_lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._shutdown.set()
        self._health = AcquisitionHealth.DEGRADED
        self._reconnect_attempt_times: deque[float] = deque()
        self._recovery_events: deque[RecoveryEvent] = deque(
            maxlen=RECOVERY_EVENT_CAPACITY
        )
        self._recovery_events_recorded = 0
        self._active_recovery: _ActiveRecovery | None = None
        self._timeout_count = 0
        self._connection_loss_count = 0
        self._connection_establishment_failure_count = 0
        self._ambiguous_request_count = 0
        self._reconnect_attempts = 0
        self._successful_reconnects = 0
        self._failed_reconnects = 0
        self._completed_recoveries = 0
        self._retry_budget_exhausted = 0
        self._acquisitions_abandoned = 0
        self._connection_dial_attempts = 0
        self._failed_connection_dial_attempts = 0

    async def async_connect(self) -> None:
        self._shutdown.clear()
        try:
            await self._session.async_connect()
        except BaseException:
            self._health = AcquisitionHealth.DEGRADED
            raise
        self._health = (
            AcquisitionHealth.DEGRADED
            if self._profile is not None
            else AcquisitionHealth.HEALTHY
        )

    async def async_close(self) -> None:
        self._shutdown.set()
        self._health = AcquisitionHealth.DEGRADED
        await self._session.async_close()

    async def async_passive(self, seconds: float) -> None:
        """Receive and route frames without sending a request."""
        if seconds <= 0:
            raise ValueError("passive duration must be positive")
        await asyncio.sleep(seconds)

    def snapshot(self) -> LuxReadSessionSnapshot:
        return self._session.snapshot()

    def metrics(self) -> LuxReadSessionMetrics:
        return self._session.metrics()

    def diagnostics(self) -> LuxReadDiagnosticsSnapshot:
        """Return detached sanitized request-lifecycle diagnostics."""
        return self._session.diagnostics()

    @property
    def request_timeout_seconds(self) -> float:
        """Historic combined deadline retained for compatible consumers."""
        return self._session.request_timeout_seconds

    @property
    def drain_timeout_seconds(self) -> float:
        """Configured socket-drain deadline for explicit FC4 reads."""
        return self._session.drain_timeout_seconds

    @property
    def reply_timeout_seconds(self) -> float:
        """Configured correlated-reply deadline for explicit FC4 reads."""
        return self._session.reply_timeout_seconds

    @property
    def split_request_deadlines(self) -> bool:
        """Whether drain and reply phases use independent budgets."""
        return self._session.split_request_deadlines

    @property
    def tcp_keepalive_enabled(self) -> bool:
        """Whether the session requests best-effort OS TCP keepalive."""
        return self._session.tcp_keepalive_enabled

    @property
    def tcp_keepalive_idle_seconds(self) -> int:
        """Requested idle time before OS TCP keepalive probing begins."""
        return self._session.tcp_keepalive_idle_seconds

    @property
    def receive_inactivity_timeout_seconds(self) -> float | None:
        """Configured application-byte inactivity deadline."""
        return self._session.receive_inactivity_timeout_seconds

    @property
    def profile(self) -> EnergyFlowReadProfile | DiagnosticReadProfile | None:
        """The resolved experimental profile, if configured."""
        return self._profile

    @property
    def recovery_policy(self) -> RecoveryPolicy | None:
        """The opt-in sanitized recovery policy, if configured."""
        return self._recovery_policy

    def profile_snapshot(self) -> EnergyFlowSnapshot | DiagnosticSnapshot:
        """Return a typed profile snapshot with truthful derived freshness."""
        if self._profile is None:
            raise ValueError("no read profile configured")
        return self._profile.snapshot(self._session.snapshot())

    def profile_metrics(self) -> HybridProfileMetrics:
        return HybridProfileMetrics(
            explicit_requests_attempted=self._profile_explicit_requests,
            explicit_requests_avoided_unsolicited=self._profile_unsolicited_avoided,
            blocks_satisfied_unsolicited=self._profile_unsolicited_avoided,
        )

    @property
    def acquisition_health(self) -> AcquisitionHealth:
        """Current experimental acquisition health without altering values."""
        return self._health

    def recovery_metrics(self) -> RecoveryMetrics:
        """Return detached sanitized bounded-recovery metrics."""
        return RecoveryMetrics(
            health=self._health,
            timeout_count=self._timeout_count,
            connection_loss_count=self._connection_loss_count,
            connection_establishment_failure_count=(
                self._connection_establishment_failure_count
            ),
            ambiguous_request_count=self._ambiguous_request_count,
            reconnect_attempts=self._reconnect_attempts,
            successful_reconnects=self._successful_reconnects,
            failed_reconnects=self._failed_reconnects,
            completed_recoveries=self._completed_recoveries,
            retry_budget_exhausted=self._retry_budget_exhausted,
            acquisitions_abandoned=self._acquisitions_abandoned,
            connection_generations_created=self._session.metrics().connections,
            connection_dial_attempts=self._connection_dial_attempts,
            failed_connection_dial_attempts=(
                self._failed_connection_dial_attempts
            ),
            events=tuple(self._recovery_events),
            recovery_event_capacity=RECOVERY_EVENT_CAPACITY,
            recovery_events_recorded=self._recovery_events_recorded,
            recovery_events_dropped=(
                self._recovery_events_recorded - len(self._recovery_events)
            ),
        )

    @property
    def last_profile_request_block(self) -> InputReadBlock | None:
        """Sanitized identity of the most recently attempted profile block."""
        return self._last_profile_request_block

    def drain_observations(self):
        """Return queued sanitized observation objects for measurement."""
        return self._session.drain_observations()

    def subscribe_observations(
        self, *, max_queue_size: int = 1024
    ) -> LuxObservationSubscription:
        """Create an independent bounded stream of accepted FC4 observations."""
        return self._session.subscribe_observations(
            max_queue_size=max_queue_size
        )

    def set_freshness_target(self, target: timedelta) -> None:
        """Change only the experimental stale threshold between bounded phases."""
        if target.total_seconds() <= 0:
            raise ValueError("freshness target must be positive")
        self._freshness_target = target

    async def async_refresh_operational(self) -> HybridRefreshResult:
        """Read only operational blocks not already sufficiently fresh."""
        started = time.monotonic()
        requested: list[InputReadBlock] = []
        skipped: list[InputReadBlock] = []
        for block in OPERATIONAL_READ_BLOCKS:
            # Re-snapshot before every block: an unsolicited frame routed while a
            # previous request was pending may have made this block fresh.
            if self._block_is_fresh(block, self._session.snapshot(), utc_now()):
                skipped.append(block)
                continue
            await self._session.async_read_input(
                block.start,
                block.count,
                context=self._request_context(LuxReadPurpose.OPERATIONAL_PROBE),
            )
            requested.append(block)
        return HybridRefreshResult(
            requested_blocks=tuple(requested),
            fresh_blocks_skipped=tuple(skipped),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def async_refresh_profile(self) -> HybridProfileRefreshResult:
        """Refresh only stale registers required by the configured profile."""
        if self._profile is None:
            raise ValueError("no read profile configured")
        async with self._profile_refresh_lock:
            return await self._async_refresh_profile_locked()

    async def _async_refresh_profile_locked(self) -> HybridProfileRefreshResult:
        started = self._monotonic()
        requested: list[InputReadBlock] = []
        skipped: list[InputReadBlock] = []
        unsolicited: list[InputReadBlock] = []
        reconnects = 0
        active_recovery: _ActiveRecovery | None = None
        restart_selection = True
        while restart_selection:
            restart_selection = False
            for block in self._profile.read_blocks:
                required = self._profile.required_registers_in(block)
                snapshot = self._session.snapshot()
                now = utc_now()
                fresh = self._required_registers_are_fresh(required, snapshot, now)
                if not fresh:
                    self._profile_explicit_requests += 1
                    self._last_profile_request_block = block
                    request_started_at = utc_now().isoformat()
                    try:
                        purpose = (
                            LuxReadPurpose.RECOVERY_REACQUISITION
                            if active_recovery is not None
                            else LuxReadPurpose.NORMAL_PROFILE
                        )
                        await self._session.async_read_input(
                            block.start,
                            block.count,
                            context=self._request_context(purpose),
                        )
                        requested.append(block)
                    except asyncio.CancelledError:
                        self._health = AcquisitionHealth.DEGRADED
                        if active_recovery is not None:
                            self._terminate_recovery(
                                active_recovery, "reacquisition_cancelled"
                            )
                            active_recovery = None
                        raise
                    except LuxPowerReadRejectedError:
                        self._health = AcquisitionHealth.DEGRADED
                        if active_recovery is not None:
                            self._terminate_recovery(
                                active_recovery, "reacquisition_rejected"
                            )
                            active_recovery = None
                        raise
                    except LuxPowerCommunicationError as exc:
                        if self._recovery_policy is None:
                            self._health = AcquisitionHealth.DEGRADED
                            raise
                        failure_kind = self._classify_recovery_failure(exc)
                        self._count_recovery_failure(failure_kind)
                        if (
                            active_recovery is not None
                            and self._shutdown.is_set()
                            and failure_kind is RecoveryFailureKind.SESSION_CLOSED
                        ):
                            self._health = AcquisitionHealth.DEGRADED
                            self._terminate_recovery(
                                active_recovery, "reacquisition_shutdown"
                            )
                            active_recovery = None
                            raise
                        if reconnects >= self._recovery_policy.max_reconnects_per_acquisition:
                            if active_recovery is not None:
                                self._terminate_recovery(
                                    active_recovery, "reacquisition_failed"
                                )
                                active_recovery = None
                            self._abandon_recovery(exc)
                        reconnects += 1
                        active_recovery = await self._recover_transport(
                            exc, block, failure_kind, request_started_at
                        )
                        # A timeout in a later block can make an earlier block
                        # stale. Restart selection; freshness will skip anything
                        # still healthy and request only what recovery requires.
                        restart_selection = True
                        break

                snapshot = self._session.snapshot()
                now = utc_now()
                observations = snapshot.observed_at.input_registers
                signature = (
                    tuple(observations[register] for register in sorted(required))
                    if all(register in observations for register in required)
                    else None
                )
                accounted = self._profile_accounted_observations.get(block)
                if accounted is None and all(
                    register in snapshot.explicit_observed_at for register in required
                ):
                    accounted = tuple(
                        snapshot.explicit_observed_at[register]
                        for register in sorted(required)
                    )
                    self._profile_accounted_observations[block] = accounted
                opportunity_due = bool(
                    accounted is None
                    or now - min(accounted) >= self._freshness_target
                )
                if fresh:
                    skipped.append(block)
                    if opportunity_due and signature is not None and signature != accounted:
                        due_indexes = (
                            tuple(range(len(signature)))
                            if accounted is None
                            else tuple(
                                index
                                for index, observed in enumerate(accounted)
                                if now - observed >= self._freshness_target
                            )
                        )
                        displaced_by_unsolicited = bool(due_indexes) and all(
                            (accounted is None or signature[index] > accounted[index])
                            and snapshot.input_sources.get(register)
                            is LuxObservationSource.UNSOLICITED
                            for index, register in enumerate(sorted(required))
                            if index in due_indexes
                        )
                        if displaced_by_unsolicited:
                            self._profile_unsolicited_avoided += 1
                            unsolicited.append(block)
                        self._profile_accounted_observations[block] = signature
                    elif accounted is None and signature is not None:
                        self._profile_accounted_observations[block] = signature
                    continue
                refreshed = self._session.snapshot().observed_at.input_registers
                if all(register in refreshed for register in required):
                    self._profile_accounted_observations[block] = tuple(
                        refreshed[register] for register in sorted(required)
                    )
        self._health = (
            AcquisitionHealth.HEALTHY
            if self._profile_is_fresh()
            else AcquisitionHealth.DEGRADED
        )
        if active_recovery is not None and self._health is AcquisitionHealth.HEALTHY:
            self._finish_recovery(active_recovery)
        elif active_recovery is not None:
            self._record_recovery(
                active_recovery,
                reconnect_succeeded=True,
                outcome="profile_remained_stale",
            )
            self._active_recovery = None
        return HybridProfileRefreshResult(
            requested_blocks=tuple(requested),
            fresh_blocks_skipped=tuple(skipped),
            blocks_satisfied_unsolicited=tuple(unsolicited),
            duration_ms=(self._monotonic() - started) * 1000,
        )

    async def _recover_transport(
        self,
        error: LuxPowerCommunicationError,
        block: InputReadBlock,
        kind: RecoveryFailureKind,
        episode_started_at: str,
    ) -> _ActiveRecovery:
        """Perform one budgeted clean reconnect; never retry the request here."""
        policy = self._recovery_policy
        if policy is None:
            raise error
        now = self._monotonic()
        recovery_started_at = utc_now().isoformat()
        cutoff = now - policy.rolling_window_seconds
        while self._reconnect_attempt_times and self._reconnect_attempt_times[0] < cutoff:
            self._reconnect_attempt_times.popleft()
        if len(self._reconnect_attempt_times) >= policy.max_reconnects_per_window:
            exhausted = _ActiveRecovery(
                failure_kind=kind,
                block=block,
                started_monotonic=now,
                episode_started_at=episode_started_at,
                recovery_started_at=recovery_started_at,
                cooldown_seconds=0,
                maximum_profile_age_seconds=self._maximum_profile_age_seconds(),
            )
            self._record_recovery(
                exhausted,
                reconnect_succeeded=False,
                outcome="rolling_budget_exhausted",
            )
            self._abandon_recovery(error)

        cooldown = (
            policy.initial_cooldown_seconds
            if not self._reconnect_attempt_times
            else policy.repeated_cooldown_seconds
        )
        active = _ActiveRecovery(
            failure_kind=kind,
            block=block,
            started_monotonic=now,
            episode_started_at=episode_started_at,
            recovery_started_at=recovery_started_at,
            cooldown_seconds=cooldown,
            maximum_profile_age_seconds=self._maximum_profile_age_seconds(),
        )
        self._health = AcquisitionHealth.RECOVERING
        self._active_recovery = active
        self._reconnect_attempt_times.append(now)
        self._reconnect_attempts += 1

        try:
            stopped = await self._shutdown_during(cooldown)
        except asyncio.CancelledError:
            self._health = AcquisitionHealth.DEGRADED
            self._record_recovery(
                active,
                reconnect_succeeded=False,
                outcome="cancelled",
            )
            self._active_recovery = None
            raise
        if stopped:
            self._record_recovery(
                active,
                reconnect_succeeded=False,
                outcome="shutdown",
            )
            self._active_recovery = None
            raise LuxPowerSessionClosedError("recovery stopped by session shutdown")
        active.reconnect_started_at = utc_now().isoformat()
        for attempt in range(policy.max_connection_attempts_per_reconnect):
            active.connection_dial_attempts += 1
            self._connection_dial_attempts += 1
            try:
                await self._session.async_connect()
                break
            except asyncio.CancelledError:
                self._health = AcquisitionHealth.DEGRADED
                self._record_recovery(
                    active,
                    reconnect_succeeded=False,
                    outcome="cancelled",
                )
                self._active_recovery = None
                raise
            except LuxPowerConnectionError:
                self._connection_establishment_failure_count += 1
                self._failed_connection_dial_attempts += 1
                active.failed_connection_dial_attempts += 1
                if attempt + 1 >= policy.max_connection_attempts_per_reconnect:
                    self._failed_reconnects += 1
                    self._acquisitions_abandoned += 1
                    self._health = AcquisitionHealth.DEGRADED
                    self._record_recovery(
                        active,
                        reconnect_succeeded=False,
                        outcome="connection_attempts_exhausted",
                    )
                    self._active_recovery = None
                    raise
                try:
                    stopped = await self._shutdown_during(
                        policy.repeated_cooldown_seconds
                    )
                except asyncio.CancelledError:
                    self._health = AcquisitionHealth.DEGRADED
                    self._record_recovery(
                        active,
                        reconnect_succeeded=False,
                        outcome="cancelled",
                    )
                    self._active_recovery = None
                    raise
                if stopped:
                    self._record_recovery(
                        active,
                        reconnect_succeeded=False,
                        outcome="shutdown",
                    )
                    self._active_recovery = None
                    raise LuxPowerSessionClosedError(
                        "recovery stopped by session shutdown"
                    )
        if self._shutdown.is_set():
            await self._session.async_close()
            self._record_recovery(
                active,
                reconnect_succeeded=False,
                outcome="shutdown",
            )
            self._active_recovery = None
            raise LuxPowerSessionClosedError("recovery stopped by session shutdown")
        self._successful_reconnects += 1
        active.connection_established_at = utc_now().isoformat()
        active.failure_to_connection_seconds = self._monotonic() - now
        active.maximum_profile_age_seconds = self._max_optional(
            active.maximum_profile_age_seconds,
            self._maximum_profile_age_seconds(),
        )
        return active

    async def _shutdown_during(self, delay: float) -> bool:
        if self._shutdown.is_set():
            return True
        if delay == 0:
            return self._shutdown.is_set()
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return self._shutdown.is_set()
        return True

    def _finish_recovery(self, active: _ActiveRecovery) -> None:
        self._completed_recoveries += 1
        self._record_recovery(
            active,
            reconnect_succeeded=True,
            outcome="profile_recovered",
            recovered=True,
        )
        self._active_recovery = None

    def _terminate_recovery(self, active: _ActiveRecovery, outcome: str) -> None:
        """Record one terminal non-success outcome and detach its sampler."""
        self._record_recovery(
            active,
            reconnect_succeeded=active.failure_to_connection_seconds is not None,
            outcome=outcome,
        )
        self._active_recovery = None

    def _record_recovery(
        self,
        active: _ActiveRecovery,
        *,
        reconnect_succeeded: bool,
        outcome: str,
        recovered: bool = False,
    ) -> None:
        current_age = self._maximum_profile_age_seconds()
        maximum_age = self._max_optional(
            active.maximum_profile_age_seconds,
            current_age,
        )
        self._recovery_events.append(
            RecoveryEvent(
                failure_kind=active.failure_kind,
                episode_started_at=active.episode_started_at,
                recovery_started_at=active.recovery_started_at,
                reconnect_started_at=active.reconnect_started_at,
                connection_established_at=active.connection_established_at,
                ended_at=utc_now().isoformat(),
                failed_register_start=active.block.start,
                failed_register_count=active.block.count,
                cooldown_seconds=active.cooldown_seconds,
                reconnect_succeeded=reconnect_succeeded,
                failure_to_connection_seconds=(
                    round(active.failure_to_connection_seconds, 6)
                    if active.failure_to_connection_seconds is not None
                    else None
                ),
                failure_to_profile_recovery_seconds=(
                    round(self._monotonic() - active.started_monotonic, 6)
                    if recovered
                    else None
                ),
                maximum_profile_age_seconds=(
                    round(maximum_age, 6) if maximum_age is not None else None
                ),
                outcome=outcome,
                connection_dial_attempts=active.connection_dial_attempts,
                failed_connection_dial_attempts=(
                    active.failed_connection_dial_attempts
                ),
            )
        )
        self._recovery_events_recorded += 1

    def _abandon_recovery(self, error: LuxPowerCommunicationError) -> None:
        self._health = AcquisitionHealth.DEGRADED
        self._retry_budget_exhausted += 1
        self._acquisitions_abandoned += 1
        raise LuxPowerRecoveryExhaustedError(
            "bounded read-session recovery budget exhausted"
        ) from error

    @staticmethod
    def _classify_recovery_failure(
        error: LuxPowerCommunicationError,
    ) -> RecoveryFailureKind:
        if isinstance(error, LuxPowerReadTimeoutError):
            return RecoveryFailureKind.REQUEST_TIMEOUT
        if isinstance(error, LuxPowerConnectionLostError):
            return RecoveryFailureKind.CONNECTION_LOST
        if isinstance(error, LuxPowerConnectionError):
            return RecoveryFailureKind.CONNECTION_ESTABLISHMENT
        if isinstance(error, LuxPowerAmbiguousRequestError):
            return RecoveryFailureKind.AMBIGUOUS_REQUEST
        if isinstance(error, LuxPowerSessionClosedError):
            return RecoveryFailureKind.SESSION_CLOSED
        raise error

    def _count_recovery_failure(self, kind: RecoveryFailureKind) -> None:
        if kind is RecoveryFailureKind.REQUEST_TIMEOUT:
            self._timeout_count += 1
        elif kind is RecoveryFailureKind.CONNECTION_LOST:
            self._connection_loss_count += 1
        elif kind is RecoveryFailureKind.CONNECTION_ESTABLISHMENT:
            self._connection_establishment_failure_count += 1
        elif kind is RecoveryFailureKind.AMBIGUOUS_REQUEST:
            self._ambiguous_request_count += 1

    def _maximum_profile_age_seconds(self) -> float | None:
        if self._profile is None:
            return None
        observations = self._session.snapshot().observed_at.input_registers
        now = utc_now()
        ages = [
            (now - observations[register]).total_seconds()
            for register in self._profile.required_registers
            if register in observations
        ]
        return max(ages) if ages else None

    def _request_context(self, purpose: LuxReadPurpose) -> LuxReadRequestContext:
        """Capture sanitized profile state without affecting request decisions."""
        return LuxReadRequestContext(
            purpose=purpose,
            profile_worst_age_seconds=self._maximum_profile_age_seconds(),
            profile_health=self._health.value,
        )

    def _profile_is_fresh(self) -> bool:
        if self._profile is None:
            return True
        snapshot = self._session.snapshot()
        now = utc_now()
        return all(
            self._required_registers_are_fresh(
                self._profile.required_registers_in(block), snapshot, now
            )
            for block in self._profile.read_blocks
        )

    def _observe_recovery_age(self) -> None:
        active = self._active_recovery
        if active is not None:
            active.maximum_profile_age_seconds = self._max_optional(
                active.maximum_profile_age_seconds,
                self._maximum_profile_age_seconds(),
            )

    @staticmethod
    def _max_optional(first: float | None, second: float | None) -> float | None:
        values = tuple(value for value in (first, second) if value is not None)
        return max(values) if values else None

    async def async_read_profile(self) -> HybridProfileRefreshResult:
        """Force one profile read for timing; excluded from avoidance metrics."""
        if self._profile is None:
            raise ValueError("no read profile configured")
        started = time.monotonic()
        for block in self._profile.read_blocks:
            self._last_profile_request_block = block
            await self._session.async_read_input(
                block.start,
                block.count,
                context=self._request_context(LuxReadPurpose.FORCED_PREFLIGHT),
            )
        self._health = (
            AcquisitionHealth.HEALTHY
            if self._profile_is_fresh()
            else AcquisitionHealth.DEGRADED
        )
        return HybridProfileRefreshResult(
            requested_blocks=self._profile.read_blocks,
            fresh_blocks_skipped=(),
            blocks_satisfied_unsolicited=(),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def async_read_operational(self) -> HybridRefreshResult:
        """Force one six-block routing validation independent of freshness."""
        started = time.monotonic()
        for block in OPERATIONAL_READ_BLOCKS:
            await self._session.async_read_input(
                block.start,
                block.count,
                context=self._request_context(LuxReadPurpose.OPERATIONAL_PROBE),
            )
        return HybridRefreshResult(
            requested_blocks=OPERATIONAL_READ_BLOCKS,
            fresh_blocks_skipped=(),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def async_full_scan(self) -> LuxReadSessionSnapshot:
        """Explicitly retain the proven aligned 0-749 full-scan capability."""
        for block in FULL_INPUT_READ_BLOCKS:
            await self._session.async_read_input(
                block.start,
                block.count,
                context=self._request_context(LuxReadPurpose.FULL_SCAN),
            )
        self._last_full_scan_completed_at = utc_now()
        return self._session.snapshot()

    async def async_run_hybrid(
        self,
        duration: float,
        *,
        include_full_scan: bool = True,
        sample_interval: float = 0.1,
        sample_sink: list[dict] | None = None,
    ) -> list[dict]:
        """Run a bounded hybrid experiment with independent freshness samples."""
        if duration <= 0:
            raise ValueError("duration must be positive")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be positive")
        deadline = time.monotonic() + duration
        samples = sample_sink if sample_sink is not None else []

        async def monitor_freshness() -> None:
            while time.monotonic() < deadline:
                now = utc_now()
                samples.append({
                    "at": now.isoformat(),
                    "operational_freshness": self._freshness_summary(
                        self._session.snapshot(), now
                    ),
                })
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                await asyncio.sleep(min(sample_interval, remaining))

        monitor = asyncio.create_task(monitor_freshness())
        try:
            while time.monotonic() < deadline:
                now = utc_now()
                full_due = bool(
                    include_full_scan
                    and (
                        self._last_full_scan_completed_at is None
                        or now - self._last_full_scan_completed_at
                        >= self._full_scan_interval
                    )
                )
                if full_due:
                    await self.async_full_scan()
                else:
                    await self.async_refresh_operational()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.25, remaining))
        finally:
            monitor.cancel()
            try:
                await monitor
            except asyncio.CancelledError:
                pass
        return samples

    async def async_run_profile(
        self,
        duration: float,
        *,
        sample_interval: float = 0.1,
        sample_sink: list[dict] | None = None,
    ) -> list[dict]:
        """Run bounded profile-only acquisition; full scans remain separate."""
        if self._profile is None:
            raise ValueError("no read profile configured")
        if duration <= 0 or sample_interval <= 0:
            raise ValueError("duration and sample_interval must be positive")
        deadline = time.monotonic() + duration
        samples = sample_sink if sample_sink is not None else []
        acquisition_done = asyncio.Event()

        def sample_freshness() -> None:
            self._observe_recovery_age()
            sampled_monotonic = time.monotonic()
            now = utc_now()
            samples.append(
                {
                    "at": now.isoformat(),
                    "monotonic_seconds": sampled_monotonic,
                    "acquisition_health": self._health.value,
                    "profile_freshness": self._profile_freshness_summary(
                        self._session.snapshot(), now
                    ),
                }
            )

        async def monitor_freshness() -> None:
            while not acquisition_done.is_set():
                sample_freshness()
                try:
                    await asyncio.wait_for(
                        acquisition_done.wait(), timeout=sample_interval
                    )
                except asyncio.TimeoutError:
                    pass
            sample_freshness()

        monitor = asyncio.create_task(monitor_freshness())
        try:
            while time.monotonic() < deadline:
                await self.async_refresh_profile()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.1, remaining))
        finally:
            acquisition_done.set()
            await monitor
        return samples

    def _block_is_fresh(
        self,
        block: InputReadBlock,
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> bool:
        threshold = self._freshness_target
        observations = snapshot.observed_at.input_registers
        return all(
            register in observations
            and now - observations[register] <= threshold
            for register in block.addresses()
            if input_register_group(register) is TelemetryGroup.OPERATIONAL
        )

    def _required_registers_are_fresh(
        self,
        required: frozenset[int],
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> bool:
        observations = snapshot.observed_at.input_registers
        return bool(required) and all(
            register in observations
            and now - observations[register] <= self._freshness_target
            for register in required
        )

    def _profile_freshness_summary(
        self,
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> dict:
        if self._profile is None:
            raise ValueError("no read profile configured")
        observations = snapshot.observed_at.input_registers
        ages_by_register = {
            register: (now - observations[register]).total_seconds()
            for register in self._profile.required_registers
            if register in observations
        }
        ages = tuple(ages_by_register.values())
        worst_register = (
            max(ages_by_register, key=ages_by_register.get)
            if ages_by_register
            else None
        )
        return {
            "known": len(ages),
            "required": len(self._profile.required_registers),
            "median_age_seconds": round(statistics.median(ages), 3) if ages else None,
            "max_age_seconds": round(max(ages), 3) if ages else None,
            "max_age_seconds_raw": max(ages) if ages else None,
            "worst_register": worst_register,
        }

    @staticmethod
    def _freshness_summary(
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> dict:
        operational = input_registers_for_group(TelemetryGroup.OPERATIONAL)
        ages = [
            (now - snapshot.observed_at.input_registers[register]).total_seconds()
            for register in operational
            if register in snapshot.observed_at.input_registers
        ]
        return {
            "known": len(ages),
            "required": len(operational),
            "median_age_seconds": round(statistics.median(ages), 3) if ages else None,
            "max_age_seconds": round(max(ages), 3) if ages else None,
        }


def _nearest_rank(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _latency_summary(
    latencies: Sequence[float],
    *,
    samples_total: int,
    reply_timeout_seconds: float | None = READ_TIMEOUT,
) -> dict:
    """Return a sanitized exact distribution for retained accepted responses."""
    values = tuple(float(value) for value in latencies)
    truncated = samples_total > len(values)
    thresholds_ms = (1000, 1500, 2000, 2500, 3000, 5000, 10000)
    above = {
        str(threshold): {
            "count": sum(value > threshold for value in values),
            "percent": (
                round(100 * sum(value > threshold for value in values) / len(values), 6)
                if values
                else None
            ),
        }
        for threshold in thresholds_ms
    }
    histogram_limits = (500, 750, 1000, 1500, 2000, 2500, 3000, 5000, 10000)
    histogram: dict[str, int] = {}
    lower = 0
    for upper in histogram_limits:
        histogram[f"{lower}-{upper}"] = sum(
            lower < value <= upper for value in values
        )
        lower = upper
    histogram[">10000"] = sum(value > 10000 for value in values)
    maximum = max(values) if values else None
    decision_buckets = {
        "0-3000": sum(0 <= value <= 3000 for value in values),
        ">3000-5000": sum(3000 < value <= 5000 for value in values),
        ">5000-10000": sum(5000 < value <= 10000 for value in values),
        ">10000": sum(value > 10000 for value in values),
        "beyond_reply_timeout": (
            sum(value > reply_timeout_seconds * 1000 for value in values)
            if reply_timeout_seconds is not None
            else None
        ),
    }
    return {
        "samples": len(values),
        "samples_total": samples_total,
        "truncated": truncated,
        "mean": round(statistics.fmean(values), 3) if values and not truncated else None,
        "median": round(statistics.median(values), 3) if values and not truncated else None,
        "p95": (
            round(_nearest_rank(values, 0.95), 3)
            if len(values) >= 20 and not truncated
            else None
        ),
        "p99": (
            round(_nearest_rank(values, 0.99), 3)
            if len(values) >= 100 and not truncated
            else None
        ),
        "min": round(min(values), 3) if values and not truncated else None,
        "max": round(maximum, 3) if maximum is not None and not truncated else None,
        "successful_max_margin_to_timeout_ms": (
            round(reply_timeout_seconds * 1000 - maximum, 3)
            if maximum is not None
            and not truncated
            and reply_timeout_seconds is not None
            else None
        ),
        "reply_timeout_seconds": reply_timeout_seconds,
        "above_threshold_ms": above if not truncated else None,
        "decision_buckets_ms": decision_buckets if not truncated else None,
        "histogram_ms": histogram if not truncated else None,
        "values_ms": [round(value, 3) for value in values],
    }


def _metrics_delta(
    before: LuxReadSessionMetrics,
    after: LuxReadSessionMetrics,
    *,
    reply_timeout_seconds: float | None = READ_TIMEOUT,
) -> dict:
    fields = (
        "bytes_received",
        "frames_received",
        "validated_fc4_frames",
        "expected_fc4_responses",
        "unmatched_fc4_observations",
        "duplicate_fc4_frames",
        "invalid_frames",
        "function_193_frames",
        "explicit_requests",
        "request_timeouts",
        "connection_losses",
        "operational_registers_expected",
        "operational_registers_unmatched",
        "observation_queue_drops",
        "connection_attempts",
        "connection_failures",
        "ambiguous_requests",
        "modbus_rejections",
        "tcp_keepalive_applied_connections",
        "tcp_keepalive_idle_applied_connections",
        "tcp_keepalive_configuration_failures",
        "tcp_keepalive_configuration_unavailable",
        "receive_inactivity_timeouts",
    )
    delta = {name: getattr(after, name) - getattr(before, name) for name in fields}
    new_latency_count = (
        after.request_latency_samples_total
        - before.request_latency_samples_total
    )
    latencies = (
        after.request_latencies_ms[-new_latency_count:]
        if new_latency_count else ()
    )
    delta["request_latency_ms"] = _latency_summary(
        latencies,
        samples_total=new_latency_count,
        reply_timeout_seconds=reply_timeout_seconds,
    )
    return delta


def _range_summary(snapshot: LuxReadSessionSnapshot) -> list[dict]:
    addresses = sorted(snapshot.observed_at.input_registers)
    if not addresses:
        return []
    ranges: list[dict] = []
    start = previous = addresses[0]
    for address in addresses[1:]:
        if address != previous + 1:
            ranges.append({"start": start, "end": previous, "count": previous - start + 1})
            start = address
        previous = address
    ranges.append({"start": start, "end": previous, "count": previous - start + 1})
    return ranges


def _observation_summary(observations) -> dict:
    """Summarize routed observations without serials, packets, or values."""
    ordered = sorted(observations, key=lambda item: item.observed_at)
    intervals = [
        (later.observed_at - earlier.observed_at).total_seconds()
        for earlier, later in zip(ordered, ordered[1:])
    ]
    return {
        "count": len(ordered),
        "explicit": sum(item.explicit_response for item in ordered),
        "unmatched": sum(not item.explicit_response for item in ordered),
        "duplicates": sum(item.duplicate for item in ordered),
        "ranges": [
            {
                "start": item.register_start,
                "count": item.register_count,
                "end": item.register_end,
                "explicit_response": item.explicit_response,
                "duplicate": item.duplicate,
            }
            for item in ordered
        ],
        "interval_seconds": {
            "samples": len(intervals),
            "median": round(statistics.median(intervals), 3) if intervals else None,
            "min": round(min(intervals), 3) if intervals else None,
            "max": round(max(intervals), 3) if intervals else None,
        },
    }


async def execute_live_validation(
    client: LuxPowerHybridReadClient,
    *,
    passive_seconds: float,
    hybrid_targets: Sequence[float],
    hybrid_seconds: float,
) -> dict:
    """Run bounded passive, explicit-routing, and progressive hybrid phases."""
    report = {
        "schema_version": HYBRID_SCHEMA_VERSION,
        "hybrid_version": HYBRID_VERSION,
        "started_at": utc_now().isoformat(),
        "safety": {
            "read_only": True,
            "permitted_function_codes": [4],
            "writes_exposed": False,
        },
        "configuration": {
            "passive_seconds": passive_seconds,
            "operational_blocks": [asdict(block) for block in OPERATIONAL_READ_BLOCKS],
            "hybrid_targets_seconds": list(hybrid_targets),
            "hybrid_seconds_per_target": hybrid_seconds,
        },
        "phases": [],
    }
    await client.async_connect()
    try:
        client.drain_observations()
        before = client.metrics()
        await client.async_passive(passive_seconds)
        after = client.metrics()
        passive_observations = client.drain_observations()
        report["phases"].append({
            "name": "passive",
            "metrics": _metrics_delta(before, after),
            "observed_ranges": _range_summary(client.snapshot()),
            "observations": _observation_summary(passive_observations),
        })

        client.drain_observations()
        before = client.metrics()
        explicit_started = time.monotonic()
        explicit = await client.async_read_operational()
        after = client.metrics()
        explicit_observations = client.drain_observations()
        explicit_phase = {
            "name": "explicit_operational",
            "duration_ms": round((time.monotonic() - explicit_started) * 1000, 3),
            "requested_blocks": [asdict(block) for block in explicit.requested_blocks],
            "fresh_blocks_skipped": [asdict(block) for block in explicit.fresh_blocks_skipped],
            "metrics": _metrics_delta(before, after),
            "observed_ranges": _range_summary(client.snapshot()),
            "observations": _observation_summary(explicit_observations),
        }
        report["phases"].append(explicit_phase)

        stable = not any(
            explicit_phase["metrics"][name]
            for name in ("request_timeouts", "connection_losses", "invalid_frames")
        )

        if stable:
            client.drain_observations()
            before = client.metrics()
            full_started = time.monotonic()
            full_error = None
            try:
                await client.async_full_scan()
            except LuxPowerCommunicationError as exc:
                full_error = type(exc).__name__
            full_duration = time.monotonic() - full_started
            after = client.metrics()
            full_phase = {
                "name": "frame_aware_full_scan",
                "duration_seconds": round(full_duration, 3),
                "requested_blocks": len(FULL_INPUT_READ_BLOCKS),
                "metrics": _metrics_delta(before, after),
                "observations": _observation_summary(client.drain_observations()),
                "observed_registers": len(client.snapshot().input_registers),
                "status": "failed" if full_error else "success",
                "error": full_error,
            }
            report["phases"].append(full_phase)
            stable = not any(
                full_phase["metrics"][name]
                for name in ("request_timeouts", "connection_losses", "invalid_frames")
            ) and full_phase["observed_registers"] == TOTAL_REGISTERS and not full_error

        for target in hybrid_targets:
            if not stable:
                break
            client.set_freshness_target(timedelta(seconds=target))
            client.drain_observations()
            before = client.metrics()
            phase_started = time.monotonic()
            samples: list[dict] = []
            phase_error = None
            try:
                await client.async_run_hybrid(
                    hybrid_seconds,
                    include_full_scan=False,
                    sample_sink=samples,
                )
            except LuxPowerCommunicationError as exc:
                phase_error = type(exc).__name__
            actual_duration = time.monotonic() - phase_started
            after = client.metrics()
            observations = client.drain_observations()
            delta = _metrics_delta(before, after)
            freshness = [sample["operational_freshness"] for sample in samples]
            phase = {
                "name": "hybrid",
                "target_seconds": target,
                "duration_seconds": hybrid_seconds,
                "actual_duration_seconds": round(actual_duration, 3),
                "fast_path_only": True,
                "full_scan_validated_separately": True,
                "status": "failed" if phase_error else "success",
                "error": phase_error,
                "metrics": delta,
                "samples": samples,
                "observations": _observation_summary(observations),
                "max_observed_operational_age_seconds": max(
                    (
                        item["max_age_seconds"]
                        for item in freshness
                        if item["max_age_seconds"] is not None
                    ),
                    default=None,
                ),
            }
            operational_observations = (
                delta["operational_registers_expected"]
                + delta["operational_registers_unmatched"]
            )
            phase["operational_receptions_by_route"] = {
                "explicit": delta["operational_registers_expected"],
                "unsolicited": delta["operational_registers_unmatched"],
                "unsolicited_reception_percent": (
                    round(
                        100 * delta["operational_registers_unmatched"]
                        / operational_observations,
                        3,
                    )
                    if operational_observations else None
                ),
            }
            phase["explicit_requests_per_minute"] = round(
                delta["explicit_requests"] * 60 / actual_duration, 3
            )
            ordered_max_ages = sorted(
                item["max_age_seconds"]
                for item in freshness
                if item["max_age_seconds"] is not None
            )
            p95_index = max(0, int(len(ordered_max_ages) * 0.95 + 0.999) - 1)
            phase["sampled_worst_register_age_seconds"] = {
                "samples": len(ordered_max_ages),
                "median": (
                    round(statistics.median(ordered_max_ages), 3)
                    if ordered_max_ages else None
                ),
                "p95": ordered_max_ages[p95_index] if ordered_max_ages else None,
                "max": max(ordered_max_ages) if ordered_max_ages else None,
                "sampling_interval_seconds": 0.1,
            }
            phase["target_met"] = bool(
                freshness
                and phase_error is None
                and all(item["known"] == item["required"] for item in freshness)
                and phase["max_observed_operational_age_seconds"] is not None
                and phase["max_observed_operational_age_seconds"] <= target
            )
            report["phases"].append(phase)
            stable = not any(
                delta[name]
                for name in ("request_timeouts", "connection_losses", "invalid_frames")
            ) and phase["target_met"] and phase_error is None
    finally:
        await client.async_close()
    report["final_metrics"] = asdict(client.metrics())
    report["completed_at"] = utc_now().isoformat()
    return report


def _load_private_target(environ: Mapping[str, str]) -> tuple[str, int, str, str]:
    required = ("LUXPOWER_HOST", "LUXPOWER_DONGLE_SERIAL", "LUXPOWER_INVERTER_SERIAL")
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise ValueError(f"missing required environment variables: {', '.join(missing)}")
    return (
        environ["LUXPOWER_HOST"],
        int(environ.get("LUXPOWER_PORT", "8000")),
        environ["LUXPOWER_DONGLE_SERIAL"],
        environ["LUXPOWER_INVERTER_SERIAL"],
    )


def _parse_targets(value: str) -> tuple[float, ...]:
    targets = tuple(float(item.strip()) for item in value.split(","))
    if not targets or any(item <= 0 for item in targets):
        raise argparse.ArgumentTypeError("targets must be positive")
    if tuple(sorted(targets, reverse=True)) != targets:
        raise argparse.ArgumentTypeError("targets must run slowest to fastest")
    return targets


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Experimental LuxPower frame-aware READ-ONLY validation"
    )
    parser.add_argument("--confirm-read-only", action="store_true")
    parser.add_argument("--passive-seconds", type=float, default=30)
    parser.add_argument("--hybrid-targets", type=_parse_targets, default=(5.0, 3.0, 2.0))
    parser.add_argument("--hybrid-seconds", type=float, default=60)
    parser.add_argument("--output", type=Path)
    return parser


async def _async_main(arguments: argparse.Namespace) -> int:
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    if arguments.passive_seconds <= 0 or arguments.hybrid_seconds <= 0:
        raise ValueError("phase durations must be positive")
    host, port, dongle, inverter = _load_private_target(os.environ)
    client = LuxPowerHybridReadClient(host, dongle, inverter, port=port)
    report = await execute_live_validation(
        client,
        passive_seconds=arguments.passive_seconds,
        hybrid_targets=arguments.hybrid_targets,
        hybrid_seconds=arguments.hybrid_seconds,
    )
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if arguments.output:
        arguments.output.write_text(serialized + "\n", encoding="utf-8")
    print("LuxPower frame-aware READ-ONLY validation completed", file=sys.stderr)
    print(serialized)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        return asyncio.run(_async_main(arguments))
    except ValueError as exc:
        build_argument_parser().error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
