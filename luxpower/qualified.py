"""Supported product boundary for the qualified read-only Lux FC4 core.

Only owner-level lifecycle, profile acquisition, typed snapshots, and sanitized
metrics are exposed here.  Qualification tooling deliberately retains access to
the underlying implementation through a private alias; production consumers
must not depend on that alias or on low-level session APIs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

from custom_components.lxp_modbus.classes.read_session import (
    LuxReadSession as _LuxReadSession,
    LuxReadSessionMetrics,
)
from custom_components.lxp_modbus.exceptions import (
    LuxPowerCommunicationError,
    LuxPowerConnectionError,
    LuxPowerReadRejectedError,
    LuxPowerRecoveryExhaustedError,
    LuxPowerSessionClosedError,
)
from custom_components.lxp_modbus.observation import utc_now
from custom_components.lxp_modbus.read_profiles import (
    ENERGY_FLOW_PROFILE_DEFINITION_VERSION,
    EnergyFlowReadProfile,
    EnergyFlowSnapshot,
    GridTopology,
    LoadLayout,
    ObservedProfileValue,
    ProfileField,
    profile_block_details,
)
from custom_components.lxp_modbus.recovery import (
    AcquisitionHealth,
    RecoveryMetrics,
    RecoveryPolicy,
)
from luxpower.hybrid import (
    HybridProfileMetrics,
    LuxPowerHybridReadClient as _LuxPowerHybridReadClient,
)

QUALIFIED_CORE_API_VERSION = 1
QUALIFIED_FRESHNESS_TARGET = timedelta(seconds=20)
QUALIFIED_DRAIN_TIMEOUT_SECONDS = 3.0
QUALIFIED_REPLY_TIMEOUT_SECONDS = 10.0

# Qualification needs forced reads and phase controls that are intentionally
# absent from the supported facade.  Keeping this private alias here proves the
# harness and facade delegate to one implementation rather than two read paths.
_QualificationLuxReadClient = _LuxPowerHybridReadClient


@dataclass(frozen=True)
class QualifiedLuxSnapshot:
    """One detached profile view and its truthful inspection-time health."""

    api_version: int
    profile: EnergyFlowSnapshot
    field_definitions: tuple[ProfileField, ...]
    acquisition_health: AcquisitionHealth
    freshness_target: timedelta
    inspected_at: datetime
    fresh: bool


class QualifiedLuxReadClient:
    """Narrow lifecycle facade over the previously qualified FC4 implementation.

    The owner starts the client, invokes :meth:`async_acquire` whenever it wants
    the configured profile refreshed, reads detached snapshots, and closes it.
    No scheduler or background polling loop is created by this facade.
    """

    def __init__(
        self,
        host: str,
        dongle_serial: str,
        inverter_serial: str,
        *,
        profile: EnergyFlowReadProfile,
        port: int = 8000,
        freshness_target: timedelta = QUALIFIED_FRESHNESS_TARGET,
        recovery_policy: RecoveryPolicy | None = RecoveryPolicy(),
        tcp_keepalive: bool = True,
        tcp_keepalive_idle_seconds: int = 60,
        receive_inactivity_timeout: float | None = 900.0,
    ) -> None:
        if not isinstance(profile, EnergyFlowReadProfile):
            raise TypeError("profile must be an EnergyFlowReadProfile")
        session = _LuxReadSession(
            host,
            dongle_serial,
            inverter_serial,
            port=port,
            drain_timeout=QUALIFIED_DRAIN_TIMEOUT_SECONDS,
            reply_timeout=QUALIFIED_REPLY_TIMEOUT_SECONDS,
            tcp_keepalive=tcp_keepalive,
            tcp_keepalive_idle_seconds=tcp_keepalive_idle_seconds,
            receive_inactivity_timeout=receive_inactivity_timeout,
        )
        self._profile = profile
        self._freshness_target = freshness_target
        self._delegate = _LuxPowerHybridReadClient(
            host,
            dongle_serial,
            inverter_serial,
            port=port,
            freshness_target=freshness_target,
            profile=profile,
            session=session,
            recovery_policy=recovery_policy,
            tcp_keepalive=tcp_keepalive,
            tcp_keepalive_idle_seconds=tcp_keepalive_idle_seconds,
            receive_inactivity_timeout=receive_inactivity_timeout,
        )
        self._lifecycle_lock = asyncio.Lock()
        self._acquisition_lock = asyncio.Lock()
        self._started = False
        self._closed = False

    @property
    def profile(self) -> EnergyFlowReadProfile:
        return self._profile

    @property
    def freshness_target(self) -> timedelta:
        return self._freshness_target

    @property
    def acquisition_health(self) -> AcquisitionHealth:
        return self._delegate.acquisition_health

    async def async_start(self) -> None:
        """Open the connection and sole reader task; repeated starts are harmless."""
        async with self._lifecycle_lock:
            if self._started:
                return
            self._closed = False
            # A cancelled close may have interrupted a previous acquisition.
            # Never start a new connection generation until that caller exits.
            async with self._acquisition_lock:
                await self._delegate.async_connect()
                self._started = True

    async def async_acquire(self) -> QualifiedLuxSnapshot:
        """Refresh the configured profile once and return a detached snapshot."""
        self._require_started()
        async with self._acquisition_lock:
            self._require_started()
            await self._delegate.async_refresh_profile()
            # A concurrent close deliberately interrupts an acquisition.  Do not
            # return a snapshot after ownership has moved to the stopped state.
            self._require_started()
            return self.snapshot()

    def snapshot(self) -> QualifiedLuxSnapshot:
        """Return current typed values without performing socket I/O."""
        self._require_started()
        profile_snapshot = self._delegate.profile_snapshot()
        inspected_at = utc_now()
        observed_at = profile_snapshot.observed_at
        fresh = (
            observed_at is not None
            and inspected_at - observed_at <= self._freshness_target
        )
        return QualifiedLuxSnapshot(
            api_version=QUALIFIED_CORE_API_VERSION,
            profile=profile_snapshot,
            field_definitions=self._profile.fields,
            acquisition_health=self._delegate.acquisition_health,
            freshness_target=self._freshness_target,
            inspected_at=inspected_at,
            fresh=fresh,
        )

    def transport_metrics(self) -> LuxReadSessionMetrics:
        """Return detached transport counters without exposing the session."""
        return self._delegate.metrics()

    def profile_metrics(self) -> HybridProfileMetrics:
        """Return detached profile acquisition counters."""
        return self._delegate.profile_metrics()

    def recovery_metrics(self) -> RecoveryMetrics:
        """Return detached recovery totals and bounded recent events."""
        return self._delegate.recovery_metrics()

    async def async_close(self) -> None:
        """Stop recovery and the sole reader task; repeated closes are harmless."""
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._started = False
            await self._delegate.async_close()
            # The transport close interrupts any pending acquisition.  Wait for
            # its caller to observe that shutdown before allowing a restart.
            async with self._acquisition_lock:
                self._closed = True

    async def __aenter__(self) -> "QualifiedLuxReadClient":
        await self.async_start()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.async_close()

    def _require_started(self) -> None:
        if not self._started:
            raise LuxPowerSessionClosedError("qualified Lux read client is not started")


__all__ = [
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
]
