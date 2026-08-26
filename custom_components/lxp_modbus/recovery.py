"""Bounded recovery policy and sanitized state for experimental Lux reads."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

MAX_CONNECTION_ATTEMPTS_PER_RECONNECT = 5


class AcquisitionHealth(str, Enum):
    """Transport/acquisition state exposed without changing cached values."""

    HEALTHY = "healthy"
    RECOVERING = "recovering"
    DEGRADED = "degraded"


class RecoveryFailureKind(str, Enum):
    """Sanitized failure classes relevant to bounded transport recovery."""

    REQUEST_TIMEOUT = "request_timeout"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_ESTABLISHMENT = "connection_establishment"
    AMBIGUOUS_REQUEST = "ambiguous_request"
    SESSION_CLOSED = "session_closed"


@dataclass(frozen=True)
class RecoveryPolicy:
    """Conservative opt-in reconnect budget for experimental acquisition."""

    max_reconnects_per_acquisition: int = 1
    max_reconnects_per_window: int = 2
    max_connection_attempts_per_reconnect: int = 3
    rolling_window_seconds: float = 300.0
    initial_cooldown_seconds: float = 1.0
    repeated_cooldown_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.max_reconnects_per_acquisition < 1:
            raise ValueError("max_reconnects_per_acquisition must be positive")
        if self.max_reconnects_per_window < 1:
            raise ValueError("max_reconnects_per_window must be positive")
        if self.max_connection_attempts_per_reconnect < 1:
            raise ValueError(
                "max_connection_attempts_per_reconnect must be positive"
            )
        if (
            self.max_connection_attempts_per_reconnect
            > MAX_CONNECTION_ATTEMPTS_PER_RECONNECT
        ):
            raise ValueError(
                "max_connection_attempts_per_reconnect cannot exceed "
                f"{MAX_CONNECTION_ATTEMPTS_PER_RECONNECT}"
            )
        if self.rolling_window_seconds <= 0:
            raise ValueError("rolling_window_seconds must be positive")
        if min(self.initial_cooldown_seconds, self.repeated_cooldown_seconds) < 0:
            raise ValueError("recovery cooldowns cannot be negative")


@dataclass(frozen=True)
class RecoveryEvent:
    """One sanitized recovery attempt and its measured outcome."""

    failure_kind: RecoveryFailureKind
    episode_started_at: str
    ended_at: str
    failed_register_start: int
    failed_register_count: int
    cooldown_seconds: float
    reconnect_succeeded: bool
    failure_to_connection_seconds: float | None
    failure_to_profile_recovery_seconds: float | None
    maximum_profile_age_seconds: float | None
    outcome: str
    connection_dial_attempts: int = 0
    failed_connection_dial_attempts: int = 0
    recovery_started_at: str | None = None
    reconnect_started_at: str | None = None
    connection_established_at: str | None = None


@dataclass(frozen=True)
class RecoveryMetrics:
    """Detached aggregate recovery metrics containing no target identifiers."""

    health: AcquisitionHealth
    timeout_count: int
    connection_loss_count: int
    connection_establishment_failure_count: int
    ambiguous_request_count: int
    reconnect_attempts: int
    successful_reconnects: int
    failed_reconnects: int
    completed_recoveries: int
    retry_budget_exhausted: int
    acquisitions_abandoned: int
    connection_generations_created: int
    connection_dial_attempts: int = 0
    failed_connection_dial_attempts: int = 0
    events: tuple[RecoveryEvent, ...] = field(default_factory=tuple)
