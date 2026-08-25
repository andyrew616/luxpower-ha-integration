"""Exceptions owned by the reusable LuxPower client layer."""


class LuxPowerError(Exception):
    """Base exception for LuxPower client failures."""


class LuxPowerCommunicationError(LuxPowerError):
    """Raised when inverter communication cannot produce usable telemetry."""


class LuxPowerConnectionError(LuxPowerCommunicationError):
    """Raised when a new read connection cannot be established."""


class LuxPowerConnectionLostError(LuxPowerCommunicationError):
    """Raised when an established frame-aware read connection is lost."""


class LuxPowerAmbiguousRequestError(LuxPowerCommunicationError):
    """Raised when an FC4 request may have been sent but cannot be correlated."""


class LuxPowerRecoveryExhaustedError(LuxPowerCommunicationError):
    """Raised when the configured bounded transport recovery budget is exhausted."""


class LuxPowerSessionClosedError(LuxPowerCommunicationError):
    """Raised when a frame-aware read session closes with work outstanding."""


class LuxPowerReadTimeoutError(LuxPowerCommunicationError):
    """Raised when no exactly matching FC4 response arrives before the deadline."""


class LuxPowerReadRejectedError(LuxPowerCommunicationError):
    """Raised when the inverter explicitly rejects an FC4 request."""
