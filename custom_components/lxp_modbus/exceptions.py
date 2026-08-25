"""Exceptions owned by the reusable LuxPower client layer."""


class LuxPowerError(Exception):
    """Base exception for LuxPower client failures."""


class LuxPowerCommunicationError(LuxPowerError):
    """Raised when inverter communication cannot produce usable telemetry."""
