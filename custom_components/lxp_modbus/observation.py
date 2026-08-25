"""Home Assistant-independent observation timestamp types and helpers."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Mapping

ObservationClock = Callable[[], datetime]
BatteryObservationMap = Mapping[int | str, datetime]


def utc_now() -> datetime:
    """Return the current absolute time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def require_aware_utc(value: datetime) -> datetime:
    """Validate and normalise an observation time to UTC."""
    if not isinstance(value, datetime):
        raise TypeError("observation clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observation clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class LuxPowerObservationTimes:
    """Local times when each raw value was last successfully accepted.

    These are client observation times, not inverter-origin measurement times.
    Missing entries mean the value has not been observed by this client.
    """

    input_registers: Mapping[int, datetime] = field(default_factory=dict)
    holding_registers: Mapping[int, datetime] = field(default_factory=dict)
    batteries: Mapping[str, BatteryObservationMap] = field(default_factory=dict)

    def detached_copy(self) -> "LuxPowerObservationTimes":
        """Return a deep-enough copy that cannot alias the client's dictionaries."""
        return LuxPowerObservationTimes(
            input_registers=dict(self.input_registers),
            holding_registers=dict(self.holding_registers),
            batteries={
                serial: dict(registers)
                for serial, registers in self.batteries.items()
            },
        )
